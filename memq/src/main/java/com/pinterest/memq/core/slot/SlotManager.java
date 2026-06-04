/**
 * Copyright 2022 Pinterest, Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *    http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.pinterest.memq.core.slot;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.pinterest.memq.core.config.SlotAccountingConfig;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;
import java.util.logging.Level;
import java.util.logging.Logger;

import static java.lang.Math.ceil;
import static java.lang.Math.exp;

public class SlotManager {

  private static final Logger logger = Logger.getLogger(SlotManager.class.getName());
  private static final double BYTES_PER_MB = 1024.0 * 1024.0;

  /**
   * Codahale registry-key prefix for the per-producer EMA gauge.
   * The full key uses {@link #encodeMetricKey(String, String, String)} to
   * inline {@code pid} and {@code topic} as OpenTSDB tags via the
   * {@code |key=value} convention parsed by
   * {@link com.pinterest.memq.commons.mon.OpenTSDBReporter}.
   */
  static final String EMA_METRIC_NAME = "producer.ema";
  static final String SLOTS_METRIC_NAME = "producer.slots";

  private final int totalSlots;
  private final double slotSizeMbps;
  private final double acquireThresholdMs;
  private final double releaseThresholdMs;
  private final double cooldownMs;
  private final double emaDecay;
  private final double tickIntervalSec;
  private final long tickIntervalMs;
  private final long idleProducerTimeoutMs;
  private final long postEvictionCooldownMs;

  private final boolean drainLatchEnabled;
  private final double drainLatchEmaDecay;
  private final double drainLatchDisengageFreeSlots;
  private final int maxSlotStep;

  private final ConcurrentHashMap<String, ConcurrentHashMap<String, ProducerSlotState>> producers =
      new ConcurrentHashMap<>();
  private final AtomicInteger totalOccupiedSlots = new AtomicInteger(0);
  private volatile long lastSlotChangeTimeMs = 0;

  /**
   * Per-(pid, topic) wall-clock millis until which {@link #tryAcquireSlots}
   * must refuse to grant additional slots. Armed by the eviction code path
   * ({@link #releaseProducerSlots}). Stored here, at the manager level, rather
   * than on {@link ProducerSlotState} so the cooldown survives the state
   * removal that {@link #decrementSlots} performs when a producer's slot count
   * for a topic drops to 0 -- otherwise the next {@link #recordWrite} recreates
   * a fresh state with no cooldown and the just-evicted producer re-acquires
   * after {@code acquireThreshold} instead of waiting out the full
   * post-eviction cooldown (the production eviction "flap"). Keyed by
   * {@link #cooldownKey(String, String)}.
   */
  private final ConcurrentHashMap<String, Long> evictionCooldownUntilMs = new ConcurrentHashMap<>();

  /**
   * Smoothed (EMA) free-slot count and the latched drain state derived from it.
   * Only mutated by the single background tick thread; read by the hot path
   * ({@link #tryAcquireSlots}) and by {@link #isFrozen()}. {@code volatile} so
   * the boolean is visible across threads without locking.
   */
  private double freeSlotsEma;
  private volatile boolean drainLatched;

  private final ConcurrentHashMap<String, Set<String>> producerConnections = new ConcurrentHashMap<>();

  /**
   * Maps a producer id (a v4 client-generated UUID) to the remote IP of the
   * connection it writes from, so slot/eviction logs keyed by the opaque UUID
   * can be traced back to a host. For v3 producers the id already is the IP.
   */
  private final ConcurrentHashMap<String, String> producerIps = new ConcurrentHashMap<>();

  /**
   * Optional MetricRegistry into which we register tag-encoded
   * {@code producer.ema} and {@code producer.slots} gauges per (pid, topic).
   * May be {@code null} -- tests that don't care about metrics use the
   * 2-arg constructor.
   */
  private final MetricRegistry registry;

  /**
   * Tracks (pid, topic) pairs we've already registered metrics for, so that
   * {@link #tick()} only registers each pair once. Key is the encoded
   * metric registry key (so deregistration can remove by exact name).
   */
  private final Set<String> registeredEmaKeys = ConcurrentHashMap.newKeySet();
  private final Set<String> registeredSlotsKeys = ConcurrentHashMap.newKeySet();

  private ScheduledExecutorService tickExecutor;

  public SlotManager(SlotAccountingConfig config, int totalSlots) {
    this(config, totalSlots, null);
  }

  /**
   * @param config slot-accounting configuration (slot size, thresholds,
   *               cooldowns, drain latch parameters, etc.).
   * @param totalSlots total slot capacity for this broker.
   * @param registry optional registry for per-(pid, topic) producer.ema /
   *                 producer.slots gauges. When non-null, gauges are
   *                 registered with names of the form
   *                 {@code "producer.ema|pid=<pid>|topic=<topic>"} that
   *                 {@link com.pinterest.memq.commons.mon.OpenTSDBReporter}
   *                 emits as {@code memq.<base>.producer.ema} with
   *                 {@code pid=<pid> topic=<topic>} tags.
   */
  public SlotManager(SlotAccountingConfig config, int totalSlots, MetricRegistry registry) {
    this.totalSlots = totalSlots;
    this.slotSizeMbps = config.getSlotSizeMbps();
    this.acquireThresholdMs = config.getAcquireThresholdSeconds() * 1000.0;
    this.releaseThresholdMs = config.getReleaseThresholdSeconds() * 1000.0;
    this.cooldownMs = config.getCooldownSeconds() * 1000.0;
    this.tickIntervalMs = config.getTickIntervalMs();
    this.tickIntervalSec = tickIntervalMs / 1000.0;
    this.idleProducerTimeoutMs = config.getIdleProducerTimeoutMs();
    this.postEvictionCooldownMs =
        (long) (config.getPostEvictionCooldownSeconds() * 1000.0);
    this.registry = registry;

    double emaWindowSec = config.getEmaWindowSeconds();
    this.emaDecay = exp(-tickIntervalSec / emaWindowSec);

    this.drainLatchEnabled = config.isDrainLatchEnabled();
    double drainLatchWindowSec = config.getDrainLatchEmaWindowSeconds();
    this.drainLatchEmaDecay = exp(-tickIntervalSec / drainLatchWindowSec);
    this.drainLatchDisengageFreeSlots =
        Math.max(config.getDrainLatchDisengageFreeSlots(), totalSlots / 10.0);
    this.maxSlotStep = config.getMaxSlotStep();
    // Start un-latched: a freshly started, empty broker has all slots free.
    this.freeSlotsEma = totalSlots;

    logger.info("SlotManager initialized: totalSlots=" + totalSlots
        + " slotSizeMbps=" + slotSizeMbps
        + " tickIntervalMs=" + tickIntervalMs
        + " emaWindowSec=" + emaWindowSec
        + " postEvictionCooldownMs=" + postEvictionCooldownMs
        + " drainLatchEnabled=" + drainLatchEnabled
        + " drainLatchEmaWindowSec=" + drainLatchWindowSec
        + " drainLatchDisengageFreeSlots=" + drainLatchDisengageFreeSlots
        + " maxSlotStep=" + maxSlotStep);
  }

  /**
   * Hot path -- called on every write request.
   * Zero-allocation on steady state: two ConcurrentHashMap.get() calls using the caller's strings.
   *
   * @param pid the producer identifier
   * @param topic the topic name
   * @param bytes the number of bytes written
   */
  public void recordWrite(String pid, String topic, int bytes) {
    ConcurrentHashMap<String, ProducerSlotState> topicMap =
        producers.computeIfAbsent(pid, k -> new ConcurrentHashMap<>());
    ProducerSlotState state = topicMap.computeIfAbsent(topic, k -> new ProducerSlotState());
    state.bytesAccumulator.add(bytes);
    state.lastWriteMs = System.currentTimeMillis();
  }

  public void start() {
    tickExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
      Thread t = new Thread(r, "slot-tick");
      t.setDaemon(true);
      return t;
    });
    tickExecutor.scheduleAtFixedRate(this::tick, tickIntervalMs, tickIntervalMs,
        TimeUnit.MILLISECONDS);
    logger.info("SlotManager tick started (interval=" + tickIntervalMs + "ms)");
  }

  public void stop() {
    if (tickExecutor != null) {
      tickExecutor.shutdownNow();
    }
    logger.info("SlotManager stopped");
  }

  /**
   * Background tick -- runs every tickIntervalMs on a single thread.
   * Computes EMA rates and adjusts slot allocations.
   */
  void tick() {
    try {
      long now = System.currentTimeMillis();

      Iterator<Map.Entry<String, ConcurrentHashMap<String, ProducerSlotState>>> outerIt =
          producers.entrySet().iterator();
      while (outerIt.hasNext()) {
        Map.Entry<String, ConcurrentHashMap<String, ProducerSlotState>> outerEntry = outerIt.next();
        String pid = outerEntry.getKey();
        ConcurrentHashMap<String, ProducerSlotState> topicMap = outerEntry.getValue();

        Iterator<Map.Entry<String, ProducerSlotState>> innerIt = topicMap.entrySet().iterator();
        while (innerIt.hasNext()) {
          Map.Entry<String, ProducerSlotState> innerEntry = innerIt.next();
          String topic = innerEntry.getKey();
          ProducerSlotState state = innerEntry.getValue();

          long bytes = state.bytesAccumulator.sumThenReset();
          double instantRateMbps = bytes / (tickIntervalSec * BYTES_PER_MB);

          state.emaRateMbps = emaDecay * state.emaRateMbps + (1 - emaDecay) * instantRateMbps;

          // Register gauges lazily on first tick we see this (pid, topic).
          // Re-registering each tick would be a no-op for codahale but adds
          // unnecessary CHM contention.
          registerProducerMetrics(pid, topic, state);

          int expectedSlots = (int) ceil(state.emaRateMbps / slotSizeMbps);
          int currentSlots = state.currentSlots;

          if (expectedSlots > currentSlots) {
            state.belowSinceMs = 0;
            if (state.exceedsSinceMs == 0) {
              state.exceedsSinceMs = now;
            }
            double exceedsDuration = now - state.exceedsSinceMs;
            if (exceedsDuration >= acquireThresholdMs) {
              // Clamp the per-tick step so load is picked up gradually (one
              // small increment per tick) instead of closing the whole EMA gap
              // at once, which overshoots and drives oscillation.
              int slotsToAcquire = clampStep(expectedSlots - currentSlots);
              if (tryAcquireSlots(pid, topic, state, slotsToAcquire, now)) {
                state.exceedsSinceMs = 0;
              }
            }
          } else if (expectedSlots < currentSlots) {
            state.exceedsSinceMs = 0;
            if (state.belowSinceMs == 0) {
              state.belowSinceMs = now;
            }
            double belowDuration = now - state.belowSinceMs;
            if (belowDuration >= releaseThresholdMs) {
              // Clamp the per-tick step (symmetric with acquisition) so a broker
              // sheds load gradually instead of dropping the whole gap at once.
              int slotsToRelease = clampStep(currentSlots - expectedSlots);
              releaseSlots(pid, topic, topicMap, state, slotsToRelease);
              state.belowSinceMs = 0;
            }
          } else {
            state.exceedsSinceMs = 0;
            state.belowSinceMs = 0;
          }

          if (now - state.lastWriteMs > idleProducerTimeoutMs) {
            if (state.currentSlots > 0) {
              decrementSlots(pid, topic, topicMap, state, state.currentSlots);
              logger.info("Released idle producer: " + pid + "/" + topic);
            } else {
              removeProducerTopic(pid, topic, topicMap);
            }
          }
        }
      }

      updateDrainLatch();

      // Drop expired post-eviction cooldowns. Expired entries no longer block
      // acquisition, so removing them is safe; active (future) entries are
      // retained even when the producer holds 0 slots, which is the whole
      // point of keeping the cooldown off ProducerSlotState.
      evictionCooldownUntilMs.entrySet().removeIf(e -> now > e.getValue());
    } catch (Exception e) {
      logger.log(Level.WARNING, "Error in slot accounting tick", e);
    }
  }

  /**
   * Recompute the smoothed free-slot count and the latched drain state from
   * end-of-tick occupancy. Running this once per tick (after all slot
   * adjustments) means {@link #tryAcquireSlots} consumes the value computed
   * from the previous tick, so the decision is stable within a tick.
   * <p>
   * The smoothing window (see {@code drainLatchEmaWindowSeconds}) is longer
   * than the eviction "flap" period, so the brief {@code free=1} spike that a
   * single eviction manufactures cannot by itself disengage the latch. The
   * latch engages once free slots have been near zero and disengages only
   * after the broker has genuinely drained by {@code drainLatchDisengageFreeSlots}.
   */
  private void updateDrainLatch() {
    if (!drainLatchEnabled) {
      return;
    }
    freeSlotsEma = drainLatchEmaDecay * freeSlotsEma
        + (1 - drainLatchEmaDecay) * getFreeSlots();
    if (drainLatched) {
      if (freeSlotsEma >= drainLatchDisengageFreeSlots) {
        drainLatched = false;
        logger.info("Drain latch disengaged: freeSlotsEma="
            + String.format("%.2f", freeSlotsEma)
            + " >= disengageFreeSlots=" + drainLatchDisengageFreeSlots);
      }
    } else if (freeSlotsEma < 1.0) {
      drainLatched = true;
      logger.info("Drain latch engaged: freeSlotsEma="
          + String.format("%.2f", freeSlotsEma)
          + " (recent free slots near zero); freezing slot acquisition until"
          + " drained to freeSlots=" + drainLatchDisengageFreeSlots);
    }
  }

  /**
   * Bound an EMA-driven slot delta to at most {@code maxSlotStep} per tick so
   * acquisition/release move gradually. A non-positive {@code maxSlotStep}
   * disables the clamp (legacy whole-gap behavior).
   */
  private int clampStep(int desiredDelta) {
    if (maxSlotStep <= 0) {
      return desiredDelta;
    }
    return Math.min(desiredDelta, maxSlotStep);
  }

  private boolean tryAcquireSlots(String pid, String topic, ProducerSlotState state,
                                  int count, long now) {
    if (now - lastSlotChangeTimeMs < cooldownMs) {
      return false;
    }
    Long cooldownUntil = evictionCooldownUntilMs.get(cooldownKey(pid, topic));
    if (cooldownUntil != null && now < cooldownUntil) {
      // Recently evicted; let the producer's EMA settle to its post-eviction
      // steady state before we consider re-acquiring. Without this gate the
      // broker reacquires the same slot the moment the global cooldown +
      // acquireThreshold elapse, because the EMA still reflects pre-eviction
      // throughput -- the source of the production eviction "flap".
      return false;
    }
    if (drainLatched) {
      // Broker has recently been at near-zero free slots. Under backpressure
      // the shaper refills any freed capacity, so granting a slot here just
      // re-occupies the slot eviction freed and the broker flaps. Refuse all
      // acquisition until the broker has genuinely drained (see updateDrainLatch).
      return false;
    }
    int available = totalSlots - totalOccupiedSlots.get();
    int actual = Math.min(count, available);
    if (actual <= 0) {
      return false;
    }
    state.currentSlots += actual;
    totalOccupiedSlots.addAndGet(actual);
    lastSlotChangeTimeMs = now;
    logger.info("+" + actual + " slot(s) for pid=" + pid + "/" + topic
        + " | total=" + state.currentSlots
        + " | occupied=" + totalOccupiedSlots.get() + "/" + totalSlots
        + ipLogSuffix(pid));
    return true;
  }

  /**
   * Single point for all slot decrements. Adjusts totalOccupiedSlots, sets
   * lastSlotChangeTimeMs, and removes the topic/producer entry when the
   * producer's slot count for that topic reaches 0.
   *
   * Every path that reduces currentSlots MUST go through this method so
   * the invariant "no zero-slot entries in the map" is maintained in one place.
   *
   * @return actual number of slots released
   */
  private int decrementSlots(String pid, String topic,
                             ConcurrentHashMap<String, ProducerSlotState> topicMap,
                             ProducerSlotState state, int count) {
    int actual = Math.min(count, state.currentSlots);
    if (actual <= 0) {
      return 0;
    }
    state.currentSlots -= actual;
    totalOccupiedSlots.addAndGet(-actual);
    lastSlotChangeTimeMs = System.currentTimeMillis();
    if (state.currentSlots == 0) {
      removeProducerTopic(pid, topic, topicMap);
    }
    return actual;
  }

  private void removeProducerTopic(String pid, String topic,
                                   ConcurrentHashMap<String, ProducerSlotState> topicMap) {
    topicMap.remove(topic);
    deregisterProducerMetrics(pid, topic);
    if (topicMap.isEmpty()) {
      // If the producer has no more topics, drop it from the v4 connection
      // registry too so that registry doesn't leak across producer churn.
      if (producers.remove(pid, topicMap)) {
        producerConnections.remove(pid);
        producerIps.remove(pid);
      }
    }
  }

  private void releaseSlots(String pid, String topic,
                            ConcurrentHashMap<String, ProducerSlotState> topicMap,
                            ProducerSlotState state, int count) {
    int actual = decrementSlots(pid, topic, topicMap, state, count);
    if (actual > 0) {
      logger.info("-" + actual + " slot(s) for pid=" + pid + "/" + topic
          + " | total=" + state.currentSlots
          + " | occupied=" + totalOccupiedSlots.get() + "/" + totalSlots
          + ipLogSuffix(pid));
    }
  }

  public int getFreeSlots() {
    return totalSlots - totalOccupiedSlots.get();
  }

  public boolean isFrozen() {
    return System.currentTimeMillis() - lastSlotChangeTimeMs < cooldownMs
        || totalOccupiedSlots.get() >= totalSlots
        || drainLatched;
  }

  /**
   * Whether the broker is currently in the drain-latched state: it has
   * recently been at near-zero free slots, so slot acquisition is frozen until
   * it has drained. Visible for test and metrics.
   *
   * @return {@code true} if the drain latch is engaged; {@code false} otherwise.
   */
  public boolean isDrainLatched() {
    return drainLatched;
  }

  /**
   * Registry/lookup key for the per-(pid, topic) post-eviction cooldown map.
   * pid (UUID or IPv4) and topic strings as used in this codebase do not
   * contain {@code |}, so simple concatenation is collision-free.
   */
  private static String cooldownKey(String pid, String topic) {
    return pid + "|" + topic;
  }

  public int getTotalSlots() {
    return totalSlots;
  }

  public int getOccupiedSlots() {
    return totalOccupiedSlots.get();
  }

  public int getProducerCount() {
    int count = 0;
    for (ConcurrentHashMap<String, ProducerSlotState> topicMap : producers.values()) {
      count += topicMap.size();
    }
    return count;
  }

  public int getProducerSlots(String pid, String topic) {
    ConcurrentHashMap<String, ProducerSlotState> topicMap = producers.get(pid);
    if (topicMap == null) {
      return 0;
    }
    ProducerSlotState state = topicMap.get(topic);
    return state != null ? state.currentSlots : 0;
  }

  public double getProducerEmaRate(String pid, String topic) {
    ConcurrentHashMap<String, ProducerSlotState> topicMap = producers.get(pid);
    if (topicMap == null) {
      return 0.0;
    }
    ProducerSlotState state = topicMap.get(topic);
    return state != null ? state.emaRateMbps : 0.0;
  }

  /**
   * Build the codahale registry key for a (pid, topic) gauge using the
   * inline-tag convention recognised by
   * {@link com.pinterest.memq.commons.mon.OpenTSDBReporter}: the segment
   * before the first {@code |} is the emitted metric name; everything
   * after is appended verbatim as space-separated OpenTSDB tag tokens.
   * <p>
   * pid (UUID or IPv4 dotted-quad) and topic strings as used in this
   * codebase only contain characters allowed in OpenTSDB tag values
   * ({@code [a-zA-Z0-9-_./]}), so no escaping is needed.
   * <p>
   * Visible for test.
   */
  static String encodeMetricKey(String metricName, String pid, String topic) {
    return metricName + "|pid=" + pid + "|topic=" + topic;
  }

  /**
   * Register {@code producer.ema} and {@code producer.slots} gauges for
   * the (pid, topic) pair, once. No-op when no registry was supplied at
   * construction time (test-only path) or when the gauge is already
   * registered.
   */
  private void registerProducerMetrics(String pid, String topic, ProducerSlotState state) {
    if (registry == null) {
      return;
    }
    String emaKey = encodeMetricKey(EMA_METRIC_NAME, pid, topic);
    if (registeredEmaKeys.add(emaKey)) {
      registry.gauge(emaKey, () -> (Gauge<Double>) () -> state.emaRateMbps);
    }
    String slotsKey = encodeMetricKey(SLOTS_METRIC_NAME, pid, topic);
    if (registeredSlotsKeys.add(slotsKey)) {
      registry.gauge(slotsKey, () -> (Gauge<Integer>) () -> state.currentSlots);
    }
  }

  /**
   * Mirror of {@link #registerProducerMetrics(String, String, ProducerSlotState)}:
   * removes both gauges from the registry and clears the bookkeeping sets
   * so a future re-registration (if the producer reappears) is a clean
   * insert. Tolerates {@code null} registry.
   */
  private void deregisterProducerMetrics(String pid, String topic) {
    if (registry == null) {
      return;
    }
    String emaKey = encodeMetricKey(EMA_METRIC_NAME, pid, topic);
    if (registeredEmaKeys.remove(emaKey)) {
      registry.remove(emaKey);
    }
    String slotsKey = encodeMetricKey(SLOTS_METRIC_NAME, pid, topic);
    if (registeredSlotsKeys.remove(slotsKey)) {
      registry.remove(slotsKey);
    }
  }

  /**
   * Force-release all slots and remove all per-producer accounting entries for
   * the given {@code topic}, across every producer that holds state for it.
   * <p>
   * Called when the local {@link com.pinterest.memq.core.processing.TopicProcessor}
   * is decommissioned: without this, {@code (pid, topic)} entries would linger
   * in {@link #producers} until the {@code idleProducerTimeoutMs} branch of
   * {@link #tick()} reclaimed them via EMA decay, during which the broker
   * would gossip an artificially low {@code freeSlots} and could spuriously
   * evict producers on the topics it still serves.
   * <p>
   * The caller MUST invoke this <i>after</i> removing the topic processor
   * from {@code MemqManager.processorMap} so that {@code PacketSwitchingHandler}
   * routes new writes for {@code topic} into the {@code REDIRECT} branch
   * before this method runs. A request that already passed the
   * {@code processorMap.get} check before the removal may still call
   * {@link #recordWrite} after this method returns; the recreated entry
   * starts at zero slots and zero EMA, occupies no capacity, and is
   * reclaimed by the next idle-timeout pass.
   * <p>
   * Concurrency follows the same contract as
   * {@link #releaseProducerSlots(String, String, int)}: callable from any
   * thread, uses the existing {@link #decrementSlots} for atomic
   * {@code totalOccupiedSlots} and {@code lastSlotChangeTimeMs} updates.
   *
   * @param topic the topic whose per-producer slot state and capacity should
   *        be released. No-op if no producer holds state for the topic.
   */
  public void dropTopic(String topic) {
    int affected = 0;
    int slotsReleased = 0;
    for (Map.Entry<String, ConcurrentHashMap<String, ProducerSlotState>> outer
        : producers.entrySet()) {
      String pid = outer.getKey();
      ConcurrentHashMap<String, ProducerSlotState> topicMap = outer.getValue();
      ProducerSlotState state = topicMap.get(topic);
      if (state == null) {
        continue;
      }
      if (state.currentSlots > 0) {
        slotsReleased += decrementSlots(pid, topic, topicMap, state, state.currentSlots);
      } else {
        removeProducerTopic(pid, topic, topicMap);
      }
      // The topic is being decommissioned on this broker, so any pending
      // post-eviction cooldown for it is moot -- drop it to avoid a leak.
      evictionCooldownUntilMs.remove(cooldownKey(pid, topic));
      affected++;
    }
    logger.info("dropTopic: cleared accounting for topic=" + topic
        + " affectedProducers=" + affected
        + " slotsReleased=" + slotsReleased
        + " freeSlots=" + getFreeSlots() + "/" + totalSlots);
  }

  /**
   * Force-release slots for a specific producer. Used by the eviction path.
   *
   * @param pid the producer identifier
   * @param topic the topic name
   * @param count the number of slots to release
   * @return the actual number of slots released
   */
  public int releaseProducerSlots(String pid, String topic, int count) {
    ConcurrentHashMap<String, ProducerSlotState> topicMap = producers.get(pid);
    if (topicMap == null) {
      return 0;
    }
    ProducerSlotState state = topicMap.get(topic);
    if (state == null) {
      return 0;
    }
    // Arm the per-(pid, topic) acquisition cooldown in the manager-level map
    // rather than on the ProducerSlotState, because decrementSlots removes the
    // state from the map when slots drop to 0. Storing it here lets the
    // cooldown survive that removal (and the fresh state a subsequent
    // recordWrite creates), which is what prevents the just-evicted producer
    // from re-acquiring after acquireThreshold instead of the full cooldown.
    long cooldownUntil = System.currentTimeMillis() + postEvictionCooldownMs;
    evictionCooldownUntilMs.put(cooldownKey(pid, topic), cooldownUntil);
    int actual = decrementSlots(pid, topic, topicMap, state, count);
    if (actual > 0) {
      logger.info("Eviction: -" + actual + " slot(s) for pid=" + pid + "/" + topic
          + " | remaining=" + state.currentSlots
          + " | occupied=" + totalOccupiedSlots.get() + "/" + totalSlots
          + " | cooldownUntilMs=" + cooldownUntil
          + ipLogSuffix(pid));
    }
    return actual;
  }

  /**
   * Total slots held by a producer across all topics.
   * Direct read from the live structure — no allocation.
   *
   * @param pid the producer identifier
   * @return the total number of slots held by this producer
   */
  public int getTotalProducerSlots(String pid) {
    ConcurrentHashMap<String, ProducerSlotState> topicMap = producers.get(pid);
    if (topicMap == null) {
      return 0;
    }
    int total = 0;
    for (ProducerSlotState state : topicMap.values()) {
      total += state.currentSlots;
    }
    return total;
  }

  /**
   * Unmodifiable view of topic names for which this producer holds slots.
   * Zero allocation — backed by the live ConcurrentHashMap keySet.
   * Safe to iterate (weakly consistent). Callers must not cache the reference.
   *
   * @param pid the producer identifier
   * @return unmodifiable collection of topic names
   */
  public Collection<String> getProducerTopics(String pid) {
    ConcurrentHashMap<String, ProducerSlotState> topicMap = producers.get(pid);
    if (topicMap == null) {
      return Collections.emptySet();
    }
    return Collections.unmodifiableSet(topicMap.keySet());
  }

  /**
   * Whether the given producer currently holds any slots.  O(1).
   *
   * @param pid the producer identifier
   * @return true if the producer holds at least one slot
   */
  public boolean producerHasSlots(String pid) {
    return producers.containsKey(pid);
  }

  /**
   * Unmodifiable view of producer IDs that currently hold at least one slot.
   * Zero allocation — backed by the live ConcurrentHashMap keySet.
   *
   * @return unmodifiable set of producer identifiers
   */
  public Set<String> getProducerIdsWithSlots() {
    return Collections.unmodifiableSet(producers.keySet());
  }

  /**
   * Record which brokers a producer is currently connected to.
   * Called on the write request hot path for v4 producers only.
   * <p>
   * This map doubles as the <b>v4 producer registry</b>: only producers
   * present here are eligible for eviction. v3 producers never call this
   * method, so they are naturally excluded from eviction decisions.
   * <p>
   * The {@code connections} set may be empty during bootstrap (the producer
   * has not yet learned about its slot ownership from any broker). An empty
   * set still registers the producer as v4-capable; the eviction strategy
   * gracefully falls back to "any v4 producer" when no producer has a known
   * connection to the eviction target.
   *
   * @param pid the producer identifier
   * @param connections the set of broker IPs this producer is connected to (may be empty)
   */
  public void recordProducerConnections(String pid, Set<String> connections) {
    producerConnections.put(pid, connections);
  }

  /**
   * Record the remote IP a producer writes from. Hot-path friendly: a plain
   * {@code get} on the common path with a {@code put} only when the IP is new or
   * changed (which is rare -- a producer keeps the same id/host for its life).
   *
   * @param pid the producer identifier
   * @param ip the remote IP of the producer's connection
   */
  public void recordProducerIp(String pid, String ip) {
    if (ip == null) {
      return;
    }
    if (!ip.equals(producerIps.get(pid))) {
      producerIps.put(pid, ip);
    }
  }

  /**
   * @param pid the producer identifier
   * @return the last known remote IP for the producer, or {@code null}
   */
  public String getProducerIp(String pid) {
    return producerIps.get(pid);
  }

  /**
   * Render a {@code " | ip=<ip>"} suffix for log lines keyed by an opaque
   * producer id, or an empty string when the IP is unknown.
   */
  private String ipLogSuffix(String pid) {
    String ip = producerIps.get(pid);
    return ip == null ? "" : " | ip=" + ip;
  }

  /**
   * Get the current producer connections map (v4 producers only).
   * Used by EvictionManager to pass to the eviction strategy.
   *
   * @return unmodifiable map of producer IDs to their connected broker IPs
   */
  public Map<String, Set<String>> getProducerConnections() {
    return Collections.unmodifiableMap(producerConnections);
  }

  /**
   * Per-producer slot accounting state.
   * {@code bytesAccumulator} and {@code lastWriteMs} are written by the hot path.
   * All other fields are only read/written by the single background tick thread.
   */
  static class ProducerSlotState {
    final LongAdder bytesAccumulator = new LongAdder();
    volatile long lastWriteMs;

    double emaRateMbps;
    int currentSlots;
    long exceedsSinceMs;
    long belowSinceMs;
  }
}

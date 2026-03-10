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
import java.util.HashSet;
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

  private final int totalSlots;
  private final double slotSizeMbps;
  private final double acquireThresholdMs;
  private final double releaseThresholdMs;
  private final double cooldownMs;
  private final double emaDecay;
  private final double tickIntervalSec;
  private final long tickIntervalMs;
  private final long idleProducerTimeoutMs;

  private final ConcurrentHashMap<String, ConcurrentHashMap<String, ProducerSlotState>> producers =
      new ConcurrentHashMap<>();
  private final AtomicInteger totalOccupiedSlots = new AtomicInteger(0);
  private volatile long lastSlotChangeTimeMs = 0;

  private final MetricRegistry registry;
  private final Set<String> registeredProducerMetrics = new HashSet<>();
  private final ConcurrentHashMap<String, Set<String>> producerConnections = new ConcurrentHashMap<>();

  private ScheduledExecutorService tickExecutor;

  public SlotManager(SlotAccountingConfig config, int totalSlots) {
    this(config, totalSlots, null);
  }

  public SlotManager(SlotAccountingConfig config, int totalSlots, MetricRegistry registry) {
    this.registry = registry;
    this.totalSlots = totalSlots;
    this.slotSizeMbps = config.getSlotSizeMbps();
    this.acquireThresholdMs = config.getAcquireThresholdSeconds() * 1000.0;
    this.releaseThresholdMs = config.getReleaseThresholdSeconds() * 1000.0;
    this.cooldownMs = config.getCooldownSeconds() * 1000.0;
    this.tickIntervalMs = config.getTickIntervalMs();
    this.tickIntervalSec = tickIntervalMs / 1000.0;
    this.idleProducerTimeoutMs = config.getIdleProducerTimeoutMs();

    double emaWindowSec = config.getEmaWindowSeconds();
    this.emaDecay = exp(-tickIntervalSec / emaWindowSec);

    logger.info("SlotManager initialized: totalSlots=" + totalSlots
        + " slotSizeMbps=" + slotSizeMbps
        + " tickIntervalMs=" + tickIntervalMs
        + " emaWindowSec=" + emaWindowSec);
  }

  /**
   * Hot path -- called on every write request.
   * Zero-allocation on steady state: two ConcurrentHashMap.get() calls using the caller's strings.
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

          registerProducerMetrics(pid, topic);

          int expectedSlots = (int) ceil(state.emaRateMbps / slotSizeMbps);
          int currentSlots = state.currentSlots;

          if (expectedSlots > currentSlots) {
            state.belowSinceMs = 0;
            if (state.exceedsSinceMs == 0) {
              state.exceedsSinceMs = now;
            }
            double exceedsDuration = now - state.exceedsSinceMs;
            if (exceedsDuration >= acquireThresholdMs) {
              int slotsToAcquire = expectedSlots - currentSlots;
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
              int slotsToRelease = currentSlots - expectedSlots;
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
            deregisterProducerMetrics(pid, topic);
          }
        }
      }
    } catch (Exception e) {
      logger.log(Level.WARNING, "Error in slot accounting tick", e);
    }
  }

  private boolean tryAcquireSlots(String pid, String topic, ProducerSlotState state,
                                  int count, long now) {
    if (now - lastSlotChangeTimeMs < cooldownMs) {
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
        + " | occupied=" + totalOccupiedSlots.get() + "/" + totalSlots);
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
    if (topicMap.isEmpty()) {
      producers.remove(pid, topicMap);
    }
  }

  private void releaseSlots(String pid, String topic,
                            ConcurrentHashMap<String, ProducerSlotState> topicMap,
                            ProducerSlotState state, int count) {
    int actual = decrementSlots(pid, topic, topicMap, state, count);
    if (actual > 0) {
      logger.info("-" + actual + " slot(s) for pid=" + pid + "/" + topic
          + " | total=" + state.currentSlots
          + " | occupied=" + totalOccupiedSlots.get() + "/" + totalSlots);
    }
  }

  public int getFreeSlots() {
    return totalSlots - totalOccupiedSlots.get();
  }

  public boolean isFrozen() {
    return System.currentTimeMillis() - lastSlotChangeTimeMs < cooldownMs
        || totalOccupiedSlots.get() >= totalSlots;
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

  static String sanitize(String value) {
    return value.replace('.', '_').replace(':', '_');
  }

  static String metricSuffix(String pid, String topic) {
    return sanitize(pid) + "." + sanitize(topic);
  }

  private void registerProducerMetrics(String pid, String topic) {
    if (registry == null) {
      return;
    }
    String suffix = metricSuffix(pid, topic);
    String emaKey = "producer.ema." + suffix;
    if (registeredProducerMetrics.add(emaKey)) {
      registry.gauge(emaKey, () -> (Gauge<Double>) () -> {
        ConcurrentHashMap<String, ProducerSlotState> topicMap = producers.get(pid);
        if (topicMap == null) return 0.0;
        ProducerSlotState s = topicMap.get(topic);
        return s != null ? s.emaRateMbps : 0.0;
      });
      String slotsKey = "producer.slots." + suffix;
      registeredProducerMetrics.add(slotsKey);
      registry.gauge(slotsKey, () -> (Gauge<Integer>) () -> {
        ConcurrentHashMap<String, ProducerSlotState> topicMap = producers.get(pid);
        if (topicMap == null) return 0;
        ProducerSlotState s = topicMap.get(topic);
        return s != null ? s.currentSlots : 0;
      });
    }
  }

  private void deregisterProducerMetrics(String pid, String topic) {
    if (registry == null) {
      return;
    }
    String suffix = metricSuffix(pid, topic);
    String emaKey = "producer.ema." + suffix;
    String slotsKey = "producer.slots." + suffix;
    registry.remove(emaKey);
    registry.remove(slotsKey);
    registeredProducerMetrics.remove(emaKey);
    registeredProducerMetrics.remove(slotsKey);
  }

  /**
   * Force-release slots for a specific producer. Used by the eviction path.
   *
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
    int actual = decrementSlots(pid, topic, topicMap, state, count);
    if (actual > 0) {
      logger.info("Eviction: -" + actual + " slot(s) for pid=" + pid + "/" + topic
          + " | remaining=" + state.currentSlots
          + " | occupied=" + totalOccupiedSlots.get() + "/" + totalSlots);
    }
    return actual;
  }

  /**
   * Total slots held by a producer across all topics.
   * Direct read from the live structure — no allocation.
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
   */
  public boolean producerHasSlots(String pid) {
    return producers.containsKey(pid);
  }

  /**
   * Unmodifiable view of producer IDs that currently hold at least one slot.
   * Zero allocation — backed by the live ConcurrentHashMap keySet.
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
   */
  public void recordProducerConnections(String pid, Set<String> connections) {
    producerConnections.put(pid, connections);
  }

  /**
   * Get the current producer connections map (v4 producers only).
   * Used by EvictionManager to pass to the eviction strategy.
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

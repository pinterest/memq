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

  private final ConcurrentHashMap<String, ProducerSlotState> producers = new ConcurrentHashMap<>();
  private final AtomicInteger totalOccupiedSlots = new AtomicInteger(0);
  private volatile long lastSlotChangeTimeMs = 0;

  private final MetricRegistry registry;
  private final Set<String> registeredProducerMetrics = new HashSet<>();

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
   * Non-blocking: LongAdder.add + volatile write.
   */
  public void recordWrite(String producerId, int bytes) {
    ProducerSlotState state = producers.computeIfAbsent(producerId, k -> new ProducerSlotState());
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

      Iterator<Map.Entry<String, ProducerSlotState>> it = producers.entrySet().iterator();
      while (it.hasNext()) {
        Map.Entry<String, ProducerSlotState> entry = it.next();
        String pid = entry.getKey();
        ProducerSlotState state = entry.getValue();

        long bytes = state.bytesAccumulator.sumThenReset();
        double instantRateMbps = bytes / (tickIntervalSec * BYTES_PER_MB);

        state.emaRateMbps = emaDecay * state.emaRateMbps + (1 - emaDecay) * instantRateMbps;

        registerProducerMetrics(pid, state);

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
            if (tryAcquireSlots(pid, state, slotsToAcquire, now)) {
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
            releaseSlots(pid, state, slotsToRelease, now);
            state.belowSinceMs = 0;
          }
        } else {
          state.exceedsSinceMs = 0;
          state.belowSinceMs = 0;
        }

        if (now - state.lastWriteMs > idleProducerTimeoutMs) {
          if (state.currentSlots > 0) {
            totalOccupiedSlots.addAndGet(-state.currentSlots);
            lastSlotChangeTimeMs = now;
            logger.info("Released " + state.currentSlots + " slot(s) from idle producer: " + pid);
          }
          it.remove();
          deregisterProducerMetrics(pid);
          logger.fine("Removed idle producer: " + pid);
        }
      }
    } catch (Exception e) {
      logger.log(Level.WARNING, "Error in slot accounting tick", e);
    }
  }

  private boolean tryAcquireSlots(String pid, ProducerSlotState state, int count, long now) {
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
    logger.info("+" + actual + " slot(s) for pid=" + pid
        + " | total=" + state.currentSlots
        + " | occupied=" + totalOccupiedSlots.get() + "/" + totalSlots);
    return true;
  }

  private void releaseSlots(String pid, ProducerSlotState state, int count, long now) {
    int actual = Math.min(count, state.currentSlots);
    if (actual <= 0) {
      return;
    }
    state.currentSlots -= actual;
    totalOccupiedSlots.addAndGet(-actual);
    lastSlotChangeTimeMs = now;
    logger.info("-" + actual + " slot(s) for pid=" + pid
        + " | total=" + state.currentSlots
        + " | occupied=" + totalOccupiedSlots.get() + "/" + totalSlots);
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
    return producers.size();
  }

  public int getProducerSlots(String producerId) {
    ProducerSlotState state = producers.get(producerId);
    return state != null ? state.currentSlots : 0;
  }

  public double getProducerEmaRate(String producerId) {
    ProducerSlotState state = producers.get(producerId);
    return state != null ? state.emaRateMbps : 0.0;
  }

  static String sanitizeProducerId(String producerId) {
    return producerId.replace('.', '_').replace(':', '_');
  }

  private void registerProducerMetrics(String pid, ProducerSlotState state) {
    if (registry == null) {
      return;
    }
    String sanitized = sanitizeProducerId(pid);
    String emaKey = "producer.ema." + sanitized;
    if (registeredProducerMetrics.add(emaKey)) {
      registry.gauge(emaKey, () -> (Gauge<Double>) () -> {
        ProducerSlotState s = producers.get(pid);
        return s != null ? s.emaRateMbps : 0.0;
      });
      String slotsKey = "producer.slots." + sanitized;
      registeredProducerMetrics.add(slotsKey);
      registry.gauge(slotsKey, () -> (Gauge<Integer>) () -> {
        ProducerSlotState s = producers.get(pid);
        return s != null ? s.currentSlots : 0;
      });
    }
  }

  private void deregisterProducerMetrics(String pid) {
    if (registry == null) {
      return;
    }
    String sanitized = sanitizeProducerId(pid);
    String emaKey = "producer.ema." + sanitized;
    String slotsKey = "producer.slots." + sanitized;
    registry.remove(emaKey);
    registry.remove(slotsKey);
    registeredProducerMetrics.remove(emaKey);
    registeredProducerMetrics.remove(slotsKey);
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

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
package com.pinterest.memq.core.eviction;

import com.pinterest.memq.core.config.EvictionConfig;
import com.pinterest.memq.core.gossip.GossipState;
import com.pinterest.memq.core.slot.SlotManager;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Periodically runs an {@link EvictionStrategy} and stores at most one
 * pending {@link EvictionResult} per producer IP. The response path
 * atomically polls and removes pending evictions via {@link #pollEviction}.
 */
public class EvictionManager {

  private static final Logger logger = Logger.getLogger(EvictionManager.class.getName());

  private final EvictionStrategy strategy;
  private final SlotManager slotManager;
  private final Supplier<Map<String, GossipState>> peerStatesSupplier;
  private final EvictionConfig config;
  private final ConcurrentHashMap<String, EvictionResult> pendingEvictions = new ConcurrentHashMap<>();

  private ScheduledExecutorService executor;

  public EvictionManager(EvictionStrategy strategy,
                         SlotManager slotManager,
                         Supplier<Map<String, GossipState>> peerStatesSupplier,
                         EvictionConfig config) {
    this.strategy = strategy;
    this.slotManager = slotManager;
    this.peerStatesSupplier = peerStatesSupplier;
    this.config = config;
  }

  public void start() {
    executor = Executors.newSingleThreadScheduledExecutor(r -> {
      Thread t = new Thread(r, "eviction-manager");
      t.setDaemon(true);
      return t;
    });
    long intervalMs = (long) (config.getIntervalSeconds() * 1000);
    long initialDelayMs = ThreadLocalRandom.current().nextLong(intervalMs);
    executor.scheduleAtFixedRate(this::runEviction, initialDelayMs, intervalMs,
        TimeUnit.MILLISECONDS);
    logger.info("EvictionManager started (interval=" + intervalMs + "ms)");
  }

  public void stop() {
    if (executor != null) {
      executor.shutdownNow();
    }
    logger.info("EvictionManager stopped");
  }

  void runEviction() {
    try {
      Map<String, GossipState> peerStates = peerStatesSupplier.get();
      Map<String, Set<String>> producerConnections = slotManager.getProducerConnections();
      EvictionResult result = strategy.evaluate(slotManager, peerStates, producerConnections);
      if (result != null) {
        pendingEvictions.put(result.getPid(), result);
        logger.info("Eviction scheduled: " + result);
      }
    } catch (Exception e) {
      logger.log(Level.WARNING, "Error in eviction run", e);
    }
  }

  /**
   * Atomically retrieve and remove a pending eviction for the given producer.
   *
   * @param producerIp the producer identifier (UUID for v4, IP for v3)
   * @return the pending EvictionResult, or null if none exists
   */
  public EvictionResult pollEviction(String producerIp) {
    return pendingEvictions.remove(producerIp);
  }

  public EvictionResult peekEviction(String producerIp) {
    return pendingEvictions.get(producerIp);
  }

  public int getPendingCount() {
    return pendingEvictions.size();
  }
}

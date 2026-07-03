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

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
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
  private final Supplier<Map<String, Set<String>>> topicToBrokerIpsSupplier;
  private final Supplier<Set<String>> balancingEnabledTopicsSupplier;
  private final EvictionConfig config;
  private final ConcurrentHashMap<String, EvictionResult> pendingEvictions = new ConcurrentHashMap<>();

  private ScheduledExecutorService executor;

  /**
   * @param strategy the eviction strategy to apply each tick.
   * @param slotManager the local broker's slot manager (source of producer
   *        connections, slot counts and drain-latch state).
   * @param peerStatesSupplier supplies the current peer gossip state.
   * @param topicToBrokerIpsSupplier supplies a map of topic to broker IPs
   *        that serve writes for that topic. Used by the strategy to
   *        constrain eviction targets to brokers that actually serve the
   *        evicted producer's topics. Pass {@link Collections#emptyMap}
   *        to disable topic-aware filtering (e.g. in tests).
   * @param balancingEnabledTopicsSupplier supplies the set of topic names for
   *        which per-topic balancing is opted in ({@code TopicConfig.enableBalancing}).
   *        Only producers writing to at least one such topic are eligible for
   *        eviction. Read live on every tick so runtime topic-config changes
   *        take effect without a restart. Pass {@code null} to disable
   *        per-topic filtering entirely (all producers eligible), e.g. in tests.
   * @param config eviction configuration (interval, thresholds, top-N sizes).
   */
  public EvictionManager(EvictionStrategy strategy,
                         SlotManager slotManager,
                         Supplier<Map<String, GossipState>> peerStatesSupplier,
                         Supplier<Map<String, Set<String>>> topicToBrokerIpsSupplier,
                         Supplier<Set<String>> balancingEnabledTopicsSupplier,
                         EvictionConfig config) {
    this.strategy = strategy;
    this.slotManager = slotManager;
    this.peerStatesSupplier = peerStatesSupplier;
    this.topicToBrokerIpsSupplier = topicToBrokerIpsSupplier;
    this.balancingEnabledTopicsSupplier = balancingEnabledTopicsSupplier;
    this.config = config;
  }

  /**
   * Convenience constructor for tests with no topic affinity and no per-topic
   * balancing filter (all producers eligible).
   *
   * @param strategy the eviction strategy to apply each tick.
   * @param slotManager the local broker's slot manager.
   * @param peerStatesSupplier supplies the current peer gossip state.
   * @param config eviction configuration.
   */
  public EvictionManager(EvictionStrategy strategy,
                         SlotManager slotManager,
                         Supplier<Map<String, GossipState>> peerStatesSupplier,
                         EvictionConfig config) {
    this(strategy, slotManager, peerStatesSupplier, Collections::emptyMap, null, config);
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
    logger.info("EvictionManager started: intervalMs=" + intervalMs
        + " initialDelayMs=" + initialDelayMs
        + " evictionPercentageThreshold=" + config.getEvictionPercentageThreshold() + "%"
        + " pendingEvictionCooldownSeconds=" + config.getPendingEvictionCooldownSeconds()
        + " topNTargets=" + config.getTopNTargets()
        + " strategy=" + strategy.getClass().getSimpleName());
  }

  public void stop() {
    if (executor != null) {
      executor.shutdownNow();
    }
    logger.info("EvictionManager stopped");
  }

  void runEviction() {
    long startMs = System.currentTimeMillis();
    try {
      Map<String, GossipState> peerStates = peerStatesSupplier.get();
      Map<String, Map<String, Integer>> producerConnections = slotManager.getProducerConnections();
      Map<String, Set<String>> topicToBrokerIps = topicToBrokerIpsSupplier.get();
      // Per-topic balancing gate: only producers writing to a topic that has
      // opted in (TopicConfig.enableBalancing) are eligible. Read live each
      // tick so toggling enableBalancing takes effect without a restart. A
      // null supplier disables filtering (tests); the broker-wide
      // EvictionConfig.enabled gate is applied upstream at manager creation.
      Map<String, Map<String, Integer>> eligibleConnections =
          filterBalancingEnabled(producerConnections);
      logger.info("Eviction tick: peers=" + peerStates.size()
          + " v4Producers=" + producerConnections.size()
          + " balancingEligible=" + eligibleConnections.size()
          + " topics=" + topicToBrokerIps.size()
          + " localFreeSlots=" + slotManager.getFreeSlots()
          + "/" + slotManager.getTotalSlots()
          + " pendingEvictions=" + pendingEvictions.size());
      List<EvictionResult> results = strategy.evaluateBatch(slotManager, peerStates,
          eligibleConnections, topicToBrokerIps);
      if (!results.isEmpty()) {
        for (EvictionResult result : results) {
          EvictionResult prev = pendingEvictions.put(result.getPid(), result);
          String producerIp = slotManager.getProducerIp(result.getPid());
          String ipSuffix = producerIp == null ? "" : " producerIp=" + producerIp;
          if (prev != null) {
            logger.info("Eviction scheduled (overwrote prior pending): " + result + ipSuffix
                + " replaced=" + prev);
          } else {
            logger.info("Eviction scheduled: " + result + ipSuffix);
          }
        }
        logger.info("Eviction tick scheduled " + results.size() + " eviction(s) (took "
            + (System.currentTimeMillis() - startMs) + "ms)");
      } else {
        logger.info("Eviction tick produced no decision (took "
            + (System.currentTimeMillis() - startMs) + "ms) -- see prior 'eviction skipped'"
            + " message for the reason");
      }
    } catch (Exception e) {
      logger.log(Level.WARNING, "Error in eviction run", e);
    }
  }

  /**
   * Restrict the producer-connection view to producers eligible for balancing:
   * those writing to at least one topic with {@code enableBalancing == true}.
   * The enabled-topic set is read live from the supplier each call, so
   * toggling a topic's {@code enableBalancing} activates/deactivates its
   * producers on the next tick without a broker restart. A {@code null}
   * supplier means "no per-topic filter" and returns the input unchanged.
   */
  private Map<String, Map<String, Integer>> filterBalancingEnabled(
      Map<String, Map<String, Integer>> producerConnections) {
    if (balancingEnabledTopicsSupplier == null) {
      return producerConnections;
    }
    Set<String> enabledTopics = balancingEnabledTopicsSupplier.get();
    if (enabledTopics == null || enabledTopics.isEmpty()) {
      return Collections.emptyMap();
    }
    Map<String, Map<String, Integer>> eligible = new HashMap<>();
    for (Map.Entry<String, Map<String, Integer>> entry : producerConnections.entrySet()) {
      for (String topic : slotManager.getProducerTopics(entry.getKey())) {
        if (enabledTopics.contains(topic)) {
          eligible.put(entry.getKey(), entry.getValue());
          break;
        }
      }
    }
    return eligible;
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

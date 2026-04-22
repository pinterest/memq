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

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.logging.Logger;
import java.util.stream.Collectors;

/**
 * Eviction strategy that prefers producers already connected to the target broker.
 * <p>
 * Only evicts if this broker's free slots are below the mean of non-frozen peers.
 * Uses probabilistic target selection from top-N brokers to prevent herding.
 * Maintains a cooldown per target to avoid over-evicting to the same broker.
 * <p>
 * <b>v3 backward compatibility:</b> Only v4 producers are eviction candidates.
 * A producer is considered v4 if it appears in {@code producerConnections},
 * which is populated exclusively from v4 write requests (via
 * {@link com.pinterest.memq.core.slot.SlotManager#recordProducerConnections}).
 * v3 producers still participate in slot accounting but are never targeted
 * for eviction because they cannot interpret eviction responses.
 */
public class CurrConnectionsEvictionStrategy implements EvictionStrategy {

  private static final Logger logger = Logger.getLogger(CurrConnectionsEvictionStrategy.class.getName());

  private final String brokerId;
  private final EvictionConfig config;
  private final ConcurrentHashMap<String, Long> pendingEvictionTargets = new ConcurrentHashMap<>();

  public CurrConnectionsEvictionStrategy(String brokerId, EvictionConfig config) {
    this.brokerId = brokerId;
    this.config = config;
  }

  @Override
  public EvictionResult evaluate(SlotManager slotManager,
                                 Map<String, GossipState> peerStates,
                                 Map<String, Set<String>> producerConnections) {
    if (peerStates.isEmpty()) {
      logger.info("[" + brokerId + "] eviction skipped: no peers known via gossip yet");
      return null;
    }

    long now = System.currentTimeMillis();
    long cooldownMs = (long) (config.getPendingEvictionCooldownSeconds() * 1000);
    pendingEvictionTargets.entrySet().removeIf(e -> now - e.getValue() > cooldownMs);

    List<Map.Entry<String, Integer>> candidates = peerStates.entrySet().stream()
        .filter(e -> !e.getValue().getMessage().isFreeze())
        .filter(e -> !pendingEvictionTargets.containsKey(e.getKey()))
        .map(e -> new AbstractMap.SimpleEntry<>(e.getKey(), e.getValue().getMessage().getFreeSlots()))
        .collect(Collectors.toList());

    if (candidates.isEmpty()) {
      logger.info("[" + brokerId + "] eviction skipped: no eligible target peers"
          + " (peerCount=" + peerStates.size() + ", pendingTargets=" + pendingEvictionTargets.size()
          + ") -- all peers are frozen or in cooldown");
      return null;
    }

    double meanFreeSlots = candidates.stream().mapToInt(Map.Entry::getValue).average().orElse(0);
    int localFreeSlots = slotManager.getFreeSlots();

    if (localFreeSlots >= meanFreeSlots) {
      logger.info("[" + brokerId + "] eviction skipped: local broker is not above mean load"
          + " (localFreeSlots=" + localFreeSlots
          + " >= meanFreeSlots=" + String.format("%.1f", meanFreeSlots)
          + ", candidatePeers=" + candidates.size() + ")");
      return null;
    }

    candidates.sort((a, b) -> Integer.compare(b.getValue(), a.getValue()));
    Map.Entry<String, Integer> target = selectTargetBroker(candidates);

    if (target == null || target.getKey().equals(brokerId)) {
      logger.info("[" + brokerId + "] eviction skipped: selected target was self or null"
          + " (target=" + (target == null ? "null" : target.getKey()) + ")");
      return null;
    }

    if (target.getValue() <= localFreeSlots) {
      logger.info("[" + brokerId + "] eviction skipped: target has no more free slots than us"
          + " (target=" + target.getKey() + " freeSlots=" + target.getValue()
          + " <= local=" + localFreeSlots + ")");
      return null;
    }

    int slotDifference = Math.abs(localFreeSlots - target.getValue());
    double percentageDifference = (double) slotDifference / slotManager.getTotalSlots() * 100;
    if (percentageDifference <= config.getEvictionPercentageThreshold()) {
      logger.info("[" + brokerId + "] eviction skipped: load gap to target is below threshold"
          + " (target=" + target.getKey()
          + " gap=" + String.format("%.2f", percentageDifference) + "%"
          + " threshold=" + config.getEvictionPercentageThreshold() + "%"
          + " localFree=" + localFreeSlots + " targetFree=" + target.getValue()
          + " totalSlots=" + slotManager.getTotalSlots() + ")");
      return null;
    }

    // Only v4 producers are eviction candidates. producerConnections is the
    // v4 registry: only producers that have sent a v4 write request appear here.
    if (producerConnections.isEmpty()) {
      logger.info("[" + brokerId + "] eviction skipped: no v4 producers registered yet"
          + " (target=" + target.getKey() + ") -- check that producers are using producer2"
          + " package and sending v4 write requests");
      return null;
    }

    // Prefer v4 producers already connected to the target broker
    List<String> candidatePids = new ArrayList<>();
    for (Map.Entry<String, Set<String>> entry : producerConnections.entrySet()) {
      String pid = entry.getKey();
      if (entry.getValue().contains(target.getKey()) && slotManager.producerHasSlots(pid)) {
        candidatePids.add(pid);
      }
    }

    String pidToEvict;
    if (!candidatePids.isEmpty()) {
      pidToEvict = candidatePids.get(ThreadLocalRandom.current().nextInt(candidatePids.size()));
      logger.info("[" + brokerId + "] evicting pid=" + pidToEvict
          + " (has connection to target=" + target.getKey() + ")");
    } else {
      // Fall back to any v4 producer that has slots
      List<String> v4Pids = new ArrayList<>();
      for (String pid : producerConnections.keySet()) {
        if (slotManager.producerHasSlots(pid)) {
          v4Pids.add(pid);
        }
      }
      if (v4Pids.isEmpty()) {
        logger.info("[" + brokerId + "] eviction skipped: no v4 producer holds slots"
            + " (registeredV4Producers=" + producerConnections.size()
            + ", target=" + target.getKey() + ")");
        return null;
      }
      pidToEvict = v4Pids.get(ThreadLocalRandom.current().nextInt(v4Pids.size()));
      logger.info("[" + brokerId + "] evicting pid=" + pidToEvict
          + " (random v4, no candidate connected to target=" + target.getKey() + ")");
    }

    pendingEvictionTargets.put(target.getKey(), now);

    EvictionResult result = new EvictionResult(pidToEvict, target.getKey(), 1);
    logger.info("[" + brokerId + "] eviction decision: pid=" + pidToEvict
        + " target=" + target.getKey() + " slotsToEvict=1"
        + " (localFree=" + localFreeSlots + " targetFree=" + target.getValue()
        + " gap=" + String.format("%.2f", percentageDifference) + "%"
        + " v4Producers=" + producerConnections.size() + ")");
    return result;
  }

  private Map.Entry<String, Integer> selectTargetBroker(
      List<Map.Entry<String, Integer>> sortedCandidates) {
    int topN = Math.min(config.getTopNTargets(), sortedCandidates.size());
    if (topN == 0) {
      return null;
    }
    if (topN == 1) {
      return sortedCandidates.get(0);
    }

    List<Map.Entry<String, Integer>> topCandidates = sortedCandidates.subList(0, topN);
    int totalFreeSlots = topCandidates.stream()
        .mapToInt(e -> Math.max(e.getValue(), 1))
        .sum();

    double rand = ThreadLocalRandom.current().nextDouble() * totalFreeSlots;
    double cumulative = 0;
    for (Map.Entry<String, Integer> candidate : topCandidates) {
      cumulative += Math.max(candidate.getValue(), 1);
      if (rand < cumulative) {
        return candidate;
      }
    }
    return topCandidates.get(topCandidates.size() - 1);
  }
}

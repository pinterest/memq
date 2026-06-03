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
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.logging.Logger;
import java.util.stream.Collectors;

/**
 * Default eviction strategy. Picks the target broker first via a sequence
 * of guards, then chooses a producer to evict to it.
 * <p>
 * <b>Target broker filter (must pass all):</b>
 * <ol>
 *   <li>Not frozen.</li>
 *   <li>Not already a pending eviction target (per-target cooldown).</li>
 *   <li>Serves at least one topic that this broker also serves -- a producer
 *       sent to a non-serving target would just receive REDIRECT and trigger
 *       a client-side metadata refresh + reconnect.</li>
 * </ol>
 * <b>Congestion check:</b> the local broker's free slots must be strictly
 * less than the mean free slots across the surviving target candidates.
 * <p>
 * <b>Target selection:</b> sort surviving targets by free slots (desc),
 * pick probabilistically from the top-N weighted by free slots. Then
 * verify the chosen target is strictly less loaded than us by more than
 * {@code evictionPercentageThreshold}.
 * <p>
 * <b>Producer selection:</b> among v4 producers that hold slots and write
 * to a topic the target broker serves, pick the one holding the most slots
 * (its share of the broker's throughput), since moving the heaviest producer
 * is what actually drains a saturated broker. A producer already connected to
 * the target is preferred (no new connection needed) unless an unconnected
 * producer is heavier by more than {@code heavyProducerSlotMargin} slots.
 * <p>
 * <b>v3 backward compatibility:</b> Only v4 producers are eviction
 * candidates -- {@code producerConnections} is populated exclusively from
 * v4 write requests, so v3 producers are naturally excluded.
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
                                 Map<String, Set<String>> producerConnections,
                                 Map<String, Set<String>> topicToBrokerIps) {
    if (peerStates.isEmpty()) {
      logger.info("[" + brokerId + "] eviction skipped: no peers known via gossip yet");
      return null;
    }

    long now = System.currentTimeMillis();
    long cooldownMs = (long) (config.getPendingEvictionCooldownSeconds() * 1000);
    pendingEvictionTargets.entrySet().removeIf(e -> now - e.getValue() > cooldownMs);

    // Build broker -> topics it serves, and the set of topics this broker
    // serves locally. Both views derive from the same governor snapshot so
    // they are consistent within this tick.
    Map<String, Set<String>> brokerToTopics = invert(topicToBrokerIps);
    Set<String> localTopics = brokerToTopics.getOrDefault(brokerId, java.util.Collections.emptySet());

    // Step 1: filter candidate target brokers. Each filter is a reason to
    // not evict; all must pass.
    List<Map.Entry<String, Integer>> candidates = peerStates.entrySet().stream()
        .filter(e -> !brokerId.equals(e.getKey()))
        .filter(e -> !e.getValue().getMessage().isFreeze())
        .filter(e -> !pendingEvictionTargets.containsKey(e.getKey()))
        .filter(e -> sharesTopic(e.getKey(), brokerToTopics, localTopics))
        .map(e -> new AbstractMap.SimpleEntry<>(e.getKey(),
            e.getValue().getMessage().getFreeSlots()))
        .collect(Collectors.toList());

    if (candidates.isEmpty()) {
      logger.info("[" + brokerId + "] eviction skipped: no eligible target peers"
          + " (peerCount=" + peerStates.size()
          + ", pendingTargets=" + pendingEvictionTargets.size()
          + ", localTopics=" + localTopics.size() + ")"
          + " -- all peers are frozen, in cooldown, or share no topics with this broker");
      return null;
    }

    // Step 2: am I more congested than the surviving candidate set?
    double meanFreeSlots = candidates.stream().mapToInt(Map.Entry::getValue).average().orElse(0);
    int localFreeSlots = slotManager.getFreeSlots();

    if (localFreeSlots >= meanFreeSlots) {
      logger.info("[" + brokerId + "] eviction skipped: local broker is not above mean load"
          + " (localFreeSlots=" + localFreeSlots
          + " >= meanFreeSlots=" + String.format("%.1f", meanFreeSlots)
          + ", candidatePeers=" + candidates.size() + ")");
      return null;
    }

    // Step 3: probabilistic top-N target selection. Prefer "swap-free" targets
    // -- ones the producer we would evict is already connected to -- so the
    // client does not have to drop a connection to honor maxConnections. This
    // is a preference, not a blocker: broker balance is still the hard gate
    // (only the already-valid candidates are considered), and if no swap-free
    // target is viable we fall back to the best balancing target (accepting the
    // client-side swap, which is the genuinely necessary case).
    candidates.sort((a, b) -> Integer.compare(b.getValue(), a.getValue()));

    Map.Entry<String, Integer> target = null;
    if (config.isPreferConnectedTarget() && !producerConnections.isEmpty()) {
      List<Map.Entry<String, Integer>> swapFree = new ArrayList<>();
      for (Map.Entry<String, Integer> candidate : candidates) {
        Set<String> served = brokerToTopics.getOrDefault(candidate.getKey(),
            java.util.Collections.emptySet());
        String pid = pickProducer(slotManager, producerConnections, candidate.getKey(),
            served, true);
        if (pid != null
            && producerConnections.getOrDefault(pid, java.util.Collections.emptySet())
                .contains(candidate.getKey())) {
          swapFree.add(candidate);
        }
      }
      if (!swapFree.isEmpty()) {
        Map.Entry<String, Integer> swapFreeTarget = selectTargetBroker(swapFree);
        if (swapFreeTarget != null
            && isViableTarget(swapFreeTarget.getValue(), localFreeSlots,
                slotManager.getTotalSlots())) {
          target = swapFreeTarget;
        }
      }
    }

    if (target == null) {
      target = selectTargetBroker(candidates);
    }

    if (target == null) {
      logger.info("[" + brokerId + "] eviction skipped: target selection returned null");
      return null;
    }

    // Step 4: verify the target is meaningfully less loaded.
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

    // Only v4 producers are eviction candidates.
    if (producerConnections.isEmpty()) {
      logger.info("[" + brokerId + "] eviction skipped: no v4 producers registered yet"
          + " (target=" + target.getKey() + ") -- check that producers are using producer2"
          + " package and sending v4 write requests");
      return null;
    }

    // Step 5: pick a producer. Constrain to producers writing to a topic the
    // target broker serves -- otherwise the producer would be sent to a
    // broker that doesn't own its topic processor and would just REDIRECT.
    Set<String> targetServedTopics = brokerToTopics.getOrDefault(target.getKey(),
        java.util.Collections.emptySet());

    String pidToEvict = pickProducer(slotManager, producerConnections, target.getKey(),
        targetServedTopics, false);

    if (pidToEvict == null) {
      logger.info("[" + brokerId + "] eviction skipped: no v4 producer holds slots on a topic"
          + " served by target=" + target.getKey()
          + " (registeredV4Producers=" + producerConnections.size()
          + ", targetServedTopics=" + targetServedTopics + ")");
      return null;
    }

    pendingEvictionTargets.put(target.getKey(), now);

    EvictionResult result = new EvictionResult(pidToEvict, target.getKey(), 1);
    logger.info("[" + brokerId + "] eviction decision: pid=" + pidToEvict
        + " target=" + target.getKey() + " slotsToEvict=1"
        + " (localFree=" + localFreeSlots + " targetFree=" + target.getValue()
        + " gap=" + String.format("%.2f", percentageDifference) + "%"
        + " v4Producers=" + producerConnections.size()
        + " targetTopics=" + targetServedTopics.size() + ")");
    return result;
  }

  /**
   * Pick a producer to evict to {@code targetIp}. Constrained to v4 producers
   * that hold slots on at least one topic the target broker serves. Among
   * those, prefer producers that already have an active connection to the
   * target (skips a new-connection setup on the producer side).
   */
  private String pickProducer(SlotManager slotManager,
                              Map<String, Set<String>> producerConnections,
                              String targetIp,
                              Set<String> targetServedTopics,
                              boolean quiet) {
    if (targetServedTopics.isEmpty()) {
      return null;
    }
    // Track the heaviest eligible producer overall, and the heaviest one
    // already connected to the target. Slots held are a proxy for the
    // producer's share of the broker's (shaper-capped) throughput, so moving
    // the heaviest producer is what actually drains a saturated broker.
    String heaviestPid = null;
    int heaviestSlots = 0;
    String heaviestConnectedPid = null;
    int heaviestConnectedSlots = 0;
    for (Map.Entry<String, Set<String>> entry : producerConnections.entrySet()) {
      String pid = entry.getKey();
      // Require real slot ownership, not just map presence. recordWrite
      // re-creates a zero-slot ProducerSlotState on every write, so a producer
      // that was evicted to 0 but keeps writing under backpressure still
      // satisfies producerHasSlots() (which is containsKey). Selecting it would
      // burn this tick's single eviction on a no-op release that frees nothing,
      // while the producer actually holding the slots is never evicted.
      int slots = slotManager.getTotalProducerSlots(pid);
      if (slots <= 0) {
        continue;
      }
      if (!writesToServedTopic(slotManager.getProducerTopics(pid), targetServedTopics)) {
        continue;
      }
      if (slots > heaviestSlots) {
        heaviestSlots = slots;
        heaviestPid = pid;
      }
      Set<String> conns = entry.getValue();
      if (conns != null && conns.contains(targetIp) && slots > heaviestConnectedSlots) {
        heaviestConnectedSlots = slots;
        heaviestConnectedPid = pid;
      }
    }
    if (heaviestPid == null) {
      return null;
    }
    // Prefer a producer already connected to the target (avoids a new
    // producer-side connection) unless an unconnected producer is heavier by
    // more than the configured margin -- in which case moving that heavier
    // producer is worth the new connection because a lighter eviction's freed
    // capacity would just be reabsorbed by the heavier, backpressured one.
    if (heaviestConnectedPid != null
        && heaviestConnectedSlots >= heaviestSlots - config.getHeavyProducerSlotMargin()) {
      if (!quiet) {
        logger.info("[" + brokerId + "] evicting pid=" + heaviestConnectedPid
            + " (connected to target=" + targetIp + ", slots=" + heaviestConnectedSlots
            + ", heaviestEligibleSlots=" + heaviestSlots + ")");
      }
      return heaviestConnectedPid;
    }
    if (!quiet) {
      logger.info("[" + brokerId + "] evicting pid=" + heaviestPid
          + " (heaviest eligible producer, slots=" + heaviestSlots
          + ", not connected to target=" + targetIp + ")");
    }
    return heaviestPid;
  }

  /**
   * A target is viable for eviction if it has strictly more free slots than us
   * and the gap exceeds the configured percentage threshold. Mirrors the Step 4
   * checks; used to validate a swap-free target before preferring it (quietly,
   * so the verbose skip diagnostics only fire on the final fallback target).
   */
  private boolean isViableTarget(int targetFreeSlots, int localFreeSlots, int totalSlots) {
    if (targetFreeSlots <= localFreeSlots) {
      return false;
    }
    double percentageDifference =
        (double) Math.abs(localFreeSlots - targetFreeSlots) / totalSlots * 100;
    return percentageDifference > config.getEvictionPercentageThreshold();
  }

  private static boolean writesToServedTopic(Collection<String> producerTopics,
                                             Set<String> targetServedTopics) {
    for (String t : producerTopics) {
      if (targetServedTopics.contains(t)) {
        return true;
      }
    }
    return false;
  }

  private static boolean sharesTopic(String peerIp,
                                     Map<String, Set<String>> brokerToTopics,
                                     Set<String> localTopics) {
    if (localTopics.isEmpty()) {
      return false;
    }
    Set<String> peerTopics = brokerToTopics.get(peerIp);
    if (peerTopics == null || peerTopics.isEmpty()) {
      return false;
    }
    for (String t : peerTopics) {
      if (localTopics.contains(t)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Invert {@code topic -> {brokerIp}} into {@code brokerIp -> {topic}} for
   * O(1) lookup of "what topics does this broker serve?".
   */
  private static Map<String, Set<String>> invert(Map<String, Set<String>> topicToBrokerIps) {
    Map<String, Set<String>> out = new HashMap<>();
    for (Map.Entry<String, Set<String>> e : topicToBrokerIps.entrySet()) {
      String topic = e.getKey();
      Set<String> ips = e.getValue();
      if (ips == null) continue;
      for (String ip : ips) {
        out.computeIfAbsent(ip, k -> new HashSet<>()).add(topic);
      }
    }
    return out;
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

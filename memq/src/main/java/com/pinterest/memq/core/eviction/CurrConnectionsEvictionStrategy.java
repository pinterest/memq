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
import java.util.Collections;
import java.util.Comparator;
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
 * Default eviction strategy. Picks the target broker first via load-balance
 * guards, then chooses a producer to send there.
 * <p>
 * The strategy operates in two modes driven by the {@link SlotManager}'s drain
 * latch:
 * <ul>
 *   <li><b>Routine mode</b> ({@code !drainLatched}): the broker is healthy and
 *       evictions are about gentle rebalancing. Producers are sorted ascending
 *       by source-slot count and the lightest are preferred -- a producer with
 *       {@code 1} slot on the source naturally drops its source connection on
 *       eviction (a "graceful swap"). The {@code maxConnectionsPerProducer}
 *       cap is enforced: an eviction that would force a client-side connection
 *       drop (producer at cap, target not in its set, source slots &gt; 1) is
 *       <i>refused</i> rather than dispatched. This avoids the "harmonic
 *       dance" oscillation where forced drops repeatedly bounce the lightest
 *       non-target connection between two brokers.</li>
 *   <li><b>Drain mode</b> ({@code drainLatched}): the broker is saturated and
 *       must shed load even at the cost of a connection drop. Producers are
 *       sorted descending by source-slot count and the heaviest are preferred
 *       -- moving a heavy, backpressured producer is what actually relieves
 *       the saturation, since freeing a slot the heavy producer would just
 *       reabsorb otherwise accomplishes nothing. The cap-violation check is
 *       relaxed.</li>
 * </ul>
 * In both modes the top-{@code topNProducers} sorted candidates are picked
 * uniformly at random, with "already connected to target" as the secondary
 * tiebreaker. The randomization breaks deterministic cycles where the same
 * (producer, source, target) triple is chosen tick after tick.
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
 * less than the mean free slots across <i>all</i> topic-sharing peers
 * (including frozen and pending-cooldown ones, since they still represent
 * load in the cluster shape).
 * <p>
 * <b>Target selection:</b> sort surviving target candidates by free slots
 * (desc), pick from the top-{@code topNTargets} using a rank-based linear
 * decay (weight {@code = topN - rank}, so for {@code topN=3} the freest peer
 * wins with probability 50%, the second with 33%, the third with 17%). The
 * decay concentrates probability on the actually-freest peer without locking
 * the broker into a single deterministic target. Then verify the chosen
 * target is strictly less loaded than us by more than
 * {@code evictionPercentageThreshold}.
 * <p>
 * <b>Steady-state consolidation:</b> when the load-balance check concludes
 * "no eviction needed" <i>and</i> the cluster's free-slot spread (max minus
 * min across topic-sharing brokers including local) is within the
 * {@code consolidationPercentageThreshold} deadband (typically looser than
 * the eviction threshold), the strategy looks for producers whose connection
 * count exceeds {@code maxConnectionsPerProducer} and picks one with the
 * lowest source-slot count to evict to an already-connected target. This
 * shrinks over-cap producers back to the cap via gradual graceful drains,
 * without disrupting balanced operation. Decoupling consolidation from the
 * eviction deadband lets consolidation fire in sparse-topology regimes where
 * normal eviction perpetually wrestles with cap-skip and the cluster never
 * tightens to the eviction deadband on its own.
 * <p>
 * Consolidation does <i>not</i> fire when:
 * <ul>
 *   <li><b>The cluster is diverged in our favor</b> -- local is on the freer
 *       side but a peer is hot. Adding load to a connected peer would
 *       compete with the work the cluster needs to do to drain the hot
 *       broker. Wait for true convergence.</li>
 *   <li><b>Normal eviction is blocked</b> (e.g. all peers frozen, cap-skip
 *       exhausted) -- the cluster is acting, not at rest, so an opportunistic
 *       move would stack noise on top of in-flight work.</li>
 * </ul>
 * <p>
 * <b>v3 backward compatibility:</b> Only v4+ producers are eviction
 * candidates -- {@code producerConnections} is populated exclusively from
 * v4 and v5 write requests, so v3 producers are naturally excluded.
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
                                 Map<String, Map<String, Integer>> producerConnections,
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
    Set<String> localTopics = brokerToTopics.getOrDefault(brokerId, Collections.emptySet());

    // Step 1: am I more loaded than the cluster? Compare local free slots to
    // the mean over the full load population -- every topic-sharing peer,
    // including frozen and pending-cooldown ones. Excluding them biases the
    // mean toward higher free slots (frozen peers are typically saturated,
    // i.e. low-free) and systematically over-evicts.
    int localFreeSlots = slotManager.getFreeSlots();
    List<Integer> loadPopulation = peerStates.entrySet().stream()
        .filter(e -> !brokerId.equals(e.getKey()))
        .filter(e -> sharesTopic(e.getKey(), brokerToTopics, localTopics))
        .map(e -> e.getValue().getMessage().getFreeSlots())
        .collect(Collectors.toList());

    if (loadPopulation.isEmpty()) {
      logger.info("[" + brokerId + "] eviction skipped: no topic-sharing peers to compare against"
          + " (peerCount=" + peerStates.size()
          + ", localTopics=" + localTopics.size() + ")");
      return null;
    }

    double meanFreeSlots = loadPopulation.stream().mapToInt(Integer::intValue).average().orElse(0);
    // Cluster-wide spread across topic-sharing brokers (including local).
    // Used to gate consolidation: consolidation should only run when the
    // cluster is genuinely near-balanced, not just when *this* broker
    // happens to be on the freer side of a diverged cluster.
    int peerMaxFree = loadPopulation.stream().mapToInt(Integer::intValue).max().orElse(localFreeSlots);
    int peerMinFree = loadPopulation.stream().mapToInt(Integer::intValue).min().orElse(localFreeSlots);
    int clusterMaxFree = Math.max(peerMaxFree, localFreeSlots);
    int clusterMinFree = Math.min(peerMinFree, localFreeSlots);
    double spreadPct = slotManager.getTotalSlots() > 0
        ? (double) (clusterMaxFree - clusterMinFree) / slotManager.getTotalSlots() * 100
        : 0.0;
    // Consolidation deadband must never be tighter than the eviction
    // deadband -- otherwise we'd consolidate inside a band where normal
    // eviction is still actively trying to shed load.
    double consolidationThreshold = Math.max(
        config.getConsolidationPercentageThreshold(),
        config.getEvictionPercentageThreshold());
    boolean clusterConverged = spreadPct <= consolidationThreshold;

    if (localFreeSlots >= meanFreeSlots) {
      logger.info("[" + brokerId + "] eviction skipped: local broker is not above mean load"
          + " (localFreeSlots=" + localFreeSlots
          + " >= meanFreeSlots=" + String.format("%.1f", meanFreeSlots)
          + ", loadPeers=" + loadPopulation.size() + ")");
      // CONVERGED branch: local broker is at or below the cluster mean. Try a
      // steady-state consolidation pass to shrink over-cap producers without
      // disturbing the balanced routing -- but only if the cluster is actually
      // converged (max-min spread within deadband). When the cluster is
      // diverged in our favor (we're free, some peer is hot), consolidation
      // would just add load to our connected peers and slow real convergence.
      return maybeConsolidate(slotManager, peerStates, producerConnections,
          brokerToTopics, localTopics, clusterConverged,
          clusterMaxFree, clusterMinFree, spreadPct, consolidationThreshold);
    }

    // Step 2: filter candidate target brokers. Each filter is a reason a
    // topic-sharing peer cannot receive an eviction right now. Frozen peers
    // and peers already in pending-target cooldown are excluded here even
    // though they count toward the mean above.
    List<Map.Entry<String, Integer>> candidates = peerStates.entrySet().stream()
        .filter(e -> !brokerId.equals(e.getKey()))
        .filter(e -> sharesTopic(e.getKey(), brokerToTopics, localTopics))
        .filter(e -> !e.getValue().getMessage().isFreeze())
        .filter(e -> !pendingEvictionTargets.containsKey(e.getKey()))
        .map(e -> new AbstractMap.SimpleEntry<>(e.getKey(),
            e.getValue().getMessage().getFreeSlots()))
        .collect(Collectors.toList());

    if (candidates.isEmpty()) {
      logger.info("[" + brokerId + "] eviction skipped: no eligible target peers"
          + " (peerCount=" + peerStates.size()
          + ", pendingTargets=" + pendingEvictionTargets.size()
          + ", localTopics=" + localTopics.size() + ")"
          + " -- all topic-sharing peers are frozen or in cooldown");
      // BLOCKED branch: eviction is needed but cannot fire because every
      // peer is frozen or in cooldown. The cluster is acting; do not stack
      // a consolidation on top.
      return null;
    }

    // Step 3: rank-based linear-decay top-N target selection. The freest
    // peer (rank 0) wins with weight = topN; each subsequent rank decays
    // linearly. For topN=3 this is 50% / 33% / 17%. The decisive bias
    // toward the freest peer focuses shedding where it helps most while
    // keeping enough randomization to break deterministic cycles.
    candidates.sort((a, b) -> Integer.compare(b.getValue(), a.getValue()));
    Map.Entry<String, Integer> target = selectTargetBroker(candidates);
    if (target == null) {
      logger.info("[" + brokerId + "] eviction skipped: target selection returned null");
      return null;
    }

    // Step 4: verify the target is meaningfully less loaded.
    if (target.getValue() <= localFreeSlots) {
      logger.info("[" + brokerId + "] eviction skipped: target has no more free slots than us"
          + " (target=" + target.getKey() + " freeSlots=" + target.getValue()
          + " <= local=" + localFreeSlots + ")");
      // CONVERGED branch (relative to chosen target). Consolidation also
      // requires the *whole* cluster to be converged.
      return maybeConsolidate(slotManager, peerStates, producerConnections,
          brokerToTopics, localTopics, clusterConverged,
          clusterMaxFree, clusterMinFree, spreadPct, consolidationThreshold);
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
      // CONVERGED branch (gap to freest target is below deadband).
      // Consolidation also requires the *whole* cluster to be converged --
      // a low gap to the freest peer can coexist with a hot tail peer.
      return maybeConsolidate(slotManager, peerStates, producerConnections,
          brokerToTopics, localTopics, clusterConverged,
          clusterMaxFree, clusterMinFree, spreadPct, consolidationThreshold);
    }

    // Only v4+ producers are eviction candidates.
    if (producerConnections.isEmpty()) {
      logger.info("[" + brokerId + "] eviction skipped: no v4+ producers registered yet"
          + " (target=" + target.getKey() + ") -- check that producers are using producer2"
          + " package and sending v4+ write requests");
      return null;
    }

    // Step 5: pick a producer. Constrain to producers writing to a topic the
    // target broker serves -- otherwise the producer would be sent to a
    // broker that doesn't own its topic processor and would just REDIRECT.
    Set<String> targetServedTopics = brokerToTopics.getOrDefault(target.getKey(),
        Collections.emptySet());
    boolean drainMode = slotManager.isDrainLatched();
    String pidToEvict = pickProducer(slotManager, producerConnections, target.getKey(),
        targetServedTopics, drainMode);

    if (pidToEvict == null) {
      // Two reasons this happens: (1) no producer writes to a topic served by
      // the target, or (2) every producer-target pair would force a client
      // connection drop and we are in routine mode. The next tick re-rolls
      // the random target so a viable combination has another shot.
      logger.info("[" + brokerId + "] eviction skipped: no eligible producer for target="
          + target.getKey()
          + " (drainMode=" + drainMode
          + ", maxConnPerProducer=" + config.getMaxConnectionsPerProducer()
          + ", registeredV4Producers=" + producerConnections.size()
          + ", targetServedTopics=" + targetServedTopics + ")");
      // BLOCKED branch: routine cap-skip exhaustion, or topic-affinity
      // mismatch. Do not consolidate -- the cluster wants to act but can't.
      return null;
    }

    pendingEvictionTargets.put(target.getKey(), now);

    EvictionResult result = new EvictionResult(pidToEvict, target.getKey(), 1);
    logger.info("[" + brokerId + "] eviction decision: pid=" + pidToEvict
        + " target=" + target.getKey() + " slotsToEvict=1"
        + " mode=" + (drainMode ? "drain" : "routine")
        + " (localFree=" + localFreeSlots + " targetFree=" + target.getValue()
        + " gap=" + String.format("%.2f", percentageDifference) + "%"
        + " v4Producers=" + producerConnections.size()
        + " targetTopics=" + targetServedTopics.size() + ")");
    return result;
  }

  /**
   * Pick a producer to evict to {@code targetIp}. Selection is by <i>load</i>
   * (EMA), while connection-cap enforcement stays keyed on routing-slot
   * ownership -- a deliberate hybrid (see below).
   * <ul>
   *   <li>Eligibility: v4 producer carrying measurable load ({@code EMA > 0})
   *       that writes to a topic the target broker serves. Load is measured by
   *       EMA, not routing-slot ownership, so a producer saturating this broker
   *       is evictable even when the drain latch has frozen it out of owning any
   *       slots -- moving its traffic is what actually sheds the broker's load.</li>
   *   <li>Routine mode ({@code !drainMode}): drop any producer whose eviction
   *       would force a client connection drop. The producer must already be
   *       below the {@code maxConnectionsPerProducer} cap, or the target must
   *       already be in its connection set, or the eviction must naturally drop
   *       the source connection ({@code sourceSlots == 1}). This cap check is
   *       about connection topology, so it stays keyed on routing-slot
   *       ownership.</li>
   *   <li>Drain mode: keep cap-violating candidates -- the broker is
   *       saturated and forcing a connection drop is the price of relief.</li>
   *   <li>Sort: ascending EMA in routine (prefer graceful, low-load swaps),
   *       descending in drain (prefer impactful, high-load shifts).
   *       Connected-to-target is the tiebreaker at equal load so swap-free
   *       moves win.</li>
   *   <li>Pick uniformly at random from the top-{@code topNProducers}, so the
   *       same producer is not deterministically targeted every tick.</li>
   * </ul>
   */
  private String pickProducer(SlotManager slotManager,
                              Map<String, Map<String, Integer>> producerConnections,
                              String targetIp,
                              Set<String> targetServedTopics,
                              boolean drainMode) {
    if (targetServedTopics.isEmpty()) {
      return null;
    }
    int maxConns = config.getMaxConnectionsPerProducer();

    List<ProducerCandidate> eligible = new ArrayList<>();
    for (Map.Entry<String, Map<String, Integer>> entry : producerConnections.entrySet()) {
      String pid = entry.getKey();
      Map<String, Integer> conns = entry.getValue() != null
          ? entry.getValue()
          : Collections.emptyMap();
      // Eligibility is by load (EMA), not routing-slot ownership. A producer
      // saturating this broker can own 0 slots: the drain latch freezes
      // acquisition while eviction releases ownership, yet its EMA stays high
      // because it keeps writing under backpressure. Keying on ownership would
      // make exactly those producers invisible, so a drain-latched broker hits
      // "no eligible producer" while still hot. The broker-side slot release on
      // eviction may then be a no-op, but the client reweight it triggers is
      // what actually moves the load.
      double emaMbps = slotManager.getTotalProducerEmaRate(pid);
      if (emaMbps <= 0.0) {
        continue;
      }
      if (!writesToServedTopic(slotManager.getProducerTopics(pid), targetServedTopics)) {
        continue;
      }
      boolean connectedToTarget = conns.containsKey(targetIp);
      // Routine cap-violation skip: the producer is at the cap, the target is
      // a fresh broker for it, and the eviction will not naturally drop the
      // source connection. Picking it here would force the client to drop one
      // of its existing connections, which is exactly the disruption the
      // routine path is trying to avoid. Drain mode allows this disruption.
      // Cap enforcement is about connection topology, so it stays keyed on
      // routing-slot ownership -- read only here, on the routine path.
      if (!drainMode
          && maxConns > 0
          && conns.size() >= maxConns
          && !connectedToTarget
          && slotManager.getTotalProducerSlots(pid) > 1) {
        continue;
      }
      eligible.add(new ProducerCandidate(pid, emaMbps, connectedToTarget));
    }
    if (eligible.isEmpty()) {
      return null;
    }

    // Primary sort: ascending by EMA in routine (lightest load first -> graceful
    // swaps), descending in drain (heaviest load first -> impactful shifts).
    // Tiebreaker: connected-to-target before unconnected at equal load.
    Comparator<ProducerCandidate> primary = drainMode
        ? (a, b) -> Double.compare(b.emaMbps, a.emaMbps)
        : (a, b) -> Double.compare(a.emaMbps, b.emaMbps);
    Comparator<ProducerCandidate> tiebreaker =
        (a, b) -> Boolean.compare(b.connectedToTarget, a.connectedToTarget);
    eligible.sort(primary.thenComparing(tiebreaker));

    int topN = Math.min(Math.max(1, config.getTopNProducers()), eligible.size());
    ProducerCandidate chosen = eligible.get(ThreadLocalRandom.current().nextInt(topN));
    logger.info("[" + brokerId + "] picking producer: pid=" + chosen.pid
        + " emaMbps=" + String.format("%.2f", chosen.emaMbps)
        + " connectedToTarget=" + chosen.connectedToTarget
        + " mode=" + (drainMode ? "drain" : "routine")
        + " topN=" + topN
        + " eligible=" + eligible.size()
        + " target=" + targetIp);
    return chosen.pid;
  }

  /**
   * Gate {@link #tryConsolidation} on cluster-wide convergence. The three
   * "no eviction needed" early-returns in {@link #evaluate} each fire on
   * local-relative conditions (e.g. "I am at-or-above the mean", "the
   * chosen target is no freer than me", "the gap to the freest peer is
   * inside the deadband"). None of those imply the cluster as a whole is
   * balanced -- in particular, a hot tail peer (low free slots) is
   * consistent with all of them when local happens to be on the freer
   * side. Firing consolidation in that case wastes a tick on rearranging
   * connections instead of letting the genuinely hot broker shed.
   * <p>
   * The spread is {@code max(freeSlots) - min(freeSlots)} over the
   * topic-sharing population including local, expressed as a percentage
   * of {@code totalSlots}. We only consolidate when it is at or below the
   * effective {@code consolidationPercentageThreshold} deadband, which is
   * clamped at runtime to be no tighter than the eviction deadband.
   */
  private EvictionResult maybeConsolidate(SlotManager slotManager,
                                          Map<String, GossipState> peerStates,
                                          Map<String, Map<String, Integer>> producerConnections,
                                          Map<String, Set<String>> brokerToTopics,
                                          Set<String> localTopics,
                                          boolean clusterConverged,
                                          int clusterMaxFree,
                                          int clusterMinFree,
                                          double spreadPct,
                                          double consolidationThreshold) {
    if (!clusterConverged) {
      logger.info("[" + brokerId + "] consolidation skipped: cluster diverged"
          + " (clusterMaxFree=" + clusterMaxFree
          + " clusterMinFree=" + clusterMinFree
          + " spread=" + String.format("%.2f", spreadPct) + "%"
          + " > consolidationThreshold=" + String.format("%.2f", consolidationThreshold) + "%)"
          + " -- local is on the freer side of a diverged cluster; let the"
          + " hot peer(s) shed before opportunistic moves");
      return null;
    }
    return tryConsolidation(slotManager, peerStates, producerConnections,
        brokerToTopics, localTopics, clusterMaxFree, clusterMinFree, spreadPct);
  }

  /**
   * Steady-state consolidation pass. Invoked by {@link #maybeConsolidate}
   * only when the cluster is genuinely near-balanced. Picks the above-cap
   * producer with the lowest source-slot count on this broker, and evicts
   * one slot of it to an already-connected target chosen lexicographically
   * by (slot count the producer holds there desc, target free slots desc)
   * -- so the receiving broker is unlikely to pick the same producer for
   * another consolidation immediately (anti-ping-pong), and we shift to the
   * freest available broker among the qualifying ones.
   * <p>
   * The eviction is a normal 1-slot eviction over the existing directive
   * channel; the only difference is the trigger condition and the log tag.
   * <p>
   * Logs at three points: {@code consolidation eviction:} when a move is
   * made, {@code consolidation skipped: no above-cap producer} when no
   * over-cap producer holds slots here, and {@code consolidation skipped:
   * no eligible target} when every above-cap producer's connected peers
   * are frozen or in pending-eviction cooldown.
   */
  private EvictionResult tryConsolidation(SlotManager slotManager,
                                          Map<String, GossipState> peerStates,
                                          Map<String, Map<String, Integer>> producerConnections,
                                          Map<String, Set<String>> brokerToTopics,
                                          Set<String> localTopics,
                                          int clusterMaxFree,
                                          int clusterMinFree,
                                          double spreadPct) {
    int maxConns = config.getMaxConnectionsPerProducer();
    if (maxConns <= 0 || producerConnections.isEmpty()) {
      logger.info("[" + brokerId + "] consolidation skipped: nothing to consolidate"
          + " (maxConnPerProducer=" + maxConns
          + " v4Producers=" + producerConnections.size() + ")");
      return null;
    }

    // Above-cap producers that hold at least one slot here, sorted ascending
    // by source-slot count so single-slot ones (which drop the source
    // connection on this eviction) are tried first, then 2-slot ones (which
    // drop on the second consolidation), etc.
    List<ConsolidationCandidate> candidates = new ArrayList<>();
    for (Map.Entry<String, Map<String, Integer>> entry : producerConnections.entrySet()) {
      String pid = entry.getKey();
      Map<String, Integer> conns = entry.getValue() != null
          ? entry.getValue() : Collections.emptyMap();
      if (conns.size() <= maxConns) {
        continue;
      }
      int sourceSlots = slotManager.getTotalProducerSlots(pid);
      if (sourceSlots <= 0) {
        continue;
      }
      candidates.add(new ConsolidationCandidate(pid, sourceSlots, conns));
    }
    if (candidates.isEmpty()) {
      logger.info("[" + brokerId + "] consolidation skipped: no above-cap producer holds slots here"
          + " (maxConnPerProducer=" + maxConns
          + " v4Producers=" + producerConnections.size()
          + " clusterSpread=" + String.format("%.2f", spreadPct) + "%)");
      return null;
    }
    candidates.sort((a, b) -> Integer.compare(a.sourceSlots, b.sourceSlots));

    long now = System.currentTimeMillis();
    for (ConsolidationCandidate cand : candidates) {
      String targetIp = pickConsolidationTarget(slotManager, peerStates,
          brokerToTopics, cand);
      if (targetIp == null) {
        continue;
      }
      pendingEvictionTargets.put(targetIp, now);
      // Re-derive the log-only fields with O(1) reads on the (rare) success
      // path -- both maps are still in scope and hold the same values the
      // selection saw, so this avoids materializing a result holder just to
      // carry two numbers into the log line.
      Integer slotsBoxed = cand.conns.get(targetIp);
      int targetProducerSlots = slotsBoxed == null ? 0 : slotsBoxed;
      int targetFreeSlots = peerStates.get(targetIp).getMessage().getFreeSlots();
      EvictionResult result = new EvictionResult(cand.pid, targetIp, 1);
      logger.info("[" + brokerId + "] consolidation eviction: pid=" + cand.pid
          + " target=" + targetIp + " slotsToEvict=1"
          + " (connectionCount=" + cand.conns.size()
          + " > cap=" + maxConns
          + ", sourceSlots=" + cand.sourceSlots
          + ", targetProducerSlots=" + targetProducerSlots
          + ", targetFreeSlots=" + targetFreeSlots
          + ", clusterSpread=" + String.format("%.2f", spreadPct) + "%)");
      return result;
    }
    logger.info("[" + brokerId + "] consolidation skipped: no eligible target for "
        + candidates.size() + " above-cap producer(s)"
        + " (clusterMaxFree=" + clusterMaxFree
        + " clusterMinFree=" + clusterMinFree
        + " clusterSpread=" + String.format("%.2f", spreadPct) + "%)"
        + " -- every connected peer is frozen, in pending-eviction cooldown,"
        + " or doesn't serve the producer's topic");
    return null;
  }

  /**
   * Pick the eviction target IP for a consolidation candidate. Targets must be
   * already-connected (so this never adds a new edge), serve a topic the
   * producer writes, not be frozen, and not be in pending-eviction cooldown.
   * The winner is the lexicographic max by (slot count the producer holds there
   * desc, target free slots desc), tracked over primitive locals so the whole
   * selection allocates nothing -- the returned reference is an existing
   * connection-map key.
   *
   * @return the chosen target IP, or {@code null} if no connected peer qualifies.
   */
  private String pickConsolidationTarget(SlotManager slotManager,
                                         Map<String, GossipState> peerStates,
                                         Map<String, Set<String>> brokerToTopics,
                                         ConsolidationCandidate cand) {
    Collection<String> producerTopics = slotManager.getProducerTopics(cand.pid);
    String bestIp = null;
    int bestProducerSlots = -1;
    int bestFree = -1;
    for (Map.Entry<String, Integer> conn : cand.conns.entrySet()) {
      String targetIp = conn.getKey();
      if (brokerId.equals(targetIp)) {
        continue;
      }
      if (pendingEvictionTargets.containsKey(targetIp)) {
        continue;
      }
      GossipState peer = peerStates.get(targetIp);
      if (peer == null || peer.getMessage().isFreeze()) {
        continue;
      }
      Set<String> targetTopics = brokerToTopics.getOrDefault(targetIp,
          Collections.emptySet());
      if (targetTopics.isEmpty()
          || !writesToServedTopic(producerTopics, targetTopics)) {
        continue;
      }
      int producerSlotsOnTarget = conn.getValue() == null ? 0 : conn.getValue();
      int targetFree = peer.getMessage().getFreeSlots();
      // Lexicographic preference: producer's slot count on the target (desc),
      // then the target's free slots (desc). First occurrence wins ties.
      if (bestIp == null
          || producerSlotsOnTarget > bestProducerSlots
          || (producerSlotsOnTarget == bestProducerSlots && targetFree > bestFree)) {
        bestIp = targetIp;
        bestProducerSlots = producerSlotsOnTarget;
        bestFree = targetFree;
      }
    }
    return bestIp;
  }

  private static final class ConsolidationCandidate {
    final String pid;
    final int sourceSlots;
    final Map<String, Integer> conns;

    ConsolidationCandidate(String pid, int sourceSlots, Map<String, Integer> conns) {
      this.pid = pid;
      this.sourceSlots = sourceSlots;
      this.conns = conns;
    }
  }

  private static final class ProducerCandidate {
    final String pid;
    final double emaMbps;
    final boolean connectedToTarget;

    ProducerCandidate(String pid, double emaMbps, boolean connectedToTarget) {
      this.pid = pid;
      this.emaMbps = emaMbps;
      this.connectedToTarget = connectedToTarget;
    }
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

  /**
   * Pick a target from the sorted (free-slots desc) candidate list using a
   * <b>rank-based linear decay</b>: weight {@code = topN - rank} for
   * {@code rank} in {@code [0, topN)}. For {@code topN=3} this is weights
   * {@code 3, 2, 1} or probabilities {@code 50%, 33%, 17%}; for
   * {@code topN=5} it is weights {@code 5, 4, 3, 2, 1} or probabilities
   * {@code 33%, 27%, 20%, 13%, 7%}.
   * <p>
   * This replaces the previous free-slot-proportional weighting where, with
   * top-3 of e.g. {@code [18, 17, 15]}, the picks were {@code 36%, 34%, 30%}
   * -- almost flat, so the freest broker won only marginally more often than
   * the third-freest. Linear decay always concentrates roughly half the
   * probability mass on the freest peer regardless of free-slot magnitude,
   * which is what we want to favor (a more decisive shift toward the freest
   * peer) while still keeping enough randomization to break deterministic
   * cycles where the same target gets hammered tick after tick.
   */
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
    // Weights: topN, topN-1, ..., 1. Sum = topN * (topN + 1) / 2.
    int totalWeight = topN * (topN + 1) / 2;
    double rand = ThreadLocalRandom.current().nextDouble() * totalWeight;
    double cumulative = 0;
    for (int rank = 0; rank < topN; rank++) {
      cumulative += (topN - rank);
      if (rand < cumulative) {
        return topCandidates.get(rank);
      }
    }
    return topCandidates.get(topN - 1);
  }
}

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

import com.pinterest.memq.core.gossip.GossipState;
import com.pinterest.memq.core.slot.SlotManager;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Pluggable strategy for deciding when and where to evict producer slots.
 * Implementations receive the current broker's slot state, gossip-reported
 * peer states, per-producer connection information, and the cluster's
 * topic-to-broker assignment so eviction targets can be restricted to
 * brokers that actually serve the producer's topics.
 * <p>
 * <b>Protocol version contract:</b> Implementations MUST only target v4+
 * producers for eviction, because v3 producers cannot interpret eviction
 * responses. The {@code producerConnections} map serves as the v4+ registry:
 * only producers that have sent a v4+ write request will appear in it.
 * <p>
 * <b>Topic affinity contract:</b> Implementations MUST only evict a
 * producer to a broker that serves at least one of the producer's topics.
 * Sending a producer to a non-serving broker would just yield REDIRECT and
 * trigger an expensive client-side metadata refresh + reconnect.
 */
public interface EvictionStrategy {

  /**
   * Evaluate whether an eviction should be performed.
   *
   * @param slotManager this broker's slot manager
   * @param peerStates gossip state from peer brokers (brokerId to GossipState)
   * @param producerConnections per-producer connection view: producer id to
   *        (broker IP to slot count). The key set of the inner map is the
   *        connection set; the values are the producer's per-target slot
   *        ownership snapshot on each broker.
   * @param topicToBrokerIps topic to set of broker IPs that serve writes for it
   * @return an EvictionResult if eviction is warranted, or null if no eviction
   */
  EvictionResult evaluate(SlotManager slotManager,
                          Map<String, GossipState> peerStates,
                          Map<String, Map<String, Integer>> producerConnections,
                          Map<String, Set<String>> topicToBrokerIps);

  /**
   * Evaluate one or more evictions for this cycle. The default is the
   * single-decision behavior: zero or one eviction, matching {@link #evaluate}.
   * <p>
   * Implementations may override this to return a batch of evictions in a
   * single cycle -- e.g. a saturated (drain-latched) broker shedding enough
   * load at once to close a large imbalance, instead of one producer per cycle
   * (which converges in {@code O(num_producers)} cycles). Each returned result
   * is an independent pending eviction keyed by its producer id.
   *
   * @param slotManager this broker's slot manager
   * @param peerStates gossip state from peer brokers (brokerId to GossipState)
   * @param producerConnections per-producer connection view (see {@link #evaluate})
   * @param topicToBrokerIps topic to set of broker IPs that serve writes for it
   * @return the evictions to schedule this cycle; empty if none are warranted
   */
  default List<EvictionResult> evaluateBatch(SlotManager slotManager,
                                             Map<String, GossipState> peerStates,
                                             Map<String, Map<String, Integer>> producerConnections,
                                             Map<String, Set<String>> topicToBrokerIps) {
    EvictionResult result = evaluate(slotManager, peerStates, producerConnections,
        topicToBrokerIps);
    return result == null ? Collections.emptyList() : Collections.singletonList(result);
  }
}

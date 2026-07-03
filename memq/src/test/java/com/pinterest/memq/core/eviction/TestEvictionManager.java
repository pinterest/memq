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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import com.pinterest.memq.core.config.EvictionConfig;
import com.pinterest.memq.core.config.SlotAccountingConfig;
import com.pinterest.memq.core.gossip.GossipState;
import com.pinterest.memq.core.slot.SlotManager;

import org.junit.Test;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

public class TestEvictionManager {

  private SlotManager createSlotManager() {
    SlotAccountingConfig config = new SlotAccountingConfig();
    config.setEnabled(true);
    config.setSlotSizeMbps(10.0);
    return new SlotManager(config, 20);
  }

  @Test
  public void testPollEvictionReturnsAndRemoves() {
    EvictionConfig config = new EvictionConfig();
    config.setEnabled(true);

    EvictionStrategy mockStrategy = (sm, peers, conns, topics) ->
        new EvictionResult("uuid-producer-1", "10.0.0.5", 1);

    EvictionManager manager = new EvictionManager(mockStrategy, createSlotManager(),
        Collections::emptyMap, config);

    manager.runEviction();

    EvictionResult result = manager.pollEviction("uuid-producer-1");
    assertNotNull(result);
    assertEquals("10.0.0.5", result.getTargetBrokerIp());
    assertEquals(1, result.getNumSlotsToEvict());

    // second poll should return null (already removed)
    assertNull(manager.pollEviction("uuid-producer-1"));
  }

  @Test
  public void testPollEvictionReturnsNullWhenNone() {
    EvictionConfig config = new EvictionConfig();
    config.setEnabled(true);

    EvictionStrategy mockStrategy = (sm, peers, conns, topics) -> null;

    EvictionManager manager = new EvictionManager(mockStrategy, createSlotManager(),
        Collections::emptyMap, config);

    assertNull(manager.pollEviction("nonexistent-uuid"));
  }

  @Test
  public void testNewEvictionOverwritesOld() {
    EvictionConfig config = new EvictionConfig();
    config.setEnabled(true);

    final int[] callCount = {0};
    EvictionStrategy mockStrategy = (sm, peers, conns, topics) -> {
      callCount[0]++;
      return new EvictionResult("uuid-producer-1", "target-" + callCount[0], 1);
    };

    EvictionManager manager = new EvictionManager(mockStrategy, createSlotManager(),
        Collections::emptyMap, config);

    manager.runEviction();
    assertEquals(1, manager.getPendingCount());

    manager.runEviction();
    assertEquals(1, manager.getPendingCount());

    EvictionResult result = manager.pollEviction("uuid-producer-1");
    assertNotNull(result);
    assertEquals("target-2", result.getTargetBrokerIp());
  }

  @Test
  public void testPeekDoesNotRemove() {
    EvictionConfig config = new EvictionConfig();
    config.setEnabled(true);

    EvictionStrategy mockStrategy = (sm, peers, conns, topics) ->
        new EvictionResult("uuid-producer-1", "10.0.0.5", 1);

    EvictionManager manager = new EvictionManager(mockStrategy, createSlotManager(),
        Collections::emptyMap, config);

    manager.runEviction();

    EvictionResult peek1 = manager.peekEviction("uuid-producer-1");
    assertNotNull(peek1);
    EvictionResult peek2 = manager.peekEviction("uuid-producer-1");
    assertNotNull(peek2);
    assertEquals(1, manager.getPendingCount());
  }

  @Test
  public void testStartStop() throws InterruptedException {
    EvictionConfig config = new EvictionConfig();
    config.setEnabled(true);
    config.setIntervalSeconds(0.1);

    final int[] callCount = {0};
    EvictionStrategy mockStrategy = (sm, peers, conns, topics) -> {
      callCount[0]++;
      return null;
    };

    EvictionManager manager = new EvictionManager(mockStrategy, createSlotManager(),
        Collections::emptyMap, config);
    manager.start();
    Thread.sleep(500);
    manager.stop();

    assertTrue("Expected at least 2 eviction runs, got " + callCount[0], callCount[0] >= 2);
  }

  /**
   * Only producers writing to a topic with balancing opted in are eligible,
   * and the enabled-topic set is read live every tick -- so toggling it at
   * runtime activates/deactivates a topic's producers without a restart.
   */
  @Test
  public void testPerTopicBalancingFilterIsLiveEachTick() {
    EvictionConfig config = new EvictionConfig();
    config.setEnabled(true);

    SlotManager sm = createSlotManager();
    // Register two producers, each on a distinct topic, both connected.
    sm.recordWrite("p-a", "topicA", 1000);
    sm.recordWrite("p-b", "topicB", 1000);
    sm.recordProducerConnections("p-a",
        new HashSet<>(Collections.singletonList("broker-2")));
    sm.recordProducerConnections("p-b",
        new HashSet<>(Collections.singletonList("broker-2")));

    // Capture the producer set the strategy actually sees each tick.
    final Set<String> seenPids = new HashSet<>();
    EvictionStrategy capturingStrategy = (s, peers, conns, topics) -> {
      seenPids.clear();
      seenPids.addAll(conns.keySet());
      return null;
    };

    // Mutable enabled-topic set, read live by the manager on each tick.
    final Set<String> enabledTopics = new HashSet<>();
    enabledTopics.add("topicA");

    Supplier<Map<String, GossipState>> peersSupplier = Collections::emptyMap;
    Supplier<Map<String, Set<String>>> topicToBrokerIps = Collections::emptyMap;
    Supplier<Set<String>> enabledTopicsSupplier = () -> enabledTopics;
    EvictionManager manager = new EvictionManager(capturingStrategy, sm,
        peersSupplier, topicToBrokerIps, enabledTopicsSupplier, config);

    // Only topicA opted in -> only p-a is eligible.
    manager.runEviction();
    assertEquals(Collections.singleton("p-a"), seenPids);

    // Flip enablement at runtime (no restart): topicB on, topicA off.
    enabledTopics.clear();
    enabledTopics.add("topicB");
    manager.runEviction();
    assertEquals(Collections.singleton("p-b"), seenPids);

    // No topic opted in -> no producers eligible.
    enabledTopics.clear();
    manager.runEviction();
    assertTrue("no producers should be eligible when no topic opts in",
        seenPids.isEmpty());
  }

  private static void assertTrue(String msg, boolean condition) {
    if (!condition) throw new AssertionError(msg);
  }
}

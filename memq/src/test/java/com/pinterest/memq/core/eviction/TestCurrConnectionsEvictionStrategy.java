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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.pinterest.memq.core.config.EvictionConfig;
import com.pinterest.memq.core.config.SlotAccountingConfig;
import com.pinterest.memq.core.gossip.GossipMessage;
import com.pinterest.memq.core.gossip.GossipState;
import com.pinterest.memq.core.slot.SlotManager;

import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Method;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class TestCurrConnectionsEvictionStrategy {

  private static final int MB = 1024 * 1024;
  private static final String TOPIC = "testTopic";
  private static final String BROKER_LOCAL = "broker-local";

  private EvictionConfig evictionConfig;
  private SlotAccountingConfig slotConfig;

  @Before
  public void setUp() {
    evictionConfig = new EvictionConfig();
    evictionConfig.setEnabled(true);
    evictionConfig.setEvictionPercentageThreshold(0.0);
    evictionConfig.setPendingEvictionCooldownSeconds(0.0);
    evictionConfig.setTopNTargets(3);
    slotConfig = new SlotAccountingConfig();
    slotConfig.setEnabled(true);
    slotConfig.setSlotSizeMbps(10.0);
    slotConfig.setSlotOverhead(0.0);
    slotConfig.setAcquireThresholdSeconds(0.0);
    slotConfig.setReleaseThresholdSeconds(0.0);
    slotConfig.setCooldownSeconds(0.0);
    slotConfig.setEmaWindowSeconds(0.001);
    slotConfig.setTickIntervalMs(1000);
  }

  private SlotManager createSlotManager(int totalSlots) {
    return new SlotManager(slotConfig, totalSlots);
  }

  private void acquireSlots(SlotManager sm, String pid, String topic, int mbToWrite) throws Exception {
    sm.recordWrite(pid, topic, mbToWrite * MB);
    tickOnce(sm);
  }

  private void tickOnce(SlotManager sm) throws Exception {
    Method tick = SlotManager.class.getDeclaredMethod("tick");
    tick.setAccessible(true);
    tick.invoke(sm);
  }

  private Map<String, GossipState> peerWithFreeSlots(String peerId, int freeSlots) {
    GossipMessage msg = new GossipMessage(peerId, freeSlots, false, System.currentTimeMillis());
    GossipState state = new GossipState(msg, System.currentTimeMillis());
    return Collections.singletonMap(peerId, state);
  }

  /**
   * Topic-to-broker map that says every peer broker (and the local broker)
   * serves the single test {@link #TOPIC}. Use this as the 4th argument to
   * {@link CurrConnectionsEvictionStrategy#evaluate} in tests that don't
   * specifically exercise topic-affinity filtering.
   */
  private Map<String, Set<String>> allPeersServeTopic(Map<String, GossipState> peers) {
    return allPeersServeTopic(TOPIC, peers);
  }

  private Map<String, Set<String>> allPeersServeTopic(String topic,
                                                      Map<String, GossipState> peers) {
    Set<String> ips = new HashSet<>(peers.keySet());
    ips.add(BROKER_LOCAL);
    return Collections.singletonMap(topic, ips);
  }

  /** Topic-to-broker map declaring multiple topics, all served by every peer + local. */
  private Map<String, Set<String>> allPeersServeTopics(Map<String, GossipState> peers,
                                                       String... topics) {
    Set<String> ips = new HashSet<>(peers.keySet());
    ips.add(BROKER_LOCAL);
    Map<String, Set<String>> map = new HashMap<>();
    for (String t : topics) {
      map.put(t, ips);
    }
    return map;
  }

  /** EvictionManager constructor for tests, defaulting to "all peers serve TOPIC". */
  private EvictionManager managerWith(EvictionStrategy strategy,
                                      SlotManager sm,
                                      Map<String, GossipState> peers,
                                      EvictionConfig config) {
    Map<String, Set<String>> topicMap = allPeersServeTopic(peers);
    return new EvictionManager(strategy, sm, () -> peers, () -> topicMap, config);
  }

  // -----------------------------------------------------------------------
  // Basic eviction (v4 producer) — sanity check
  // -----------------------------------------------------------------------

  @Test
  public void testEvictsV4ProducerConnectedToTarget() throws Exception {
    SlotManager sm = createSlotManager(20);
    acquireSlots(sm, "producer-1", TOPIC, 150);

    Set<String> connections = new HashSet<>();
    connections.add("broker-2");
    sm.recordProducerConnections("producer-1", connections);

    CurrConnectionsEvictionStrategy strategy =
        new CurrConnectionsEvictionStrategy(BROKER_LOCAL, evictionConfig);

    Map<String, GossipState> peers = peerWithFreeSlots("broker-2", 15);
    EvictionResult result = strategy.evaluate(sm, peers, sm.getProducerConnections(), allPeersServeTopic(peers));

    assertNotNull("should evict a v4 producer", result);
    assertEquals("producer-1", result.getPid());
    assertEquals("broker-2", result.getTargetBrokerIp());
  }

  // -----------------------------------------------------------------------
  // v3 backward compatibility — core tests
  // -----------------------------------------------------------------------

  @Test
  public void testV3OnlyProducersNeverEvicted() throws Exception {
    SlotManager sm = createSlotManager(20);
    // v3 producer writes and acquires slots, but never calls recordProducerConnections
    acquireSlots(sm, "v3-producer", TOPIC, 150);
    assertTrue("v3 producer should have slots", sm.producerHasSlots("v3-producer"));

    CurrConnectionsEvictionStrategy strategy =
        new CurrConnectionsEvictionStrategy(BROKER_LOCAL, evictionConfig);

    Map<String, GossipState> peers = peerWithFreeSlots("broker-2", 15);

    // producerConnections is empty — no v4 producers registered
    EvictionResult result = strategy.evaluate(sm, peers, sm.getProducerConnections(), allPeersServeTopic(peers));

    assertNull("must NOT evict v3 producers", result);
  }

  @Test
  public void testMixedV3V4OnlyV4Evicted() throws Exception {
    SlotManager sm = createSlotManager(20);

    // v3 producer with many slots
    acquireSlots(sm, "v3-producer", TOPIC, 150);
    assertTrue(sm.producerHasSlots("v3-producer"));

    // v4 producer with slots
    acquireSlots(sm, "v4-producer", TOPIC, 15);
    assertTrue(sm.producerHasSlots("v4-producer"));
    sm.recordProducerConnections("v4-producer",
        new HashSet<>(Collections.singletonList("broker-2")));

    CurrConnectionsEvictionStrategy strategy =
        new CurrConnectionsEvictionStrategy(BROKER_LOCAL, evictionConfig);

    Map<String, GossipState> peers = peerWithFreeSlots("broker-2", 15);

    // Run multiple evaluations to verify v3 is never selected
    for (int i = 0; i < 50; i++) {
      EvictionResult result = strategy.evaluate(sm, peers, sm.getProducerConnections(), allPeersServeTopic(peers));
      if (result != null) {
        assertEquals("only v4 producer should be evicted", "v4-producer", result.getPid());
      }
    }
  }

  @Test
  public void testV3ProducerSlotsNotReleasedByEviction() throws Exception {
    SlotManager sm = createSlotManager(20);

    acquireSlots(sm, "v3-producer", TOPIC, 150);
    int v3Slots = sm.getTotalProducerSlots("v3-producer");
    assertTrue(v3Slots > 0);
    int occupiedBefore = sm.getOccupiedSlots();

    CurrConnectionsEvictionStrategy strategy =
        new CurrConnectionsEvictionStrategy(BROKER_LOCAL, evictionConfig);

    Map<String, GossipState> peers = peerWithFreeSlots("broker-2", 15);
    EvictionResult result = strategy.evaluate(sm, peers, sm.getProducerConnections(), allPeersServeTopic(peers));

    assertNull("no eviction for v3-only cluster", result);
    assertEquals("v3 slots unchanged", v3Slots, sm.getTotalProducerSlots("v3-producer"));
    assertEquals("occupied slots unchanged", occupiedBefore, sm.getOccupiedSlots());
  }

  @Test
  public void testFallbackPathOnlyConsidersV4Producers() throws Exception {
    SlotManager sm = createSlotManager(20);

    // v3 producer
    acquireSlots(sm, "v3-producer", TOPIC, 150);

    // v4 producer that is NOT connected to the target broker
    acquireSlots(sm, "v4-producer", TOPIC, 15);
    sm.recordProducerConnections("v4-producer",
        new HashSet<>(Collections.singletonList("broker-3")));

    CurrConnectionsEvictionStrategy strategy =
        new CurrConnectionsEvictionStrategy(BROKER_LOCAL, evictionConfig);

    // Target is broker-2, but v4-producer is connected to broker-3
    // The "prefer connected" path won't find a match, so it falls back to random v4
    Map<String, GossipState> peers = peerWithFreeSlots("broker-2", 15);

    for (int i = 0; i < 50; i++) {
      EvictionResult result = strategy.evaluate(sm, peers, sm.getProducerConnections(), allPeersServeTopic(peers));
      if (result != null) {
        assertEquals("fallback must only pick v4 producer", "v4-producer", result.getPid());
      }
    }
  }

  @Test
  public void testSkipsZeroSlotProducerAndEvictsRealHolder() throws Exception {
    // Regression: producerHasSlots() is containsKey(), and recordWrite()
    // re-creates a zero-slot ProducerSlotState on every write. A producer that
    // was evicted to 0 but keeps writing under backpressure therefore still
    // looks like an eviction candidate. If it is also the one connected to the
    // target (the preferred pick), eviction burns the tick on a no-op release
    // that frees nothing, while the producer actually holding the slots is
    // never evicted -- so localFreeSlots stays pinned at 0.
    SlotManager sm = createSlotManager(20);

    // Real holder: acquires slots, but is NOT connected to the target broker
    // (so it can only be chosen via the fallback path).
    acquireSlots(sm, "holder", TOPIC, 150);
    assertTrue("holder should own slots", sm.getTotalProducerSlots("holder") > 0);
    sm.recordProducerConnections("holder",
        new HashSet<>(Collections.singletonList("broker-3")));

    // Slotless producer: has a (re-created) state entry but zero slots, and IS
    // connected to the target -- i.e. the producer that the old "prefer
    // connected" logic would wrongly select.
    sm.recordWrite("slotless", TOPIC, 15 * MB); // no tick -> never acquires
    assertTrue("slotless has a state entry", sm.producerHasSlots("slotless"));
    assertEquals("slotless holds no slots", 0, sm.getTotalProducerSlots("slotless"));
    sm.recordProducerConnections("slotless",
        new HashSet<>(Collections.singletonList("broker-2")));

    CurrConnectionsEvictionStrategy strategy =
        new CurrConnectionsEvictionStrategy(BROKER_LOCAL, evictionConfig);

    Map<String, GossipState> peers = peerWithFreeSlots("broker-2", 15);

    boolean evictedHolder = false;
    for (int i = 0; i < 50; i++) {
      EvictionResult result = strategy.evaluate(sm, peers,
          sm.getProducerConnections(), allPeersServeTopic(peers));
      if (result != null) {
        assertEquals("must never evict the zero-slot producer",
            "holder", result.getPid());
        evictedHolder = true;
      }
    }
    assertTrue("eviction should target the real slot holder", evictedHolder);
  }

  @Test
  public void testEvictsHeavyUnconnectedProducerOverLightConnected() throws Exception {
    // The heavy producer drives saturation; a light producer connected to the
    // target must NOT shield it. When the unconnected producer is heavier than
    // the connected one by more than the margin, evict the heavy one even
    // though it costs a new producer-side connection -- otherwise the light
    // eviction's freed slot is just reabsorbed by the heavy, backpressured one.
    SlotManager sm = createSlotManager(20);

    // Both producers must record demand within the same tick so they hold
    // slots simultaneously (the test EMA window is ~instant, so an idle tick
    // would decay a producer back to zero slots).
    sm.recordWrite("heavy", TOPIC, 150 * MB);   // ceil(150/10)=15 slots
    sm.recordWrite("light", TOPIC, 15 * MB);    // ceil(15/10)=2 slots
    tickOnce(sm);
    assertTrue(sm.getTotalProducerSlots("heavy") > 10);
    assertTrue(sm.getTotalProducerSlots("light") > 0);

    // Heavy connected only to a non-target broker; light connected to target.
    sm.recordProducerConnections("heavy",
        new HashSet<>(Collections.singletonList("broker-3")));
    sm.recordProducerConnections("light",
        new HashSet<>(Collections.singletonList("broker-2")));

    CurrConnectionsEvictionStrategy strategy =
        new CurrConnectionsEvictionStrategy(BROKER_LOCAL, evictionConfig);

    Map<String, GossipState> peers = peerWithFreeSlots("broker-2", 15);

    EvictionResult result = strategy.evaluate(sm, peers,
        sm.getProducerConnections(), allPeersServeTopic(peers));

    assertNotNull(result);
    assertEquals("must evict the heavy producer, not the light connected one",
        "heavy", result.getPid());
  }

  @Test
  public void testPrefersConnectedProducerWithinSlotMargin() throws Exception {
    // When the connected producer is within the margin of the heaviest, keep
    // the cheap behavior: evict the already-connected one (no new connection).
    SlotManager sm = createSlotManager(20);

    // Unconnected slightly heavier (5 slots) than connected (4 slots) -- diff
    // of 1 is within the default margin of 2, so the connected producer wins.
    sm.recordWrite("unconnected", TOPIC, 50 * MB); // ceil(50/10)=5
    sm.recordWrite("connected", TOPIC, 40 * MB);   // ceil(40/10)=4
    tickOnce(sm);
    assertEquals(5, sm.getTotalProducerSlots("unconnected"));
    assertEquals(4, sm.getTotalProducerSlots("connected"));

    sm.recordProducerConnections("unconnected",
        new HashSet<>(Collections.singletonList("broker-3")));
    sm.recordProducerConnections("connected",
        new HashSet<>(Collections.singletonList("broker-2")));

    CurrConnectionsEvictionStrategy strategy =
        new CurrConnectionsEvictionStrategy(BROKER_LOCAL, evictionConfig);

    Map<String, GossipState> peers = peerWithFreeSlots("broker-2", 18);

    boolean fired = false;
    for (int i = 0; i < 20; i++) {
      EvictionResult result = strategy.evaluate(sm, peers,
          sm.getProducerConnections(), allPeersServeTopic(peers));
      if (result != null) {
        fired = true;
        assertEquals("within margin, prefer the connected producer",
            "connected", result.getPid());
      }
    }
    assertTrue("eviction should have fired at least once", fired);
  }

  @Test
  public void testNoPendingEvictionLeakForV3Producers() throws Exception {
    SlotManager sm = createSlotManager(20);
    acquireSlots(sm, "v3-producer", TOPIC, 150);

    EvictionConfig config = new EvictionConfig();
    config.setEnabled(true);
    config.setIntervalSeconds(0.1);
    config.setEvictionPercentageThreshold(0.0);
    config.setPendingEvictionCooldownSeconds(0.0);

    CurrConnectionsEvictionStrategy strategy =
        new CurrConnectionsEvictionStrategy(BROKER_LOCAL, config);

    Map<String, GossipState> peers = peerWithFreeSlots("broker-2", 15);

    EvictionManager manager = managerWith(strategy, sm, peers, config);
    manager.runEviction();

    assertEquals("no pending eviction should be created for v3 producers",
        0, manager.getPendingCount());
    assertNull(manager.pollEviction("v3-producer"));
  }

  // -----------------------------------------------------------------------
  // End-to-end: EvictionManager + v3/v4 mix
  // -----------------------------------------------------------------------

  @Test
  public void testEvictionManagerEndToEndWithV3V4Mix() throws Exception {
    SlotManager sm = createSlotManager(20);

    // v3 producer (heavy load, but should be ignored)
    acquireSlots(sm, "v3-heavy", TOPIC, 150);

    // v4 producer (lighter load)
    acquireSlots(sm, "v4-light", TOPIC, 15);
    sm.recordProducerConnections("v4-light",
        new HashSet<>(Collections.singletonList("broker-2")));

    EvictionConfig config = new EvictionConfig();
    config.setEnabled(true);
    config.setEvictionPercentageThreshold(0.0);
    config.setPendingEvictionCooldownSeconds(0.0);
    config.setTopNTargets(3);


    CurrConnectionsEvictionStrategy strategy =
        new CurrConnectionsEvictionStrategy(BROKER_LOCAL, config);

    Map<String, GossipState> peers = peerWithFreeSlots("broker-2", 15);

    EvictionManager manager = managerWith(strategy, sm, peers, config);
    manager.runEviction();

    assertNull("v3 producer must have no pending eviction",
        manager.peekEviction("v3-heavy"));
    EvictionResult result = manager.pollEviction("v4-light");
    if (result != null) {
      assertEquals("v4-light", result.getPid());
      assertEquals("broker-2", result.getTargetBrokerIp());
    }
  }

  @Test
  public void testV3ProducerSlotAccountingStillWorks() throws Exception {
    SlotManager sm = createSlotManager(32);

    // v3 producer should still get slots from normal slot accounting
    acquireSlots(sm, "v3-producer", TOPIC, 25);

    assertTrue("v3 producer should have slots via slot accounting",
        sm.producerHasSlots("v3-producer"));
    assertEquals(3, sm.getTotalProducerSlots("v3-producer"));
    assertTrue(sm.getProducerTopics("v3-producer").contains(TOPIC));
  }

  // -----------------------------------------------------------------------
  // Multi-producer same IP — producerId (UUID) isolation
  // -----------------------------------------------------------------------

  @Test
  public void testTwoProducersSameIpIsolatedByUuid() throws Exception {
    SlotManager sm = createSlotManager(32);

    String uuid1 = "uuid-producer-A";
    String uuid2 = "uuid-producer-B";

    // Both producers on the same host, different topics
    // Write both before ticking to avoid EMA decay zeroing out the first
    sm.recordWrite(uuid1, "topicA", 15 * MB);
    sm.recordWrite(uuid2, "topicB", 25 * MB);
    Method tick = SlotManager.class.getDeclaredMethod("tick");
    tick.setAccessible(true);
    tick.invoke(sm);

    assertEquals(2, sm.getTotalProducerSlots(uuid1));
    assertEquals(3, sm.getTotalProducerSlots(uuid2));
    assertTrue(sm.getProducerTopics(uuid1).contains("topicA"));
    assertFalse(sm.getProducerTopics(uuid1).contains("topicB"));
    assertTrue(sm.getProducerTopics(uuid2).contains("topicB"));

    // Register connections separately — no clobbering
    sm.recordProducerConnections(uuid1,
        new HashSet<>(Collections.singletonList("broker-2")));
    sm.recordProducerConnections(uuid2,
        new HashSet<>(Collections.singletonList("broker-3")));

    assertEquals(1, sm.getProducerConnections().get(uuid1).size());
    assertTrue(sm.getProducerConnections().get(uuid1).contains("broker-2"));
    assertEquals(1, sm.getProducerConnections().get(uuid2).size());
    assertTrue(sm.getProducerConnections().get(uuid2).contains("broker-3"));
  }

  @Test
  public void testEvictionTargetsCorrectUuidNotSibling() throws Exception {
    SlotManager sm = createSlotManager(32);

    String uuid1 = "uuid-A";
    String uuid2 = "uuid-B";

    sm.recordWrite(uuid1, "topicA", 15 * MB);
    sm.recordWrite(uuid2, "topicB", 25 * MB);
    Method tick = SlotManager.class.getDeclaredMethod("tick");
    tick.setAccessible(true);
    tick.invoke(sm);

    sm.recordProducerConnections(uuid1,
        new HashSet<>(Collections.singletonList("broker-2")));
    sm.recordProducerConnections(uuid2,
        new HashSet<>(Collections.singletonList("broker-2")));

    CurrConnectionsEvictionStrategy strategy =
        new CurrConnectionsEvictionStrategy(BROKER_LOCAL, evictionConfig);

    Map<String, GossipState> peers = peerWithFreeSlots("broker-2", 15);

    // Run many evaluations — evicted pid must always be one of the two UUIDs
    Map<String, Set<String>> topicMap = allPeersServeTopics(peers, "topicA", "topicB");
    Set<String> evictedPids = new HashSet<>();
    for (int i = 0; i < 50; i++) {
      EvictionResult result = strategy.evaluate(sm, peers, sm.getProducerConnections(), topicMap);
      if (result != null) {
        evictedPids.add(result.getPid());
        assertTrue("evicted pid must be a UUID, not an IP",
            result.getPid().equals(uuid1) || result.getPid().equals(uuid2));
      }
    }
  }

  @Test
  public void testEvictionSlotReleaseIsolatedPerUuid() throws Exception {
    SlotManager sm = createSlotManager(32);

    String uuid1 = "uuid-A";
    String uuid2 = "uuid-B";

    sm.recordWrite(uuid1, "topicA", 15 * MB);
    sm.recordWrite(uuid2, "topicB", 25 * MB);
    Method tick = SlotManager.class.getDeclaredMethod("tick");
    tick.setAccessible(true);
    tick.invoke(sm);

    int uuid1SlotsBefore = sm.getTotalProducerSlots(uuid1);
    int uuid2SlotsBefore = sm.getTotalProducerSlots(uuid2);

    // Release slots from uuid1 only
    sm.releaseProducerSlots(uuid1, "topicA", 1);

    assertEquals(uuid1SlotsBefore - 1, sm.getTotalProducerSlots(uuid1));
    assertEquals("uuid2 slots must be untouched",
        uuid2SlotsBefore, sm.getTotalProducerSlots(uuid2));
  }

  // -----------------------------------------------------------------------
  // Core eviction decision logic
  // -----------------------------------------------------------------------

  @Test
  public void testNoEvictionWhenLocalFreeAboveMean() throws Exception {
    SlotManager sm = createSlotManager(20);
    acquireSlots(sm, "v4-producer", TOPIC, 15);
    sm.recordProducerConnections("v4-producer",
        new HashSet<>(Collections.singletonList("broker-2")));

    // Local has 18 free, peers have 5 and 10 → mean = 7.5. Local is well above mean.
    Map<String, GossipState> peers = new HashMap<>();
    peers.putAll(peerWithFreeSlots("broker-2", 5));
    peers.putAll(peerWithFreeSlots("broker-3", 10));

    CurrConnectionsEvictionStrategy strategy =
        new CurrConnectionsEvictionStrategy(BROKER_LOCAL, evictionConfig);

    EvictionResult result = strategy.evaluate(sm, peers, sm.getProducerConnections(), allPeersServeTopic(peers));
    assertNull("should not evict when local free slots >= mean", result);
  }

  @Test
  public void testEvictsWhenLocalFreeBelowMean() throws Exception {
    SlotManager sm = createSlotManager(20);
    // Fill up local broker so it has very few free slots
    acquireSlots(sm, "v4-producer", TOPIC, 150);
    sm.recordProducerConnections("v4-producer",
        new HashSet<>(Collections.singletonList("broker-2")));
    assertTrue(sm.getFreeSlots() < 20);

    // Peers have lots of free slots → mean is high, local is below
    Map<String, GossipState> peers = new HashMap<>();
    peers.putAll(peerWithFreeSlots("broker-2", 18));
    peers.putAll(peerWithFreeSlots("broker-3", 16));

    CurrConnectionsEvictionStrategy strategy =
        new CurrConnectionsEvictionStrategy(BROKER_LOCAL, evictionConfig);

    EvictionResult result = strategy.evaluate(sm, peers, sm.getProducerConnections(), allPeersServeTopic(peers));
    assertNotNull("should evict when local free slots < mean", result);
  }

  @Test
  public void testTargetBrokerWeightedByFreeSlots() throws Exception {
    SlotManager sm = createSlotManager(20);
    acquireSlots(sm, "v4-producer", TOPIC, 150);
    sm.recordProducerConnections("v4-producer",
        new HashSet<>(Collections.singletonList("broker-high")));

    // broker-high has many more free slots than broker-low
    Map<String, GossipState> peers = new HashMap<>();
    peers.putAll(peerWithFreeSlots("broker-high", 100));
    peers.putAll(peerWithFreeSlots("broker-low", 1));

    EvictionConfig config = new EvictionConfig();
    config.setEnabled(true);
    config.setEvictionPercentageThreshold(0.0);
    config.setPendingEvictionCooldownSeconds(0.0);
    config.setTopNTargets(2);


    CurrConnectionsEvictionStrategy strategy =
        new CurrConnectionsEvictionStrategy(BROKER_LOCAL, config);

    Map<String, Integer> targetCounts = new HashMap<>();
    int trials = 1000;
    for (int i = 0; i < trials; i++) {
      EvictionResult result = strategy.evaluate(sm, peers, sm.getProducerConnections(), allPeersServeTopic(peers));
      if (result != null) {
        targetCounts.merge(result.getTargetBrokerIp(), 1, Integer::sum);
      }
    }

    int highCount = targetCounts.getOrDefault("broker-high", 0);
    int lowCount = targetCounts.getOrDefault("broker-low", 0);
    assertTrue("broker-high (100 free) should be selected much more often than broker-low (1 free),"
        + " but got high=" + highCount + " low=" + lowCount,
        highCount > lowCount * 5);
  }

  @Test
  public void testPrefersProducerConnectedToTarget() throws Exception {
    SlotManager sm = createSlotManager(20);

    // Two v4 producers, both with slots
    acquireSlots(sm, "connected-producer", TOPIC, 50);
    acquireSlots(sm, "unconnected-producer", TOPIC, 50);

    // Only connected-producer is connected to the target broker
    sm.recordProducerConnections("connected-producer",
        new HashSet<>(Collections.singletonList("broker-2")));
    sm.recordProducerConnections("unconnected-producer",
        new HashSet<>(Collections.singletonList("broker-3")));

    EvictionConfig config = new EvictionConfig();
    config.setEnabled(true);
    config.setEvictionPercentageThreshold(0.0);
    config.setPendingEvictionCooldownSeconds(0.0);
    config.setTopNTargets(1);


    CurrConnectionsEvictionStrategy strategy =
        new CurrConnectionsEvictionStrategy(BROKER_LOCAL, config);

    Map<String, GossipState> peers = peerWithFreeSlots("broker-2", 15);

    for (int i = 0; i < 100; i++) {
      EvictionResult result = strategy.evaluate(sm, peers, sm.getProducerConnections(), allPeersServeTopic(peers));
      if (result != null) {
        assertEquals("should always prefer the producer connected to target",
            "connected-producer", result.getPid());
      }
    }
  }

  @Test
  public void testPercentageThresholdPreventsEviction() throws Exception {
    SlotManager sm = createSlotManager(100);
    acquireSlots(sm, "v4-producer", TOPIC, 150);
    sm.recordProducerConnections("v4-producer",
        new HashSet<>(Collections.singletonList("broker-2")));

    // Peer has slightly more free slots than local (small difference)
    int localFree = sm.getFreeSlots();
    int peerFree = localFree + 5;

    Map<String, GossipState> peers = peerWithFreeSlots("broker-2", peerFree);

    EvictionConfig config = new EvictionConfig();
    config.setEnabled(true);
    config.setEvictionPercentageThreshold(50.0);
    config.setPendingEvictionCooldownSeconds(0.0);
    config.setTopNTargets(3);


    CurrConnectionsEvictionStrategy strategy =
        new CurrConnectionsEvictionStrategy(BROKER_LOCAL, config);

    EvictionResult result = strategy.evaluate(sm, peers, sm.getProducerConnections(), allPeersServeTopic(peers));
    assertNull("should not evict when percentage difference is below threshold", result);
  }

  @Test
  public void testCooldownPreventsRepeatedEvictionToSameTarget() throws Exception {
    SlotManager sm = createSlotManager(20);
    acquireSlots(sm, "v4-producer", TOPIC, 150);
    sm.recordProducerConnections("v4-producer",
        new HashSet<>(Collections.singletonList("broker-2")));

    EvictionConfig config = new EvictionConfig();
    config.setEnabled(true);
    config.setEvictionPercentageThreshold(0.0);
    config.setPendingEvictionCooldownSeconds(60.0);
    config.setTopNTargets(3);


    CurrConnectionsEvictionStrategy strategy =
        new CurrConnectionsEvictionStrategy(BROKER_LOCAL, config);

    Map<String, GossipState> peers = peerWithFreeSlots("broker-2", 15);

    EvictionResult first = strategy.evaluate(sm, peers, sm.getProducerConnections(), allPeersServeTopic(peers));
    assertNotNull("first eviction should succeed", first);
    assertEquals("broker-2", first.getTargetBrokerIp());

    EvictionResult second = strategy.evaluate(sm, peers, sm.getProducerConnections(), allPeersServeTopic(peers));
    assertNull("second eviction to same target should be blocked by cooldown", second);
  }

  @Test
  public void testFrozenPeersExcludedFromTargets() throws Exception {
    SlotManager sm = createSlotManager(20);
    acquireSlots(sm, "v4-producer", TOPIC, 150);
    sm.recordProducerConnections("v4-producer",
        new HashSet<>(Collections.singletonList("broker-frozen")));

    // broker-frozen has lots of free slots but is frozen
    Map<String, GossipState> peers = new HashMap<>();
    GossipMessage frozenMsg = new GossipMessage("broker-frozen", 50, true, System.currentTimeMillis());
    peers.put("broker-frozen", new GossipState(frozenMsg, System.currentTimeMillis()));

    CurrConnectionsEvictionStrategy strategy =
        new CurrConnectionsEvictionStrategy(BROKER_LOCAL, evictionConfig);

    EvictionResult result = strategy.evaluate(sm, peers, sm.getProducerConnections(), allPeersServeTopic(peers));
    assertNull("should not evict to a frozen broker", result);
  }

  @Test
  public void testFrozenPeerSkippedNonFrozenChosen() throws Exception {
    SlotManager sm = createSlotManager(20);
    acquireSlots(sm, "v4-producer", TOPIC, 150);
    sm.recordProducerConnections("v4-producer",
        new HashSet<>(Collections.singletonList("broker-ok")));

    Map<String, GossipState> peers = new HashMap<>();
    GossipMessage frozenMsg = new GossipMessage("broker-frozen", 50, true, System.currentTimeMillis());
    peers.put("broker-frozen", new GossipState(frozenMsg, System.currentTimeMillis()));
    GossipMessage okMsg = new GossipMessage("broker-ok", 15, false, System.currentTimeMillis());
    peers.put("broker-ok", new GossipState(okMsg, System.currentTimeMillis()));

    EvictionConfig config = new EvictionConfig();
    config.setEnabled(true);
    config.setEvictionPercentageThreshold(0.0);
    config.setPendingEvictionCooldownSeconds(0.0);
    config.setTopNTargets(3);


    CurrConnectionsEvictionStrategy strategy =
        new CurrConnectionsEvictionStrategy(BROKER_LOCAL, config);

    for (int i = 0; i < 50; i++) {
      EvictionResult result = strategy.evaluate(sm, peers, sm.getProducerConnections(), allPeersServeTopic(peers));
      if (result != null) {
        assertEquals("should always target the non-frozen broker", "broker-ok", result.getTargetBrokerIp());
      }
    }
  }

  // -----------------------------------------------------------------------
  // Multi-producer same IP — producerId (UUID) isolation
  // -----------------------------------------------------------------------

  @Test
  public void testPendingEvictionIsolatedPerUuid() throws Exception {
    SlotManager sm = createSlotManager(32);

    String uuid1 = "uuid-A";
    String uuid2 = "uuid-B";

    sm.recordWrite(uuid1, "topicA", 15 * MB);
    sm.recordWrite(uuid2, "topicB", 25 * MB);
    Method tick = SlotManager.class.getDeclaredMethod("tick");
    tick.setAccessible(true);
    tick.invoke(sm);

    sm.recordProducerConnections(uuid1,
        new HashSet<>(Collections.singletonList("broker-2")));
    sm.recordProducerConnections(uuid2,
        new HashSet<>(Collections.singletonList("broker-2")));

    EvictionConfig config = new EvictionConfig();
    config.setEnabled(true);
    config.setEvictionPercentageThreshold(0.0);
    config.setPendingEvictionCooldownSeconds(0.0);
    config.setTopNTargets(3);


    CurrConnectionsEvictionStrategy strategy =
        new CurrConnectionsEvictionStrategy(BROKER_LOCAL, config);

    Map<String, GossipState> peers = peerWithFreeSlots("broker-2", 20);
    Map<String, Set<String>> topicMap = allPeersServeTopics(peers, "topicA", "topicB");

    EvictionManager manager = new EvictionManager(strategy, sm, () -> peers,
        () -> topicMap, config);
    manager.runEviction();

    // At most 1 pending eviction per run, targeting one specific UUID
    assertTrue(manager.getPendingCount() <= 1);
    if (manager.getPendingCount() == 1) {
      // Polling the other UUID must return null
      EvictionResult r1 = manager.peekEviction(uuid1);
      EvictionResult r2 = manager.peekEviction(uuid2);
      assertTrue("exactly one UUID should have a pending eviction",
          (r1 != null) ^ (r2 != null));
    }
  }

  // -----------------------------------------------------------------------
  // Topic-affinity guard: target broker must serve a topic this broker
  // serves AND the chosen producer's topic must be served by the target.
  //
  // Regression for the scenario where producers were sent to brokers that
  // didn't own their topic processor, triggering REDIRECT responses and
  // forcing client-side reconnects (NetworkClient.reset() on the producer).
  // -----------------------------------------------------------------------

  @Test
  public void testEvictionSkippedWhenNoPeerSharesTopic() throws Exception {
    SlotManager sm = createSlotManager(20);
    acquireSlots(sm, "producer-1", TOPIC, 150);
    sm.recordProducerConnections("producer-1",
        new HashSet<>(Collections.singletonList("broker-2")));

    CurrConnectionsEvictionStrategy strategy =
        new CurrConnectionsEvictionStrategy(BROKER_LOCAL, evictionConfig);

    Map<String, GossipState> peers = peerWithFreeSlots("broker-2", 15);

    // broker-2 advertises a different topic. Even though it has free slots
    // and a connected v4 producer, redirecting "producer-1" (which writes
    // TOPIC) there would just bounce back as REDIRECT.
    Map<String, Set<String>> topicMap = new HashMap<>();
    topicMap.put(TOPIC, Collections.singleton(BROKER_LOCAL));
    topicMap.put("otherTopic", Collections.singleton("broker-2"));

    EvictionResult result = strategy.evaluate(sm, peers,
        sm.getProducerConnections(), topicMap);

    assertNull("must not evict when no peer serves a topic this broker also serves",
        result);
  }

  @Test
  public void testEvictionSkippedWhenProducerTopicNotServedByTarget() throws Exception {
    // producer-1 writes "topicX" (only local serves topicX). broker-2
    // shares "shellTopic" with local (passes broker-stage filter) but
    // serves no topic this producer writes -- so producer selection
    // returns null and no eviction happens.
    SlotManager sm = createSlotManager(20);
    acquireSlots(sm, "producer-1", "topicX", 150);
    sm.recordProducerConnections("producer-1",
        new HashSet<>(Collections.singletonList("broker-2")));

    CurrConnectionsEvictionStrategy strategy =
        new CurrConnectionsEvictionStrategy(BROKER_LOCAL, evictionConfig);

    Map<String, GossipState> peers = peerWithFreeSlots("broker-2", 15);

    Map<String, Set<String>> topicMap = new HashMap<>();
    topicMap.put("shellTopic",
        new HashSet<>(java.util.Arrays.asList(BROKER_LOCAL, "broker-2")));
    topicMap.put("topicX", Collections.singleton(BROKER_LOCAL));

    EvictionResult result = strategy.evaluate(sm, peers,
        sm.getProducerConnections(), topicMap);

    assertNull("must not evict producer to a target that doesn't serve its topic",
        result);
  }

  @Test
  public void testEvictionTargetsPeerSharingProducerTopic() throws Exception {
    SlotManager sm = createSlotManager(20);
    acquireSlots(sm, "producer-1", TOPIC, 150);
    sm.recordProducerConnections("producer-1",
        new HashSet<>(Collections.singletonList("broker-2")));

    CurrConnectionsEvictionStrategy strategy =
        new CurrConnectionsEvictionStrategy(BROKER_LOCAL, evictionConfig);

    // Two peers: broker-2 only serves "otherTopic" (not TOPIC); broker-3
    // serves TOPIC. Strategy must select broker-3 -- broker-2 is filtered
    // even though it has a connected producer.
    Map<String, GossipState> peers = new HashMap<>();
    peers.put("broker-2",
        new GossipState(new GossipMessage("broker-2", 18, false,
            System.currentTimeMillis()), System.currentTimeMillis()));
    peers.put("broker-3",
        new GossipState(new GossipMessage("broker-3", 15, false,
            System.currentTimeMillis()), System.currentTimeMillis()));

    Map<String, Set<String>> topicMap = new HashMap<>();
    topicMap.put(TOPIC,
        new HashSet<>(java.util.Arrays.asList(BROKER_LOCAL, "broker-3")));
    topicMap.put("otherTopic", Collections.singleton("broker-2"));

    EvictionResult result = strategy.evaluate(sm, peers,
        sm.getProducerConnections(), topicMap);

    assertNotNull("must evict to broker-3 (the only peer serving TOPIC)", result);
    assertEquals("broker-3", result.getTargetBrokerIp());
    assertEquals("producer-1", result.getPid());
  }
}

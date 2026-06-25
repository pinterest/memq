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

import java.lang.reflect.Field;
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
    // These tests acquire whole-gap in a single tick; opt out of the per-tick
    // step clamp (covered by TestSlotManagerSlotStep).
    slotConfig.setMaxSlotStep(0);
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

  /**
   * Drain-mode tests need the latch engaged. The latch EMA decays slowly by
   * design (so a single eviction's brief free-slot spike cannot disengage it),
   * which makes natural engagement in unit tests prohibitively slow -- so the
   * test forces it via reflection. Production engagement is exercised by
   * {@code TestSlotManagerDrainLatch}.
   */
  private static void setDrainLatched(SlotManager sm, boolean latched) throws Exception {
    Field f = SlotManager.class.getDeclaredField("drainLatched");
    f.setAccessible(true);
    f.set(sm, latched);
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
  public void testRoutineModeEvictsLightestProducerForGracefulSwap() throws Exception {
    // Routine mode (drain latch not engaged) sorts producers ascending by
    // source-slot count. The lightest producer is preferred so the eviction
    // is a "graceful swap" -- ideally a producer with a single source slot,
    // whose connection naturally drops on eviction with no client-side
    // disruption. The heavy producer is intentionally NOT picked in routine
    // mode, even though it holds far more of the saturated broker's load,
    // because moving it would force a client connection drop (against the
    // maxConnectionsPerProducer cap) if it is also at the cap.
    SlotManager sm = createSlotManager(20);

    sm.recordWrite("heavy", TOPIC, 150 * MB);   // ceil(150/10)=15 slots
    sm.recordWrite("light", TOPIC, 15 * MB);    // ceil(15/10)=2 slots
    tickOnce(sm);
    assertTrue(sm.getTotalProducerSlots("heavy") > 10);
    assertTrue(sm.getTotalProducerSlots("light") > 0);

    sm.recordProducerConnections("heavy",
        new HashSet<>(Collections.singletonList("broker-3")));
    sm.recordProducerConnections("light",
        new HashSet<>(Collections.singletonList("broker-2")));

    // Deterministic pick: topNProducers=1 so the (ascending) top is always
    // chosen, no random jitter. The wider-topN behavior is covered separately.
    evictionConfig.setTopNProducers(1);
    CurrConnectionsEvictionStrategy strategy =
        new CurrConnectionsEvictionStrategy(BROKER_LOCAL, evictionConfig);

    Map<String, GossipState> peers = peerWithFreeSlots("broker-2", 15);

    EvictionResult result = strategy.evaluate(sm, peers,
        sm.getProducerConnections(), allPeersServeTopic(peers));

    assertNotNull(result);
    assertEquals("routine mode picks the lightest producer for a graceful swap",
        "light", result.getPid());
  }

  @Test
  public void testDrainModeEvictsHeaviestProducer() throws Exception {
    // Drain mode (drain latch engaged) sorts producers descending by
    // source-slot count. The heaviest is preferred because the broker is
    // saturated and only shifting a meaningful share of the load relieves
    // pressure -- evicting a light producer just lets the heavy one
    // reabsorb the freed slot.
    SlotManager sm = createSlotManager(20);

    sm.recordWrite("heavy", TOPIC, 150 * MB);
    sm.recordWrite("light", TOPIC, 15 * MB);
    tickOnce(sm);
    sm.recordProducerConnections("heavy",
        new HashSet<>(Collections.singletonList("broker-3")));
    sm.recordProducerConnections("light",
        new HashSet<>(Collections.singletonList("broker-2")));

    // Force drain mode (production engages it via slow EMA; test bypasses).
    setDrainLatched(sm, true);

    evictionConfig.setTopNProducers(1);
    CurrConnectionsEvictionStrategy strategy =
        new CurrConnectionsEvictionStrategy(BROKER_LOCAL, evictionConfig);

    Map<String, GossipState> peers = peerWithFreeSlots("broker-2", 15);

    EvictionResult result = strategy.evaluate(sm, peers,
        sm.getProducerConnections(), allPeersServeTopic(peers));

    assertNotNull(result);
    assertEquals("drain mode picks the heaviest producer to shift real load",
        "heavy", result.getPid());
  }

  @Test
  public void testRoutineModeSkipsCapViolatingEviction() throws Exception {
    // The candidate producer is already at the cap and holds multiple source
    // slots; the chosen target is a broker outside its connection set.
    // Evicting it would force the client to drop one of its existing
    // connections. Routine mode refuses the move so the producer keeps its
    // connections; the broker waits for a graceful-swap opportunity instead.
    // A non-v4 "filler" producer holds the rest of the slots so that the
    // congestion check passes (localFree < mean) and the eviction flow
    // actually reaches the cap-skip rule we are exercising here.
    SlotManager sm = createSlotManager(20);
    // Both writes must land within the same tick so both producers hold
    // slots simultaneously (the test EMA window is ~instant, so a tick
    // between them would decay the earlier producer back to zero slots).
    sm.recordWrite("filler", TOPIC, 150 * MB);              // 15 slots, no v4 conns
    sm.recordWrite("at-cap", TOPIC, 50 * MB);               // 5 slots, v4 candidate
    tickOnce(sm);
    assertTrue(sm.getTotalProducerSlots("at-cap") > 1);

    Set<String> existingConns = new HashSet<>();
    existingConns.add("broker-X");
    existingConns.add("broker-Y");
    existingConns.add("broker-Z");
    sm.recordProducerConnections("at-cap", existingConns);

    evictionConfig.setMaxConnectionsPerProducer(3);
    CurrConnectionsEvictionStrategy strategy =
        new CurrConnectionsEvictionStrategy(BROKER_LOCAL, evictionConfig);

    Map<String, GossipState> peers = peerWithFreeSlots("broker-2", 18);
    EvictionResult result = strategy.evaluate(sm, peers,
        sm.getProducerConnections(), allPeersServeTopic(peers));

    assertNull("routine mode must refuse evictions that would force a connection drop",
        result);
  }

  @Test
  public void testDrainModeAllowsCapViolatingEviction() throws Exception {
    // Same setup as testRoutineModeSkipsCapViolatingEviction, but the drain
    // latch is engaged. The broker is saturated and must shed load even at
    // the cost of a client-side connection drop, so the cap-skip rule is
    // relaxed and eviction proceeds.
    SlotManager sm = createSlotManager(20);
    sm.recordWrite("filler", TOPIC, 150 * MB);
    sm.recordWrite("at-cap", TOPIC, 50 * MB);
    tickOnce(sm);

    Set<String> existingConns = new HashSet<>();
    existingConns.add("broker-X");
    existingConns.add("broker-Y");
    existingConns.add("broker-Z");
    sm.recordProducerConnections("at-cap", existingConns);

    setDrainLatched(sm, true);

    evictionConfig.setMaxConnectionsPerProducer(3);
    CurrConnectionsEvictionStrategy strategy =
        new CurrConnectionsEvictionStrategy(BROKER_LOCAL, evictionConfig);

    Map<String, GossipState> peers = peerWithFreeSlots("broker-2", 18);
    EvictionResult result = strategy.evaluate(sm, peers,
        sm.getProducerConnections(), allPeersServeTopic(peers));

    assertNotNull("drain mode must allow cap-violating evictions to relieve saturation",
        result);
    assertEquals("at-cap", result.getPid());
  }

  @Test
  public void testRoutineModeAllowsGracefulSwapAtCap() throws Exception {
    // The producer is at the cap but holds only one source slot, so the
    // eviction will naturally drop the source connection and the connection
    // count stays at or below the cap. This is the "graceful swap" routine
    // mode is specifically structured to prefer -- it must not be skipped
    // even though target is not in the connection set. As above, a non-v4
    // filler producer drives the congestion check.
    SlotManager sm = createSlotManager(20);
    sm.recordWrite("filler", TOPIC, 150 * MB);
    sm.recordWrite("one-slot", TOPIC, 5 * MB);
    tickOnce(sm);
    assertEquals(1, sm.getTotalProducerSlots("one-slot"));

    Set<String> existingConns = new HashSet<>();
    existingConns.add("broker-X");
    existingConns.add("broker-Y");
    existingConns.add("broker-Z");
    sm.recordProducerConnections("one-slot", existingConns);

    evictionConfig.setMaxConnectionsPerProducer(3);
    CurrConnectionsEvictionStrategy strategy =
        new CurrConnectionsEvictionStrategy(BROKER_LOCAL, evictionConfig);

    Map<String, GossipState> peers = peerWithFreeSlots("broker-2", 18);
    EvictionResult result = strategy.evaluate(sm, peers,
        sm.getProducerConnections(), allPeersServeTopic(peers));

    assertNotNull("graceful swap (1 source slot) must not be blocked by cap check",
        result);
    assertEquals("one-slot", result.getPid());
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
    assertTrue(sm.getProducerConnections().get(uuid1).containsKey("broker-2"));
    assertEquals(1, sm.getProducerConnections().get(uuid2).size());
    assertTrue(sm.getProducerConnections().get(uuid2).containsKey("broker-3"));
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
    // Two producers with equal source-slot counts -- the connected-to-target
    // producer wins via the secondary tiebreaker. topNProducers=1 makes the
    // pick deterministic regardless of the random jitter that would otherwise
    // spread choices across the top-N.
    SlotManager sm = createSlotManager(20);

    acquireSlots(sm, "connected-producer", TOPIC, 50);
    acquireSlots(sm, "unconnected-producer", TOPIC, 50);

    sm.recordProducerConnections("connected-producer",
        new HashSet<>(Collections.singletonList("broker-2")));
    sm.recordProducerConnections("unconnected-producer",
        new HashSet<>(Collections.singletonList("broker-3")));

    EvictionConfig config = new EvictionConfig();
    config.setEnabled(true);
    config.setEvictionPercentageThreshold(0.0);
    config.setPendingEvictionCooldownSeconds(0.0);
    config.setTopNTargets(1);
    config.setTopNProducers(1);


    CurrConnectionsEvictionStrategy strategy =
        new CurrConnectionsEvictionStrategy(BROKER_LOCAL, config);

    Map<String, GossipState> peers = peerWithFreeSlots("broker-2", 15);

    for (int i = 0; i < 100; i++) {
      EvictionResult result = strategy.evaluate(sm, peers, sm.getProducerConnections(), allPeersServeTopic(peers));
      if (result != null) {
        assertEquals("at equal slot count the connected-to-target producer wins via tiebreaker",
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

  @Test
  public void testEvictsProducerWhenLocalCongestedRegardlessOfTarget() throws Exception {
    // Local broker is congested; either peer is a valid target. Target picks
    // are weighted random over the top-N, so we don't assert which target is
    // chosen -- only that an eviction fires and routes the single eligible
    // producer somewhere meaningful. Cap is not violated (producer holds 1
    // connection, well under the default cap), so routine mode does not skip.
    SlotManager sm = createSlotManager(32);
    acquireSlots(sm, "heavy", TOPIC, 200); // ~20 slots -> localFree ~12
    sm.recordProducerConnections("heavy",
        new HashSet<>(Collections.singletonList("broker-2")));

    CurrConnectionsEvictionStrategy strategy =
        new CurrConnectionsEvictionStrategy(BROKER_LOCAL, evictionConfig);

    long now = System.currentTimeMillis();
    Map<String, GossipState> peers = new HashMap<>();
    peers.put("broker-1",
        new GossipState(new GossipMessage("broker-1", 28, false, now), now));
    peers.put("broker-2",
        new GossipState(new GossipMessage("broker-2", 20, false, now), now));

    EvictionResult result = strategy.evaluate(sm, peers,
        sm.getProducerConnections(), allPeersServeTopic(peers));

    assertNotNull(result);
    assertEquals("heavy", result.getPid());
    assertTrue("target must be one of the two candidates",
        "broker-1".equals(result.getTargetBrokerIp())
            || "broker-2".equals(result.getTargetBrokerIp()));
  }

  @Test
  public void testFallsBackToBalanceTargetWhenNoConnectedTarget() throws Exception {
    // The producer is connected only to a broker that is not a candidate target,
    // so no candidate is swap-free. The strategy must still evict, falling back
    // to the balance-optimal target (accepting the client-side swap).
    SlotManager sm = createSlotManager(32);
    acquireSlots(sm, "heavy", TOPIC, 200);
    sm.recordProducerConnections("heavy",
        new HashSet<>(Collections.singletonList("broker-offsite")));

    EvictionConfig config = new EvictionConfig();
    config.setEnabled(true);
    config.setEvictionPercentageThreshold(0.0);
    config.setPendingEvictionCooldownSeconds(0.0);
    config.setTopNTargets(1); // deterministic: pick the coldest candidate
    CurrConnectionsEvictionStrategy strategy =
        new CurrConnectionsEvictionStrategy(BROKER_LOCAL, config);

    long now = System.currentTimeMillis();
    Map<String, GossipState> peers = new HashMap<>();
    peers.put("broker-1",
        new GossipState(new GossipMessage("broker-1", 28, false, now), now));
    peers.put("broker-2",
        new GossipState(new GossipMessage("broker-2", 20, false, now), now));

    EvictionResult result = strategy.evaluate(sm, peers,
        sm.getProducerConnections(), allPeersServeTopic(peers));

    assertNotNull("must still evict when no swap-free target exists", result);
    assertEquals("falls back to the balance-optimal (coldest) target",
        "broker-1", result.getTargetBrokerIp());
    assertEquals("heavy", result.getPid());
  }

  // -----------------------------------------------------------------------
  // Consolidation (steady-state, over-cap producers)
  // -----------------------------------------------------------------------

  /**
   * Build a slot map for an over-cap producer.
   */
  private static Map<String, Integer> slotMap(Object... pairs) {
    Map<String, Integer> m = new java.util.LinkedHashMap<>();
    for (int i = 0; i < pairs.length; i += 2) {
      m.put((String) pairs[i], (Integer) pairs[i + 1]);
    }
    return m;
  }

  @Test
  public void testConsolidationFiresWhenConvergedAndOverCap() throws Exception {
    // Setup: local and all peers have free-slot counts within the deadband
    // (cluster genuinely converged). Producer has 4 connections (cap=3)
    // and 1 slot here. Consolidation must fire.
    SlotManager sm = createSlotManager(32);
    acquireSlots(sm, "fat", TOPIC, 10); // 1 slot here -> local free=31

    EvictionConfig config = new EvictionConfig();
    config.setEnabled(true);
    config.setEvictionPercentageThreshold(10.0); // 32 * 10% = 3.2 slot spread allowance
    config.setPendingEvictionCooldownSeconds(0.0);
    config.setTopNTargets(1);
    config.setMaxConnectionsPerProducer(3);
    CurrConnectionsEvictionStrategy strategy =
        new CurrConnectionsEvictionStrategy(BROKER_LOCAL, config);

    long now = System.currentTimeMillis();
    Map<String, GossipState> peers = new HashMap<>();
    // Spread = 31-30 = 1 slot (3.1%), within the 10% deadband -> converged.
    peers.put("broker-1",
        new GossipState(new GossipMessage("broker-1", 30, false, now), now));
    peers.put("broker-2",
        new GossipState(new GossipMessage("broker-2", 31, false, now), now));
    peers.put("broker-3",
        new GossipState(new GossipMessage("broker-3", 31, false, now), now));

    sm.recordProducerConnections("fat",
        slotMap("broker-1", 5, "broker-2", 3, "broker-3", 2, BROKER_LOCAL, 1));

    EvictionResult result = strategy.evaluate(sm, peers,
        sm.getProducerConnections(), allPeersServeTopic(peers));

    assertNotNull("consolidation should fire in converged + over-cap state", result);
    assertEquals("fat", result.getPid());
    assertEquals("anti-ping-pong: pick target where producer owns most slots",
        "broker-1", result.getTargetBrokerIp());
  }

  @Test
  public void testConsolidationSuppressedWhenClusterDiverged() throws Exception {
    // Local is on the freer side of a diverged cluster (one hot peer well
    // below the deadband). Even though local would normally hit the
    // "no eviction needed" branch (we're above mean), consolidation must
    // be suppressed -- the hot peer needs to shed first.
    SlotManager sm = createSlotManager(32);
    acquireSlots(sm, "fat", TOPIC, 10); // local free=31

    EvictionConfig config = new EvictionConfig();
    config.setEnabled(true);
    config.setEvictionPercentageThreshold(10.0); // 32 * 10% = 3.2 slot spread allowance
    config.setPendingEvictionCooldownSeconds(0.0);
    config.setTopNTargets(1);
    config.setMaxConnectionsPerProducer(3);
    CurrConnectionsEvictionStrategy strategy =
        new CurrConnectionsEvictionStrategy(BROKER_LOCAL, config);

    long now = System.currentTimeMillis();
    Map<String, GossipState> peers = new HashMap<>();
    // Spread = 31-5 = 26 slots (81%), way above the 10% deadband.
    // localFreeSlots (31) >= meanFreeSlots so branch 1 fires, but the
    // cluster-spread gate must veto consolidation.
    peers.put("broker-1",
        new GossipState(new GossipMessage("broker-1", 5, false, now), now));
    peers.put("broker-2",
        new GossipState(new GossipMessage("broker-2", 28, false, now), now));
    peers.put("broker-3",
        new GossipState(new GossipMessage("broker-3", 30, false, now), now));

    // An above-cap producer is present -- pre-gate this test would have
    // consolidated. After the gate, it must NOT.
    sm.recordProducerConnections("fat",
        slotMap("broker-1", 2, "broker-2", 5, "broker-3", 3, BROKER_LOCAL, 1));

    EvictionResult result = strategy.evaluate(sm, peers,
        sm.getProducerConnections(), allPeersServeTopic(peers));

    assertNull("consolidation must be suppressed when the cluster is diverged",
        result);
  }

  @Test
  public void testConsolidationDoesNotFireWhenAtCap() throws Exception {
    // Producer has exactly 3 connections (=cap). Consolidation must not fire.
    SlotManager sm = createSlotManager(32);
    acquireSlots(sm, "ok", TOPIC, 10);

    EvictionConfig config = new EvictionConfig();
    config.setEnabled(true);
    config.setMaxConnectionsPerProducer(3);
    config.setEvictionPercentageThreshold(10.0);
    config.setPendingEvictionCooldownSeconds(0.0);
    config.setTopNTargets(1);
    CurrConnectionsEvictionStrategy strategy =
        new CurrConnectionsEvictionStrategy(BROKER_LOCAL, config);

    long now = System.currentTimeMillis();
    Map<String, GossipState> peers = new HashMap<>();
    // Cluster converged so the cluster-spread gate doesn't mask the test.
    peers.put("broker-1",
        new GossipState(new GossipMessage("broker-1", 31, false, now), now));

    sm.recordProducerConnections("ok",
        slotMap("broker-1", 5, "broker-2", 3, BROKER_LOCAL, 1));

    EvictionResult result = strategy.evaluate(sm, peers,
        sm.getProducerConnections(), allPeersServeTopic(peers));

    assertNull("consolidation should not fire when producer is at or below cap", result);
  }

  @Test
  public void testConsolidationDoesNotFireWhenBlockedByFrozenPeers() throws Exception {
    // Local is loaded (above mean -> wants to evict), but all peers are
    // frozen -> normal eviction is blocked. Consolidation must not stack on
    // top of a blocked eviction.
    SlotManager sm = createSlotManager(32);
    acquireSlots(sm, "fat", TOPIC, 250); // 25 slots, local free=7

    EvictionConfig config = new EvictionConfig();
    config.setEnabled(true);
    config.setMaxConnectionsPerProducer(3);
    config.setEvictionPercentageThreshold(0.0);
    config.setPendingEvictionCooldownSeconds(0.0);
    config.setTopNTargets(1);
    CurrConnectionsEvictionStrategy strategy =
        new CurrConnectionsEvictionStrategy(BROKER_LOCAL, config);

    long now = System.currentTimeMillis();
    Map<String, GossipState> peers = new HashMap<>();
    // freeze=true on every peer
    peers.put("broker-1",
        new GossipState(new GossipMessage("broker-1", 30, true, now), now));
    peers.put("broker-2",
        new GossipState(new GossipMessage("broker-2", 30, true, now), now));

    sm.recordProducerConnections("fat",
        slotMap("broker-1", 5, "broker-2", 5, "broker-3", 5, BROKER_LOCAL, 25));

    EvictionResult result = strategy.evaluate(sm, peers,
        sm.getProducerConnections(), allPeersServeTopic(peers));

    assertNull("consolidation should not fire when eviction is blocked", result);
  }

  @Test
  public void testConsolidationPrefersLowestSourceSlotProducer() throws Exception {
    // Two over-cap producers: "light" has 1 slot here, "heavy" has 10.
    // The lightest should be picked so it drops the source connection
    // on this eviction (graceful single-step drain).
    SlotManager sm = createSlotManager(32);
    // Concurrent writes -> single tick so both producers' EMAs land in the
    // same window. Calling acquireSlots twice would let the first producer's
    // EMA decay across the second tick.
    sm.recordWrite("light", TOPIC, 10 * MB);   // ceil(10/10)=1 slot
    sm.recordWrite("heavy", TOPIC, 100 * MB);  // ceil(100/10)=10 slots
    tickOnce(sm);

    EvictionConfig config = new EvictionConfig();
    config.setEnabled(true);
    config.setMaxConnectionsPerProducer(3);
    config.setEvictionPercentageThreshold(10.0);
    config.setPendingEvictionCooldownSeconds(0.0);
    config.setTopNTargets(1);
    CurrConnectionsEvictionStrategy strategy =
        new CurrConnectionsEvictionStrategy(BROKER_LOCAL, config);

    long now = System.currentTimeMillis();
    Map<String, GossipState> peers = new HashMap<>();
    // Local uses 11 slots (1+10), free=21. Keep peers within deadband.
    peers.put("broker-1",
        new GossipState(new GossipMessage("broker-1", 21, false, now), now));
    peers.put("broker-2",
        new GossipState(new GossipMessage("broker-2", 21, false, now), now));

    sm.recordProducerConnections("light",
        slotMap("broker-1", 3, "broker-2", 2, "broker-3", 1, BROKER_LOCAL, 1));
    sm.recordProducerConnections("heavy",
        slotMap("broker-1", 4, "broker-2", 2, "broker-3", 1, BROKER_LOCAL, 10));

    EvictionResult result = strategy.evaluate(sm, peers,
        sm.getProducerConnections(), allPeersServeTopic(peers));

    assertNotNull(result);
    assertEquals("lowest-source-slot producer should be picked first",
        "light", result.getPid());
  }

  @Test
  public void testConsolidationAntiPingPongTargetByProducerSlots() throws Exception {
    // Producer is over-cap with non-uniform slot distribution on peers.
    // Consolidation must pick the broker where the producer owns the most
    // slots so the receiving broker has the least incentive to consolidate
    // the producer right back to us.
    SlotManager sm = createSlotManager(32);
    acquireSlots(sm, "fat", TOPIC, 10);

    EvictionConfig config = new EvictionConfig();
    config.setEnabled(true);
    config.setMaxConnectionsPerProducer(3);
    config.setEvictionPercentageThreshold(10.0);
    config.setPendingEvictionCooldownSeconds(0.0);
    config.setTopNTargets(1);
    CurrConnectionsEvictionStrategy strategy =
        new CurrConnectionsEvictionStrategy(BROKER_LOCAL, config);

    long now = System.currentTimeMillis();
    Map<String, GossipState> peers = new HashMap<>();
    // All peers near local's free count (spread within deadband -> cluster
    // converged). Producer slot counts vary; broker-1 should win.
    peers.put("broker-1",
        new GossipState(new GossipMessage("broker-1", 30, false, now), now));
    peers.put("broker-2",
        new GossipState(new GossipMessage("broker-2", 30, false, now), now));
    peers.put("broker-3",
        new GossipState(new GossipMessage("broker-3", 31, false, now), now));

    sm.recordProducerConnections("fat",
        slotMap("broker-1", 12, "broker-2", 5, "broker-3", 1, BROKER_LOCAL, 1));

    EvictionResult result = strategy.evaluate(sm, peers,
        sm.getProducerConnections(), allPeersServeTopic(peers));

    assertNotNull(result);
    assertEquals("anti-ping-pong: pick target with highest producer slot count",
        "broker-1", result.getTargetBrokerIp());
  }

  @Test
  public void testConsolidationFreeSlotsTiebreakerWithEqualWeights() throws Exception {
    // Equal-weight fallback: producer-side wire format is v4 (no slot
    // counts), so all connection entries have value 1. With ties on the
    // primary key, target should be the freest.
    SlotManager sm = createSlotManager(32);
    acquireSlots(sm, "fat", TOPIC, 10);

    EvictionConfig config = new EvictionConfig();
    config.setEnabled(true);
    config.setMaxConnectionsPerProducer(3);
    config.setEvictionPercentageThreshold(10.0);
    config.setPendingEvictionCooldownSeconds(0.0);
    config.setTopNTargets(1);
    CurrConnectionsEvictionStrategy strategy =
        new CurrConnectionsEvictionStrategy(BROKER_LOCAL, config);

    long now = System.currentTimeMillis();
    Map<String, GossipState> peers = new HashMap<>();
    // Spread = 31-28 = 3 slots (9.4%), inside the 10% deadband -> converged.
    // broker-3 is the freest peer; with equal producer weights it should win.
    peers.put("broker-1",
        new GossipState(new GossipMessage("broker-1", 28, false, now), now));
    peers.put("broker-2",
        new GossipState(new GossipMessage("broker-2", 29, false, now), now));
    peers.put("broker-3",
        new GossipState(new GossipMessage("broker-3", 30, false, now), now));

    // Set<String> overload simulates a v4 producer: every entry is weight=1.
    Set<String> conns = new HashSet<>();
    conns.add("broker-1");
    conns.add("broker-2");
    conns.add("broker-3");
    conns.add(BROKER_LOCAL);
    sm.recordProducerConnections("fat", conns);

    EvictionResult result = strategy.evaluate(sm, peers,
        sm.getProducerConnections(), allPeersServeTopic(peers));

    assertNotNull("consolidation works in equal-weight mode too", result);
    assertEquals("with all weights equal, freest target wins",
        "broker-3", result.getTargetBrokerIp());
  }

  @Test
  public void testConsolidationSkipsFrozenAndPendingTargets() throws Exception {
    // Producer is over-cap. Of its 3 peer connections, broker-1 is frozen
    // and broker-2 is the only valid target. Consolidation must pick broker-2.
    SlotManager sm = createSlotManager(32);
    acquireSlots(sm, "fat", TOPIC, 10);

    EvictionConfig config = new EvictionConfig();
    config.setEnabled(true);
    config.setMaxConnectionsPerProducer(3);
    config.setEvictionPercentageThreshold(10.0);
    config.setPendingEvictionCooldownSeconds(0.0);
    config.setTopNTargets(1);
    CurrConnectionsEvictionStrategy strategy =
        new CurrConnectionsEvictionStrategy(BROKER_LOCAL, config);

    long now = System.currentTimeMillis();
    Map<String, GossipState> peers = new HashMap<>();
    // All peers near local's free count -> cluster converged. broker-1 has
    // the most producer slots but is frozen, so it must be excluded.
    peers.put("broker-1",
        new GossipState(new GossipMessage("broker-1", 30, true, now), now));
    peers.put("broker-2",
        new GossipState(new GossipMessage("broker-2", 30, false, now), now));
    peers.put("broker-3",
        new GossipState(new GossipMessage("broker-3", 31, false, now), now));

    sm.recordProducerConnections("fat",
        slotMap("broker-1", 20, "broker-2", 3, "broker-3", 2, BROKER_LOCAL, 1));

    EvictionResult result = strategy.evaluate(sm, peers,
        sm.getProducerConnections(), allPeersServeTopic(peers));

    assertNotNull(result);
    // broker-1 is best by producer slots but frozen -> excluded.
    // Between broker-2 (3 producer slots) and broker-3 (2 producer slots),
    // broker-2 has the higher producer-slot count -> picked.
    assertEquals("frozen target must be excluded; pick next-best",
        "broker-2", result.getTargetBrokerIp());
  }

  @Test
  public void testConsolidationSkipsZeroSlotProducer() throws Exception {
    // Over-cap producer with 0 slots here: nothing to evict, skip.
    SlotManager sm = createSlotManager(32);

    EvictionConfig config = new EvictionConfig();
    config.setEnabled(true);
    config.setMaxConnectionsPerProducer(3);
    config.setEvictionPercentageThreshold(10.0);
    config.setPendingEvictionCooldownSeconds(0.0);
    config.setTopNTargets(1);
    CurrConnectionsEvictionStrategy strategy =
        new CurrConnectionsEvictionStrategy(BROKER_LOCAL, config);

    long now = System.currentTimeMillis();
    Map<String, GossipState> peers = new HashMap<>();
    // Cluster converged (local free=32, peer free=32) so the gate passes
    // and we actually exercise the zero-slot rejection in tryConsolidation.
    peers.put("broker-1",
        new GossipState(new GossipMessage("broker-1", 32, false, now), now));

    // Register the over-cap producer but with 0 slots locally.
    sm.recordProducerConnections("ghost",
        slotMap("broker-1", 5, "broker-2", 3, "broker-3", 2, BROKER_LOCAL, 0));

    EvictionResult result = strategy.evaluate(sm, peers,
        sm.getProducerConnections(), allPeersServeTopic(peers));

    assertNull("over-cap producer with 0 source slots cannot be consolidated", result);
  }

  @Test
  public void testConsolidationSkipsLoadGapBelowThresholdAndFires() throws Exception {
    // Load gap is non-zero but below threshold (deadband) -> "converged"
    // branch -> consolidation fires.
    SlotManager sm = createSlotManager(32);
    acquireSlots(sm, "fat", TOPIC, 60); // 6 slots, free=26

    EvictionConfig config = new EvictionConfig();
    config.setEnabled(true);
    config.setMaxConnectionsPerProducer(3);
    // Big deadband: gap between 26 free and ~28 free is below threshold.
    config.setEvictionPercentageThreshold(50.0);
    config.setPendingEvictionCooldownSeconds(0.0);
    config.setTopNTargets(1);
    CurrConnectionsEvictionStrategy strategy =
        new CurrConnectionsEvictionStrategy(BROKER_LOCAL, config);

    long now = System.currentTimeMillis();
    Map<String, GossipState> peers = new HashMap<>();
    peers.put("broker-1",
        new GossipState(new GossipMessage("broker-1", 28, false, now), now));
    peers.put("broker-2",
        new GossipState(new GossipMessage("broker-2", 28, false, now), now));

    sm.recordProducerConnections("fat",
        slotMap("broker-1", 6, "broker-2", 3, "broker-3", 2, BROKER_LOCAL, 6));

    EvictionResult result = strategy.evaluate(sm, peers,
        sm.getProducerConnections(), allPeersServeTopic(peers));

    assertNotNull("converged-by-deadband should still allow consolidation", result);
    assertEquals("fat", result.getPid());
    assertEquals("broker-1", result.getTargetBrokerIp());
  }

  // ============================================================
  // Linear-decay top-N target weighting
  // ============================================================

  /**
   * Invoke the package-private {@code selectTargetBroker} via reflection so
   * tests can verify the weighting distribution directly without going
   * through {@code evaluate()} (which carries cooldown state that biases
   * repeated calls).
   */
  @SuppressWarnings("unchecked")
  private static Map.Entry<String, Integer> invokeSelectTargetBroker(
      CurrConnectionsEvictionStrategy strategy,
      java.util.List<Map.Entry<String, Integer>> sortedCandidates) throws Exception {
    Method m = CurrConnectionsEvictionStrategy.class.getDeclaredMethod(
        "selectTargetBroker", java.util.List.class);
    m.setAccessible(true);
    return (Map.Entry<String, Integer>) m.invoke(strategy, sortedCandidates);
  }

  @Test
  public void testLinearDecayTargetWeighting() throws Exception {
    // Direct invocation of selectTargetBroker to verify the rank-based
    // linear decay distribution. With topN=3 the expected probabilities
    // are 50% / 33% / 17% regardless of free-slot magnitudes (which is
    // exactly the property that distinguishes the new rank-based weighting
    // from the old free-slot-proportional weighting where, with free counts
    // 30/29/28, picks would be roughly equal).
    EvictionConfig config = new EvictionConfig();
    config.setTopNTargets(3);
    CurrConnectionsEvictionStrategy strategy =
        new CurrConnectionsEvictionStrategy(BROKER_LOCAL, config);

    java.util.List<Map.Entry<String, Integer>> candidates = new java.util.ArrayList<>();
    candidates.add(new java.util.AbstractMap.SimpleEntry<>("broker-1", 30));
    candidates.add(new java.util.AbstractMap.SimpleEntry<>("broker-2", 29));
    candidates.add(new java.util.AbstractMap.SimpleEntry<>("broker-3", 28));

    Map<String, Integer> picks = new HashMap<>();
    int trials = 30000;
    for (int i = 0; i < trials; i++) {
      Map.Entry<String, Integer> r = invokeSelectTargetBroker(strategy, candidates);
      picks.merge(r.getKey(), 1, Integer::sum);
    }

    double p1 = (double) picks.getOrDefault("broker-1", 0) / trials;
    double p2 = (double) picks.getOrDefault("broker-2", 0) / trials;
    double p3 = (double) picks.getOrDefault("broker-3", 0) / trials;
    // Expected 50/33/17; allow ±3 percentage points slack (30k trials,
    // binomial sd ~0.3pp, so 3pp is ~10 sigma).
    assertTrue("rank-1 should win ~50% (got " + p1 + ")",
        Math.abs(p1 - 0.50) < 0.03);
    assertTrue("rank-2 should win ~33% (got " + p2 + ")",
        Math.abs(p2 - 0.333) < 0.03);
    assertTrue("rank-3 should win ~17% (got " + p3 + ")",
        Math.abs(p3 - 0.167) < 0.03);
  }

  @Test
  public void testLinearDecayInvariantToFreeSlotMagnitude() throws Exception {
    // Distribution is purely rank-based, so wildly different free-slot
    // magnitudes ([18, 17, 15] vs [30, 4, 2]) produce the same probabilities.
    EvictionConfig config = new EvictionConfig();
    config.setTopNTargets(3);
    CurrConnectionsEvictionStrategy strategy =
        new CurrConnectionsEvictionStrategy(BROKER_LOCAL, config);

    java.util.List<Map.Entry<String, Integer>> closeMagnitudes = new java.util.ArrayList<>();
    closeMagnitudes.add(new java.util.AbstractMap.SimpleEntry<>("a1", 18));
    closeMagnitudes.add(new java.util.AbstractMap.SimpleEntry<>("a2", 17));
    closeMagnitudes.add(new java.util.AbstractMap.SimpleEntry<>("a3", 15));

    java.util.List<Map.Entry<String, Integer>> wideMagnitudes = new java.util.ArrayList<>();
    wideMagnitudes.add(new java.util.AbstractMap.SimpleEntry<>("b1", 30));
    wideMagnitudes.add(new java.util.AbstractMap.SimpleEntry<>("b2", 4));
    wideMagnitudes.add(new java.util.AbstractMap.SimpleEntry<>("b3", 2));

    int trials = 30000;
    int closeRank1 = 0, wideRank1 = 0;
    for (int i = 0; i < trials; i++) {
      if (invokeSelectTargetBroker(strategy, closeMagnitudes).getKey().equals("a1")) closeRank1++;
      if (invokeSelectTargetBroker(strategy, wideMagnitudes).getKey().equals("b1")) wideRank1++;
    }
    double closeP = (double) closeRank1 / trials;
    double wideP = (double) wideRank1 / trials;
    // Both should be ~50% (rank-based, magnitude-independent).
    assertTrue("close-magnitude rank-1 wins ~50% (got " + closeP + ")",
        Math.abs(closeP - 0.50) < 0.03);
    assertTrue("wide-magnitude rank-1 wins ~50% (got " + wideP + ")",
        Math.abs(wideP - 0.50) < 0.03);
  }

  @Test
  public void testLinearDecayTopNOne() throws Exception {
    // topN=1 collapses to deterministic top-1 pick (no randomization).
    EvictionConfig config = new EvictionConfig();
    config.setTopNTargets(1);
    CurrConnectionsEvictionStrategy strategy =
        new CurrConnectionsEvictionStrategy(BROKER_LOCAL, config);

    java.util.List<Map.Entry<String, Integer>> candidates = new java.util.ArrayList<>();
    candidates.add(new java.util.AbstractMap.SimpleEntry<>("broker-1", 30));
    candidates.add(new java.util.AbstractMap.SimpleEntry<>("broker-2", 20));

    for (int i = 0; i < 50; i++) {
      Map.Entry<String, Integer> r = invokeSelectTargetBroker(strategy, candidates);
      assertEquals("topN=1 must always pick the freest peer",
          "broker-1", r.getKey());
    }
  }

  // ============================================================
  // Decoupled consolidation threshold
  // ============================================================

  @Test
  public void testConsolidationFiresInLooseDeadbandOnly() throws Exception {
    // Cluster spread 12.5% (= 4 slots / 32) is OUTSIDE the eviction deadband
    // (10%) but INSIDE the consolidation deadband (20%). Local is at or above
    // mean so the broker enters the "no eviction needed" branch, and the
    // looser consolidation threshold should permit consolidation.
    SlotManager sm = createSlotManager(32);
    acquireSlots(sm, "fat", TOPIC, 10); // local free=31

    EvictionConfig config = new EvictionConfig();
    config.setEnabled(true);
    config.setEvictionPercentageThreshold(10.0);
    config.setConsolidationPercentageThreshold(20.0);
    config.setPendingEvictionCooldownSeconds(0.0);
    config.setTopNTargets(1);
    config.setMaxConnectionsPerProducer(3);
    CurrConnectionsEvictionStrategy strategy =
        new CurrConnectionsEvictionStrategy(BROKER_LOCAL, config);

    long now = System.currentTimeMillis();
    Map<String, GossipState> peers = new HashMap<>();
    // Spread = 31-27 = 4 slots (12.5%). > eviction threshold (10%), <= consolidation (20%).
    peers.put("broker-1",
        new GossipState(new GossipMessage("broker-1", 27, false, now), now));
    peers.put("broker-2",
        new GossipState(new GossipMessage("broker-2", 30, false, now), now));
    peers.put("broker-3",
        new GossipState(new GossipMessage("broker-3", 31, false, now), now));

    sm.recordProducerConnections("fat",
        slotMap("broker-1", 5, "broker-2", 3, "broker-3", 2, BROKER_LOCAL, 1));

    EvictionResult result = strategy.evaluate(sm, peers,
        sm.getProducerConnections(), allPeersServeTopic(peers));

    assertNotNull("loose consolidation deadband should permit the consolidation",
        result);
    assertEquals("fat", result.getPid());
    assertEquals("broker-1", result.getTargetBrokerIp());
  }

  @Test
  public void testConsolidationSuppressedAboveLooseDeadband() throws Exception {
    // Cluster spread 25% (= 8 slots / 32) is outside BOTH thresholds (eviction
    // 10%, consolidation 20%). Consolidation must not fire.
    SlotManager sm = createSlotManager(32);
    acquireSlots(sm, "fat", TOPIC, 10);

    EvictionConfig config = new EvictionConfig();
    config.setEnabled(true);
    config.setEvictionPercentageThreshold(10.0);
    config.setConsolidationPercentageThreshold(20.0);
    config.setPendingEvictionCooldownSeconds(0.0);
    config.setTopNTargets(1);
    config.setMaxConnectionsPerProducer(3);
    CurrConnectionsEvictionStrategy strategy =
        new CurrConnectionsEvictionStrategy(BROKER_LOCAL, config);

    long now = System.currentTimeMillis();
    Map<String, GossipState> peers = new HashMap<>();
    peers.put("broker-1",
        new GossipState(new GossipMessage("broker-1", 23, false, now), now));
    peers.put("broker-2",
        new GossipState(new GossipMessage("broker-2", 25, false, now), now));
    peers.put("broker-3",
        new GossipState(new GossipMessage("broker-3", 30, false, now), now));

    sm.recordProducerConnections("fat",
        slotMap("broker-1", 5, "broker-2", 3, "broker-3", 2, BROKER_LOCAL, 1));

    EvictionResult result = strategy.evaluate(sm, peers,
        sm.getProducerConnections(), allPeersServeTopic(peers));

    assertNull("consolidation must be suppressed when spread exceeds loose threshold",
        result);
  }

  @Test
  public void testConsolidationThresholdClampedToEvictionThreshold() throws Exception {
    // Misconfiguration: consolidation threshold (5%) < eviction threshold (10%).
    // The runtime clamp must use max() so consolidation can never be tighter
    // than the band where eviction is still actively trying to shed.
    SlotManager sm = createSlotManager(32);
    acquireSlots(sm, "fat", TOPIC, 10); // local free=31

    EvictionConfig config = new EvictionConfig();
    config.setEnabled(true);
    config.setEvictionPercentageThreshold(10.0);
    config.setConsolidationPercentageThreshold(5.0); // misconfigured tighter
    config.setPendingEvictionCooldownSeconds(0.0);
    config.setTopNTargets(1);
    config.setMaxConnectionsPerProducer(3);
    CurrConnectionsEvictionStrategy strategy =
        new CurrConnectionsEvictionStrategy(BROKER_LOCAL, config);

    long now = System.currentTimeMillis();
    Map<String, GossipState> peers = new HashMap<>();
    // Spread = 31-28 = 3 slots (9.4%). Inside eviction (10%), but
    // OUTSIDE the misconfigured-tighter consolidation (5%). The clamp must
    // raise it to 10% so consolidation fires.
    peers.put("broker-1",
        new GossipState(new GossipMessage("broker-1", 28, false, now), now));
    peers.put("broker-2",
        new GossipState(new GossipMessage("broker-2", 30, false, now), now));

    sm.recordProducerConnections("fat",
        slotMap("broker-1", 5, "broker-2", 3, "broker-3", 2, BROKER_LOCAL, 1));

    EvictionResult result = strategy.evaluate(sm, peers,
        sm.getProducerConnections(), allPeersServeTopic(peers));

    assertNotNull("clamp must permit consolidation at the eviction threshold",
        result);
  }

  // ============================================================
  // Configurable eviction step: budget, damping, multi-producer batch
  // ============================================================

  @Test
  public void testDefaultBudgetShedsSingleSlot() throws Exception {
    // Default config (evictionBudgetPercentage=0) must reproduce the legacy
    // one-slot-per-cycle behavior regardless of how many slots the producer
    // holds or how wide the gap.
    SlotManager sm = createSlotManager(32);
    acquireSlots(sm, "heavy", TOPIC, 320); // 32 slots -> local free 0
    sm.recordProducerConnections("heavy",
        new HashSet<>(Collections.singletonList("broker-2")));

    CurrConnectionsEvictionStrategy strategy =
        new CurrConnectionsEvictionStrategy(BROKER_LOCAL, evictionConfig);

    Map<String, GossipState> peers = peerWithFreeSlots("broker-2", 32);
    EvictionResult result = strategy.evaluate(sm, peers,
        sm.getProducerConnections(), allPeersServeTopic(peers));

    assertNotNull(result);
    assertEquals("heavy", result.getPid());
    assertEquals("default budget sheds exactly one slot", 1, result.getNumSlotsToEvict());
  }

  @Test
  public void testEvictionStepBoundedByBudget() throws Exception {
    // budget binds: held (32) and damped half-gap (16) both exceed the
    // 8-slot budget, so the move is capped at the budget.
    SlotManager sm = createSlotManager(32);
    acquireSlots(sm, "heavy", TOPIC, 320); // 32 slots -> local free 0
    sm.recordProducerConnections("heavy",
        new HashSet<>(Collections.singletonList("broker-2")));

    EvictionConfig config = new EvictionConfig();
    config.setEnabled(true);
    config.setEvictionPercentageThreshold(0.0);
    config.setPendingEvictionCooldownSeconds(0.0);
    config.setTopNTargets(1);
    config.setEvictionBudgetPercentage(25.0); // 25% of 32 = 8
    config.setEvictionDampingFactor(0.5);
    CurrConnectionsEvictionStrategy strategy =
        new CurrConnectionsEvictionStrategy(BROKER_LOCAL, config);

    Map<String, GossipState> peers = peerWithFreeSlots("broker-2", 32);
    EvictionResult result = strategy.evaluate(sm, peers,
        sm.getProducerConnections(), allPeersServeTopic(peers));

    assertNotNull(result);
    assertEquals("heavy", result.getPid());
    // min(budget=8, held=32, floor((32-0)*0.5)=16) = 8
    assertEquals(8, result.getNumSlotsToEvict());
  }

  @Test
  public void testEvictionStepBoundedByDampedHalfGap() throws Exception {
    // damping binds: budget (16) and held (32) both exceed the damped
    // half-gap, so the move is capped at floor((targetFree-localFree)*0.5).
    SlotManager sm = createSlotManager(32);
    acquireSlots(sm, "heavy", TOPIC, 320); // 32 slots -> local free 0
    sm.recordProducerConnections("heavy",
        new HashSet<>(Collections.singletonList("broker-2")));

    EvictionConfig config = new EvictionConfig();
    config.setEnabled(true);
    config.setEvictionPercentageThreshold(0.0);
    config.setPendingEvictionCooldownSeconds(0.0);
    config.setTopNTargets(1);
    config.setEvictionBudgetPercentage(50.0); // 16, won't bind
    config.setEvictionDampingFactor(0.5);
    CurrConnectionsEvictionStrategy strategy =
        new CurrConnectionsEvictionStrategy(BROKER_LOCAL, config);

    Map<String, GossipState> peers = peerWithFreeSlots("broker-2", 10);
    EvictionResult result = strategy.evaluate(sm, peers,
        sm.getProducerConnections(), allPeersServeTopic(peers));

    assertNotNull(result);
    // min(budget=16, held=32, floor((10-0)*0.5)=5) = 5
    assertEquals(5, result.getNumSlotsToEvict());
  }

  @Test
  public void testBudgetSpreadAcrossMultipleProducers() throws Exception {
    // No single light producer can cover the budget on its own, so the cycle
    // accumulates one slot from each of K=3 producers, each sent to a distinct
    // target (per-target cooldown applies within the cycle).
    SlotManager sm = createSlotManager(32);
    sm.recordWrite("filler", TOPIC, 280 * MB); // 28 slots, v3 (no connections)
    sm.recordWrite("p1", TOPIC, 10 * MB);       // 1 slot each
    sm.recordWrite("p2", TOPIC, 10 * MB);
    sm.recordWrite("p3", TOPIC, 10 * MB);
    tickOnce(sm); // occupied 31 -> local free 1
    sm.recordProducerConnections("p1",
        new HashSet<>(Collections.singletonList("broker-a")));
    sm.recordProducerConnections("p2",
        new HashSet<>(Collections.singletonList("broker-b")));
    sm.recordProducerConnections("p3",
        new HashSet<>(Collections.singletonList("broker-c")));

    EvictionConfig config = new EvictionConfig();
    config.setEnabled(true);
    config.setEvictionPercentageThreshold(0.0);
    config.setPendingEvictionCooldownSeconds(0.0);
    config.setTopNTargets(3);
    config.setTopNProducers(3);
    config.setEvictionBudgetPercentage(50.0); // 16, won't bind
    config.setMaxProducersPerCycle(3);
    config.setEvictionDampingFactor(0.5);
    CurrConnectionsEvictionStrategy strategy =
        new CurrConnectionsEvictionStrategy(BROKER_LOCAL, config);

    Map<String, GossipState> peers = new HashMap<>();
    peers.putAll(peerWithFreeSlots("broker-a", 30));
    peers.putAll(peerWithFreeSlots("broker-b", 30));
    peers.putAll(peerWithFreeSlots("broker-c", 30));

    java.util.List<EvictionResult> results = strategy.evaluateBatch(sm, peers,
        sm.getProducerConnections(), allPeersServeTopic(peers));

    assertEquals("batch should shed across K=3 producers", 3, results.size());
    Set<String> pids = new HashSet<>();
    Set<String> targets = new HashSet<>();
    for (EvictionResult r : results) {
      pids.add(r.getPid());
      targets.add(r.getTargetBrokerIp());
      assertEquals("each light producer sheds exactly one slot",
          1, r.getNumSlotsToEvict());
    }
    assertEquals("distinct producers across the cycle", 3, pids.size());
    assertEquals("distinct targets within the cycle (per-target cooldown)",
        3, targets.size());
  }

  @Test
  public void testMaxProducersPerCycleCapsBatch() throws Exception {
    // Same topology as above but K=1 -- the batch must stop after a single
    // producer even though budget and additional eligible producers remain.
    SlotManager sm = createSlotManager(32);
    sm.recordWrite("filler", TOPIC, 280 * MB);
    sm.recordWrite("p1", TOPIC, 10 * MB);
    sm.recordWrite("p2", TOPIC, 10 * MB);
    sm.recordWrite("p3", TOPIC, 10 * MB);
    tickOnce(sm);
    sm.recordProducerConnections("p1",
        new HashSet<>(Collections.singletonList("broker-a")));
    sm.recordProducerConnections("p2",
        new HashSet<>(Collections.singletonList("broker-b")));
    sm.recordProducerConnections("p3",
        new HashSet<>(Collections.singletonList("broker-c")));

    EvictionConfig config = new EvictionConfig();
    config.setEnabled(true);
    config.setEvictionPercentageThreshold(0.0);
    config.setPendingEvictionCooldownSeconds(0.0);
    config.setTopNTargets(3);
    config.setTopNProducers(3);
    config.setEvictionBudgetPercentage(50.0);
    config.setMaxProducersPerCycle(1);
    CurrConnectionsEvictionStrategy strategy =
        new CurrConnectionsEvictionStrategy(BROKER_LOCAL, config);

    Map<String, GossipState> peers = new HashMap<>();
    peers.putAll(peerWithFreeSlots("broker-a", 30));
    peers.putAll(peerWithFreeSlots("broker-b", 30));
    peers.putAll(peerWithFreeSlots("broker-c", 30));

    java.util.List<EvictionResult> results = strategy.evaluateBatch(sm, peers,
        sm.getProducerConnections(), allPeersServeTopic(peers));

    assertEquals("K=1 must cap the batch at a single producer", 1, results.size());
  }

}

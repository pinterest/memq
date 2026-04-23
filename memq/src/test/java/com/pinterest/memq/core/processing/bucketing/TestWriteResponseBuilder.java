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
package com.pinterest.memq.core.processing.bucketing;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.codahale.metrics.MetricRegistry;
import com.pinterest.memq.commons.protocol.RequestType;
import com.pinterest.memq.commons.protocol.ResponseCodes;
import com.pinterest.memq.commons.protocol.WriteResponsePacket;
import com.pinterest.memq.core.config.EvictionConfig;
import com.pinterest.memq.core.config.SlotAccountingConfig;
import com.pinterest.memq.core.eviction.EvictionManager;
import com.pinterest.memq.core.eviction.EvictionResult;
import com.pinterest.memq.core.eviction.EvictionStrategy;
import com.pinterest.memq.core.gossip.GossipState;
import com.pinterest.memq.core.slot.SlotManager;

import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Method;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Unit tests for {@link WriteResponseBuilder}.
 * <p>
 * Pinned regressions:
 * <ul>
 *   <li>v3 producers always get an empty packet (no v4 fields).</li>
 *   <li>v4 producers always get a populated packet (slot ownership, even when
 *       {@code disableAcks=true} — see {@link TestBucketingTopicProcessor}'s
 *       {@code testDisableAcksV4ResponseIncludesSlotInfo}, which guards the
 *       call site that historically returned {@code new WriteResponsePacket()}
 *       and broke v4 routing on the producer.</li>
 *   <li>Pending evictions are surfaced exactly once and consume slots.</li>
 * </ul>
 */
public class TestWriteResponseBuilder {

  private static final int MB = 1024 * 1024;
  private static final String TOPIC = "topicA";
  private static final String V4_PID = "11111111-2222-3333-4444-555555555555";

  private SlotAccountingConfig slotConfig;
  private EvictionConfig evictionConfig;

  @Before
  public void setUp() {
    slotConfig = new SlotAccountingConfig();
    slotConfig.setEnabled(true);
    slotConfig.setSlotSizeMbps(10.0);
    slotConfig.setSlotOverhead(0.0);
    slotConfig.setAcquireThresholdSeconds(0.0);
    slotConfig.setReleaseThresholdSeconds(0.0);
    slotConfig.setCooldownSeconds(0.0);
    slotConfig.setEmaWindowSeconds(0.001);
    slotConfig.setTickIntervalMs(1000);

    evictionConfig = new EvictionConfig();
    evictionConfig.setEnabled(true);
  }

  private SlotManager createSlotManager() {
    return new SlotManager(slotConfig, 100, new MetricRegistry());
  }

  private void acquireSlots(SlotManager sm, String pid, int mb) throws Exception {
    sm.recordWrite(pid, TOPIC, mb * MB);
    Method tick = SlotManager.class.getDeclaredMethod("tick");
    tick.setAccessible(true);
    tick.invoke(sm);
  }

  // -----------------------------------------------------------------------
  // Protocol gating
  // -----------------------------------------------------------------------

  @Test
  public void testV3ProducerAlwaysGetsEmptyPacket() throws Exception {
    SlotManager sm = createSlotManager();
    acquireSlots(sm, V4_PID, 50);

    WriteResponsePacket resp = WriteResponseBuilder.build(
        V4_PID, (short) 3, ResponseCodes.OK, null, sm);

    assertNotNull(resp);
    assertEquals("v3 must never receive a slot count", 0, resp.getNumSlotsOwned());
    assertFalse("v3 must never receive an eviction directive", resp.hasEviction());
    assertEquals(0, resp.getNumSlotsToEvict());
    // The broker still stamps its capability so the producer can know it
    // could speak v4 if it upgraded.
    assertEquals("broker must always stamp its protocol version",
        RequestType.PROTOCOL_VERSION, resp.getServerProtocolVersion());
  }

  @Test
  public void testEveryReturnedPacketStampsServerProtocolVersion() throws Exception {
    // The fix for "v4Active never flips when slots=0": the broker must
    // declare its capability on every response, regardless of whether
    // there is any v4 payload to deliver. Pin all four code paths.
    SlotManager sm = createSlotManager();

    // Path 1: v3 client
    WriteResponsePacket v3 = WriteResponseBuilder.build(
        V4_PID, (short) 3, ResponseCodes.OK, null, sm);
    assertEquals(RequestType.PROTOCOL_VERSION, v3.getServerProtocolVersion());

    // Path 2: non-OK
    WriteResponsePacket err = WriteResponseBuilder.build(
        V4_PID, (short) 4, ResponseCodes.INTERNAL_SERVER_ERROR, null, sm);
    assertEquals(RequestType.PROTOCOL_VERSION, err.getServerProtocolVersion());

    // Path 3: missing producerId
    WriteResponsePacket noPid = WriteResponseBuilder.build(
        null, (short) 4, ResponseCodes.OK, null, sm);
    assertEquals(RequestType.PROTOCOL_VERSION, noPid.getServerProtocolVersion());

    // Path 4: v4 happy path with zero slots — this is the regression case.
    // Without the explicit stamp, the producer cannot tell v3 from v4 here.
    WriteResponsePacket zeroSlots = WriteResponseBuilder.build(
        V4_PID, (short) 4, ResponseCodes.OK, null, sm);
    assertEquals("v4 broker must stamp version even when numSlotsOwned=0",
        RequestType.PROTOCOL_VERSION, zeroSlots.getServerProtocolVersion());
    assertEquals(0, zeroSlots.getNumSlotsOwned());
  }

  @Test
  public void testNonOkResponseGetsEmptyPacket() throws Exception {
    SlotManager sm = createSlotManager();
    acquireSlots(sm, V4_PID, 50);

    WriteResponsePacket resp = WriteResponseBuilder.build(
        V4_PID, (short) 4, ResponseCodes.INTERNAL_SERVER_ERROR, null, sm);

    assertEquals(0, resp.getNumSlotsOwned());
    assertFalse(resp.hasEviction());
  }

  @Test
  public void testNullProducerIdGetsEmptyPacket() throws Exception {
    SlotManager sm = createSlotManager();

    WriteResponsePacket resp = WriteResponseBuilder.build(
        null, (short) 4, ResponseCodes.OK, null, sm);

    assertEquals(0, resp.getNumSlotsOwned());
    assertFalse(resp.hasEviction());
  }

  @Test
  public void testEmptyProducerIdGetsEmptyPacket() throws Exception {
    SlotManager sm = createSlotManager();

    WriteResponsePacket resp = WriteResponseBuilder.build(
        "", (short) 4, ResponseCodes.OK, null, sm);

    assertEquals(0, resp.getNumSlotsOwned());
    assertFalse(resp.hasEviction());
  }

  // -----------------------------------------------------------------------
  // v4 happy path: slot ownership is reported
  // -----------------------------------------------------------------------

  @Test
  public void testV4ResponseCarriesSlotOwnership() throws Exception {
    SlotManager sm = createSlotManager();
    acquireSlots(sm, V4_PID, 50);
    int expected = sm.getTotalProducerSlots(V4_PID);
    assertTrue("test setup: producer must hold > 0 slots", expected > 0);

    WriteResponsePacket resp = WriteResponseBuilder.build(
        V4_PID, (short) 4, ResponseCodes.OK, null, sm);

    assertEquals("v4 producer must receive its current slot count",
        expected, resp.getNumSlotsOwned());
    assertFalse("no eviction pending → no eviction directive", resp.hasEviction());
  }

  @Test
  public void testV4ResponseWithUnknownProducerReportsZeroSlots() {
    SlotManager sm = createSlotManager();

    WriteResponsePacket resp = WriteResponseBuilder.build(
        "unknown-pid", (short) 4, ResponseCodes.OK, null, sm);

    assertEquals(0, resp.getNumSlotsOwned());
    assertFalse(resp.hasEviction());
  }

  @Test
  public void testV4ResponseSurvivesNullSlotManager() {
    WriteResponsePacket resp = WriteResponseBuilder.build(
        V4_PID, (short) 4, ResponseCodes.OK, null, null);

    assertNotNull(resp);
    assertEquals(0, resp.getNumSlotsOwned());
    assertFalse(resp.hasEviction());
  }

  // -----------------------------------------------------------------------
  // v4 eviction path: pending eviction is delivered exactly once
  // -----------------------------------------------------------------------

  @Test
  public void testV4ResponseDeliversPendingEvictionAndReleasesSlots() throws Exception {
    SlotManager sm = createSlotManager();
    acquireSlots(sm, V4_PID, 50);
    int initialSlots = sm.getTotalProducerSlots(V4_PID);
    assertTrue("test setup", initialSlots >= 2);

    EvictionManager em = managerWithPendingEviction(sm,
        new EvictionResult(V4_PID, "10.0.0.99", 1));

    WriteResponsePacket resp = WriteResponseBuilder.build(
        V4_PID, (short) 4, ResponseCodes.OK, em, sm);

    assertTrue("eviction must be delivered to v4 producer", resp.hasEviction());
    assertEquals("10.0.0.99", resp.getTargetBrokerIp());
    assertEquals(1, resp.getNumSlotsToEvict());
    assertEquals("remaining slots in response must reflect post-release count",
        initialSlots - 1, resp.getNumSlotsOwned());
    assertEquals("SlotManager must be mutated to drop the evicted slot",
        initialSlots - 1, sm.getTotalProducerSlots(V4_PID));
  }

  @Test
  public void testV4ResponseDeliversEvictionExactlyOnce() throws Exception {
    SlotManager sm = createSlotManager();
    acquireSlots(sm, V4_PID, 50);
    EvictionManager em = managerWithPendingEviction(sm,
        new EvictionResult(V4_PID, "10.0.0.99", 1));

    WriteResponsePacket first = WriteResponseBuilder.build(
        V4_PID, (short) 4, ResponseCodes.OK, em, sm);
    WriteResponsePacket second = WriteResponseBuilder.build(
        V4_PID, (short) 4, ResponseCodes.OK, em, sm);

    assertTrue("first response delivers the eviction", first.hasEviction());
    assertFalse("second response must NOT redeliver the eviction",
        second.hasEviction());
    assertEquals("second response still reports current slot count",
        sm.getTotalProducerSlots(V4_PID), second.getNumSlotsOwned());
  }

  @Test
  public void testV3ProducerNeverGetsEvictionEvenIfSomehowPending() throws Exception {
    SlotManager sm = createSlotManager();
    acquireSlots(sm, V4_PID, 50);
    EvictionManager em = managerWithPendingEviction(sm,
        new EvictionResult(V4_PID, "10.0.0.99", 1));

    WriteResponsePacket resp = WriteResponseBuilder.build(
        V4_PID, (short) 3, ResponseCodes.OK, em, sm);

    assertFalse("v3 protocol must never carry an eviction directive",
        resp.hasEviction());
    assertEquals(0, resp.getNumSlotsOwned());
    assertNotNull("v3 path must NOT consume the pending eviction; a later v4 "
        + "request from the same producer should still be able to claim it",
        em.peekEviction(V4_PID));
  }

  // -----------------------------------------------------------------------
  // Helpers
  // -----------------------------------------------------------------------

  /**
   * Build an {@link EvictionManager} pre-loaded with one pending eviction by
   * driving a stub strategy through {@link EvictionManager#runEviction()}.
   * <p>
   * Doing it via the public path (instead of poking {@code pendingEvictions}
   * with reflection) keeps this test resilient to internal refactors.
   */
  private EvictionManager managerWithPendingEviction(SlotManager sm,
                                                     EvictionResult result) {
    // Strategy must be one-shot: returns the result on first call (so runEviction
    // populates pendingEvictions exactly once), null thereafter.
    EvictionStrategy strategy = new EvictionStrategy() {
      private boolean fired = false;
      @Override
      public EvictionResult evaluate(SlotManager slotManager,
                                     Map<String, GossipState> peerStates,
                                     Map<String, Set<String>> producerConnections,
                                     Map<String, Set<String>> topicToBrokerIps) {
        if (fired) return null;
        fired = true;
        return result;
      }
    };
    // Also seed producerConnections so EvictionManager.runEviction's "peers" log
    // doesn't NPE on a missing producer registry. The strategy stub above
    // ignores the contents anyway.
    sm.recordProducerConnections(result.getPid(),
        new HashSet<>(Collections.singletonList(result.getTargetBrokerIp())));
    EvictionManager em = new EvictionManager(strategy, sm,
        Collections::<String, GossipState>emptyMap, evictionConfig);
    try {
      Method run = EvictionManager.class.getDeclaredMethod("runEviction");
      run.setAccessible(true);
      run.invoke(em);
    } catch (Exception e) {
      throw new RuntimeException("failed to drive runEviction via reflection", e);
    }
    assertNotNull("test setup: pending eviction must have been registered",
        em.peekEviction(result.getPid()));
    return em;
  }
}

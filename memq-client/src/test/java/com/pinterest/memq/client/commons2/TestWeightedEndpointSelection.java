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
package com.pinterest.memq.client.commons2;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.pinterest.memq.commons.protocol.WriteResponsePacket;

import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class TestWeightedEndpointSelection {

  private MemqCommonClient client;
  private List<Endpoint> endpoints;

  @Before
  public void setUp() throws Exception {
    Properties props = new Properties();
    props.setProperty("numWriteEndpoints", "3");
    client = new MemqCommonClient(null, props);

    endpoints = new ArrayList<>();
    endpoints.add(new Endpoint(InetSocketAddress.createUnresolved("10.0.0.1", 9092)));
    endpoints.add(new Endpoint(InetSocketAddress.createUnresolved("10.0.0.2", 9092)));
    endpoints.add(new Endpoint(InetSocketAddress.createUnresolved("10.0.0.3", 9092)));
    client.initialize(endpoints);
  }

  private void setWriteEndpoints(MemqCommonClient c, List<Endpoint> eps) throws Exception {
    Field writeEndpointsField = MemqCommonClient.class.getDeclaredField("writeEndpoints");
    writeEndpointsField.setAccessible(true);
    writeEndpointsField.set(c, eps);
  }

  /**
   * Build a WriteResponsePacket as a v4 broker would send it: stamped with
   * an explicit {@code serverProtocolVersion=4} so the producer's v4
   * detection flips to active. Tests that simulate v3 brokers should NOT
   * use this helper — they should use {@code new WriteResponsePacket(...)}
   * directly (which leaves serverProtocolVersion at the default 0).
   */
  private static WriteResponsePacket v4Response(String targetIp,
                                                int slotsToEvict,
                                                int slotsOwned) {
    WriteResponsePacket p = new WriteResponsePacket(targetIp, slotsToEvict, slotsOwned);
    p.setServerProtocolVersion((short) 4);
    return p;
  }

  @Test
  public void testFallbackToRoundRobinWithNoSlotData() {
    List<Endpoint> result = client.getEndpointsToTry();
    assertNotNull(result);
    assertFalse(result.isEmpty());
  }

  @Test
  public void testWeightedSelectionDistribution() throws Exception {
    Properties props = new Properties();
    props.setProperty("numWriteEndpoints", "3");
    MemqCommonClient weightedClient = new MemqCommonClient(null, props);

    List<Endpoint> eps = new ArrayList<>();
    eps.add(new Endpoint(InetSocketAddress.createUnresolved("10.0.0.1", 9092)));
    eps.add(new Endpoint(InetSocketAddress.createUnresolved("10.0.0.2", 9092)));
    eps.add(new Endpoint(InetSocketAddress.createUnresolved("10.0.0.3", 9092)));
    weightedClient.initialize(eps);
    setWriteEndpoints(weightedClient, eps);

    weightedClient.getSlotsOwned().put("10.0.0.1", 8);
    weightedClient.getSlotsOwned().put("10.0.0.2", 1);
    weightedClient.getSlotsOwned().put("10.0.0.3", 1);
    weightedClient.rebuildWeightedMap();

    Map<String, Integer> firstPickCounts = new HashMap<>();
    int trials = 1000;
    for (int i = 0; i < trials; i++) {
      List<Endpoint> result = weightedClient.getEndpointsToTry();
      String firstIp = result.get(0).getAddress().getHostString();
      firstPickCounts.merge(firstIp, 1, Integer::sum);
    }

    int ip1Count = firstPickCounts.getOrDefault("10.0.0.1", 0);
    assertTrue("Expected 10.0.0.1 to be picked first ~80% of the time, got " + ip1Count + "/" + trials,
        ip1Count > trials * 0.5);

    weightedClient.close();
  }

  @Test
  public void testEvictionResponseUpdatesSlotsOwned() {
    client.getSlotsOwned().put("10.0.0.1", 5);

    InetSocketAddress sourceAddr = InetSocketAddress.createUnresolved("10.0.0.1", 9092);
    WriteResponsePacket evictionResponse = new WriteResponsePacket("10.0.0.4", 2, 3);
    client.handleWriteResponse(evictionResponse, sourceAddr);

    assertEquals(2, (int) client.getSlotsOwned().getOrDefault("10.0.0.4", 0));
    assertEquals(3, (int) client.getSlotsOwned().getOrDefault("10.0.0.1", 0));
  }

  @Test
  public void testEvictionImmediatelyRegistersTargetAsWriteEndpoint() throws Exception {
    // Pre-eviction: producer is actively writing to 10.0.0.1 and 10.0.0.2
    setWriteEndpoints(client, new ArrayList<>(endpoints.subList(0, 2)));

    InetSocketAddress sourceAddr = InetSocketAddress.createUnresolved("10.0.0.1", 9092);
    // Target broker (10.0.0.99) is brand new — not in locality endpoints
    WriteResponsePacket evictionResponse = new WriteResponsePacket("10.0.0.99", 1, 4);
    client.handleWriteResponse(evictionResponse, sourceAddr);

    boolean targetInWrites = client.currentWriteEndpoints().stream()
        .anyMatch(e -> e.getAddress().getHostString().equals("10.0.0.99"));
    assertTrue("Target broker must be added to writeEndpoints immediately, got "
        + client.currentWriteEndpoints(), targetInWrites);
    // Synthesized endpoint should reuse the cluster port (9092 in this fixture)
    Endpoint targetEp = client.currentWriteEndpoints().stream()
        .filter(e -> e.getAddress().getHostString().equals("10.0.0.99"))
        .findFirst().orElseThrow(AssertionError::new);
    assertEquals(9092, targetEp.getAddress().getPort());
  }

  @Test
  public void testEvictionAddsTargetToLocalityEndpointsForFutureDiscovery() throws Exception {
    setWriteEndpoints(client, new ArrayList<>(endpoints.subList(0, 1)));

    InetSocketAddress sourceAddr = InetSocketAddress.createUnresolved("10.0.0.1", 9092);
    WriteResponsePacket evictionResponse = new WriteResponsePacket("10.0.0.99", 1, 0);
    client.handleWriteResponse(evictionResponse, sourceAddr);

    Field localsField = MemqCommonClient.class.getDeclaredField("localityEndpoints");
    localsField.setAccessible(true);
    @SuppressWarnings("unchecked")
    List<Endpoint> locals = (List<Endpoint>) localsField.get(client);
    assertTrue("Target must also be added to localityEndpoints",
        locals.stream().anyMatch(e -> e.getAddress().getHostString().equals("10.0.0.99")));
  }

  @Test
  public void testCapKeepsHeavySourceDropsLightestNonTarget() throws Exception {
    // maxConnections = 3, current [broker_0: 2, broker_1: 10, broker_2: 20].
    // broker_2 (the heavy source) evicts 1 slot to a brand-new target broker_4,
    // pushing the set to size 4. The cap must be honored by dropping the
    // *lightest non-target* connection (broker_0, 2 slots) -- NOT the source,
    // which still owns 19 slots -- and without fabricating a redistribution of
    // the dropped broker's slots onto the survivors.
    Properties props = new Properties();
    props.setProperty("numWriteEndpoints", "3");
    MemqCommonClient c = new MemqCommonClient(null, props);
    List<Endpoint> eps = new ArrayList<>();
    eps.add(new Endpoint(InetSocketAddress.createUnresolved("10.0.0.0", 9092)));
    eps.add(new Endpoint(InetSocketAddress.createUnresolved("10.0.0.1", 9092)));
    eps.add(new Endpoint(InetSocketAddress.createUnresolved("10.0.0.2", 9092)));
    c.initialize(eps);
    setWriteEndpoints(c, new ArrayList<>(eps));
    c.setMaxConnections(3);

    c.getSlotsOwned().put("10.0.0.0", 2);
    c.getSlotsOwned().put("10.0.0.1", 10);
    c.getSlotsOwned().put("10.0.0.2", 20);

    InetSocketAddress sourceAddr = InetSocketAddress.createUnresolved("10.0.0.2", 9092);
    WriteResponsePacket eviction = new WriteResponsePacket("10.0.0.4", 1, 19);
    c.handleWriteResponse(eviction, sourceAddr);

    Map<String, Integer> result = c.getSlotsOwned();
    assertEquals("Connection count must be capped at maxConnections", 3, result.size());
    assertFalse("Lightest non-target connection must be dropped",
        result.containsKey("10.0.0.0"));
    assertTrue("Heavy evicting source must be retained", result.containsKey("10.0.0.2"));
    assertTrue("Eviction target must be present", result.containsKey("10.0.0.4"));

    // No fabrication: survivors keep their real counts; the dropped broker's
    // slots are NOT sprayed onto them.
    assertEquals("Source keeps its remaining slots", 19, (int) result.get("10.0.0.2"));
    assertEquals("Survivor count must be unchanged (no fabrication)",
        10, (int) result.get("10.0.0.1"));
    assertEquals("Target carries only the evicted slot", 1, (int) result.get("10.0.0.4"));

    // writeEndpoints reflects the change: dropped broker out, target in.
    boolean targetInWrites = c.currentWriteEndpoints().stream()
        .anyMatch(e -> e.getAddress().getHostString().equals("10.0.0.4"));
    boolean droppedInWrites = c.currentWriteEndpoints().stream()
        .anyMatch(e -> e.getAddress().getHostString().equals("10.0.0.0"));
    assertTrue("Target must be a write endpoint", targetInWrites);
    assertFalse("Dropped broker must be removed from write endpoints", droppedInWrites);
    assertEquals(3, c.currentWriteEndpoints().size());

    c.close();
  }

  @Test
  public void testCapNeverDropsEvictionTarget() throws Exception {
    // Even when the eviction target is the globally lightest entry (it starts at
    // just the evicted slot count), the cap must never drop the target itself --
    // it drops the lightest *other* connection instead.
    Properties props = new Properties();
    props.setProperty("numWriteEndpoints", "3");
    MemqCommonClient c = new MemqCommonClient(null, props);
    List<Endpoint> eps = new ArrayList<>();
    eps.add(new Endpoint(InetSocketAddress.createUnresolved("10.0.0.0", 9092)));
    eps.add(new Endpoint(InetSocketAddress.createUnresolved("10.0.0.1", 9092)));
    eps.add(new Endpoint(InetSocketAddress.createUnresolved("10.0.0.2", 9092)));
    c.initialize(eps);
    setWriteEndpoints(c, new ArrayList<>(eps));
    c.setMaxConnections(3);

    c.getSlotsOwned().put("10.0.0.0", 5);
    c.getSlotsOwned().put("10.0.0.1", 6);
    c.getSlotsOwned().put("10.0.0.2", 7);

    InetSocketAddress sourceAddr = InetSocketAddress.createUnresolved("10.0.0.2", 9092);
    WriteResponsePacket eviction = new WriteResponsePacket("10.0.0.9", 1, 6);
    c.handleWriteResponse(eviction, sourceAddr);

    Map<String, Integer> result = c.getSlotsOwned();
    assertEquals(3, result.size());
    assertTrue("Eviction target must never be dropped", result.containsKey("10.0.0.9"));
    assertEquals("Target carries only the evicted slot", 1, (int) result.get("10.0.0.9"));
    assertFalse("Lightest non-target must be dropped", result.containsKey("10.0.0.0"));
    c.close();
  }

  @Test
  public void testEvictionWithoutMaxConnectionsKeepsSourceWhenRemainingPositive()
      throws Exception {
    Properties props = new Properties();
    props.setProperty("numWriteEndpoints", "3");
    // Explicitly opt out of the cap (default is 3); we want unbounded growth here.
    props.setProperty(MemqCommonClient.CONFIG_MAX_CONNECTIONS, "0");
    MemqCommonClient c = new MemqCommonClient(null, props);
    List<Endpoint> eps = new ArrayList<>();
    eps.add(new Endpoint(InetSocketAddress.createUnresolved("10.0.0.0", 9092)));
    eps.add(new Endpoint(InetSocketAddress.createUnresolved("10.0.0.1", 9092)));
    eps.add(new Endpoint(InetSocketAddress.createUnresolved("10.0.0.2", 9092)));
    c.initialize(eps);
    setWriteEndpoints(c, new ArrayList<>(eps));
    // maxConnections == 0 means unbounded — no swap should happen.

    c.getSlotsOwned().put("10.0.0.0", 10);
    c.getSlotsOwned().put("10.0.0.1", 10);
    c.getSlotsOwned().put("10.0.0.2", 10);

    InetSocketAddress sourceAddr = InetSocketAddress.createUnresolved("10.0.0.2", 9092);
    WriteResponsePacket eviction = new WriteResponsePacket("10.0.0.4", 1, 9);
    c.handleWriteResponse(eviction, sourceAddr);

    Map<String, Integer> result = c.getSlotsOwned();
    assertEquals("Without maxConnections cap, set may grow", 4, result.size());
    assertEquals(9, (int) result.get("10.0.0.2"));
    assertEquals(1, (int) result.get("10.0.0.4"));
    c.close();
  }

  @Test
  public void testNonEvictionResponseUpdatesSlotsForKnownSource() throws Exception {
    setWriteEndpoints(client, new ArrayList<>(endpoints));

    InetSocketAddress sourceAddr = InetSocketAddress.createUnresolved("10.0.0.1", 9092);
    WriteResponsePacket normalResponse = new WriteResponsePacket(null, 0, 7);
    client.handleWriteResponse(normalResponse, sourceAddr);

    assertEquals("Non-eviction response must refresh slot count for known sources",
        7, (int) client.getSlotsOwned().getOrDefault("10.0.0.1", 0));
  }

  @Test
  public void testNonEvictionResponseIgnoresUnknownSource() {
    InetSocketAddress sourceAddr = InetSocketAddress.createUnresolved("10.99.99.99", 9092);
    WriteResponsePacket normalResponse = new WriteResponsePacket(null, 0, 5);
    client.handleWriteResponse(normalResponse, sourceAddr);

    assertFalse("Unknown sources must not be added to slotsOwned by non-eviction responses",
        client.getSlotsOwned().containsKey("10.99.99.99"));
  }

  @Test
  public void testNonEvictionResponseDoesNotCrash() {
    WriteResponsePacket normalResponse = new WriteResponsePacket(null, 0, 5);
    client.handleWriteResponse(normalResponse, null);
  }

  @Test
  public void testHandleNullWriteResponse() {
    client.handleWriteResponse(null, null);
  }

  @Test
  public void testGetCurrentConnectionsList() {
    assertTrue(client.getCurrentConnectionsList().isEmpty());

    client.getSlotsOwned().put("10.0.0.1", 3);
    client.getSlotsOwned().put("10.0.0.2", 5);

    List<String> connections = client.getCurrentConnectionsList();
    assertEquals(2, connections.size());
    assertTrue(connections.contains("10.0.0.1"));
    assertTrue(connections.contains("10.0.0.2"));
  }

  @Test
  public void testV4ActivatesWhenServerDeclaresV4() throws Exception {
    // The producer flips v4Active EXPLICITLY off the broker's declared
    // protocol version, never inferring from slot counts.
    setWriteEndpoints(client, new ArrayList<>(endpoints.subList(0, 1)));
    assertFalse("v4Active must start false", client.isV4Active());

    InetSocketAddress sourceAddr = InetSocketAddress.createUnresolved("10.0.0.1", 9092);
    // numSlotsOwned=0, no eviction — but the v4 broker stamps its version.
    client.handleWriteResponse(v4Response(null, 0, 0), sourceAddr);

    assertTrue("Explicit serverProtocolVersion>=4 must flip v4Active even"
        + " when slot count is 0 (regression test: pre-fix this needed"
        + " numSlotsOwned>0, which was just a heuristic)",
        client.isV4Active());
  }

  @Test
  public void testV4ActivatesOnFirstEviction() throws Exception {
    setWriteEndpoints(client, new ArrayList<>(endpoints.subList(0, 1)));
    assertFalse("v4Active must start false", client.isV4Active());

    InetSocketAddress sourceAddr = InetSocketAddress.createUnresolved("10.0.0.1", 9092);
    client.handleWriteResponse(v4Response("10.0.0.99", 1, 4), sourceAddr);

    assertTrue("Eviction from v4-stamped broker must flip v4Active", client.isV4Active());
  }

  @Test
  public void testV4StaysInactiveWhenSlotsArrivedButServerVersionMissing()
      throws Exception {
    // Even a non-zero numSlotsOwned must NOT flip v4Active — the activation
    // signal is exclusively the broker's declared serverProtocolVersion.
    setWriteEndpoints(client, new ArrayList<>(endpoints.subList(0, 1)));
    assertFalse(client.isV4Active());

    InetSocketAddress sourceAddr = InetSocketAddress.createUnresolved("10.0.0.1", 9092);
    // Slot fields populated, but serverProtocolVersion left at 0.
    client.handleWriteResponse(new WriteResponsePacket(null, 0, 3), sourceAddr);

    assertFalse("numSlotsOwned alone must NOT flip v4Active — that was the"
        + " old heuristic; activation is now driven exclusively by the"
        + " broker's declared serverProtocolVersion",
        client.isV4Active());
  }

  @Test
  public void testV4StaysInactiveForV3Brokers() throws Exception {
    // v3 brokers send a default WriteResponsePacket (serverProtocolVersion=0).
    setWriteEndpoints(client, new ArrayList<>(endpoints));
    InetSocketAddress sourceAddr = InetSocketAddress.createUnresolved("10.0.0.1", 9092);

    for (int i = 0; i < 20; i++) {
      client.handleWriteResponse(new WriteResponsePacket(), sourceAddr);
    }

    assertFalse("v4Active must remain false against v3 brokers",
        client.isV4Active());
  }

  @Test
  public void testWeightedActivatesImmediatelyOnFirstSlotData() throws Exception {
    // numWriteEndpoints=3 (configured), only 1 endpoint actively writing.
    // Pre-fix this would block weighted activation until writes.size() >= 3.
    // Post-fix, weighted activates as soon as we have any slot data from a
    // v4-stamped broker.
    setWriteEndpoints(client, new ArrayList<>(endpoints.subList(0, 1)));

    InetSocketAddress sourceAddr = InetSocketAddress.createUnresolved("10.0.0.1", 9092);
    client.handleWriteResponse(v4Response(null, 0, 5), sourceAddr);

    boolean usedWeighted = false;
    for (int i = 0; i < 20; i++) {
      List<Endpoint> picks = client.getEndpointsToTry();
      // The weighted path always puts the chosen endpoint first; with only one
      // slot-data entry (10.0.0.1) it must always be picked first.
      if (picks.get(0).getAddress().getHostString().equals("10.0.0.1")) {
        usedWeighted = true;
      }
    }
    assertTrue("Weighted selection must activate without waiting for numWriteEndpoints",
        usedWeighted);
  }

  @Test
  public void testV4ActiveDisablesNumWriteEndpointsCapInMaybeRegister() throws Exception {
    // numWriteEndpoints=3, but with v4Active flipped on, maybeRegisterWriteEndpoint
    // should NOT keep growing writeEndpoints — eviction logic owns the set now.
    setWriteEndpoints(client, new ArrayList<>(endpoints.subList(0, 1)));
    InetSocketAddress sourceAddr = InetSocketAddress.createUnresolved("10.0.0.1", 9092);
    client.handleWriteResponse(v4Response(null, 0, 5), sourceAddr);
    assertTrue(client.isV4Active());

    int sizeBefore = client.currentWriteEndpoints().size();
    // Simulate a successful send to a brand-new locality endpoint (10.0.0.2).
    Endpoint freshLocality = endpoints.get(1);
    client.maybeRegisterWriteEndpoint(freshLocality, "topic");

    assertEquals("v4Active must prevent legacy growth via maybeRegisterWriteEndpoint",
        sizeBefore, client.currentWriteEndpoints().size());
  }

  @Test
  public void testLegacyRegisterStillGrowsWhenV4Inactive() throws Exception {
    // Pure v3 path: maybeRegisterWriteEndpoint should keep adding up to numWriteEndpoints.
    setWriteEndpoints(client, new ArrayList<>());
    assertFalse(client.isV4Active());

    client.maybeRegisterWriteEndpoint(endpoints.get(0), "topic");
    client.maybeRegisterWriteEndpoint(endpoints.get(1), "topic");
    client.maybeRegisterWriteEndpoint(endpoints.get(2), "topic");
    // numWriteEndpoints=3 here (set in setUp), so we should have exactly 3.
    assertEquals(3, client.currentWriteEndpoints().size());
  }

  @Test
  public void testWeightedFallsBackToRoundRobinWhenNoSlotData() throws Exception {
    setWriteEndpoints(client, endpoints);
    List<Endpoint> result1 = client.getEndpointsToTry();
    List<Endpoint> result2 = client.getEndpointsToTry();

    assertNotNull(result1);
    assertEquals(3, result1.size());
    assertNotNull(result2);
    assertEquals(3, result2.size());
    // should rotate (round-robin), not weighted
    assertFalse(result1.get(0).equals(result2.get(0)) && result1.get(1).equals(result2.get(1)));
  }

  @Test
  public void testMaxConnectionsDefaultMatchesConstant() throws Exception {
    MemqCommonClient c = new MemqCommonClient(null, new Properties());
    Field f = MemqCommonClient.class.getDeclaredField("maxConnections");
    f.setAccessible(true);
    assertEquals("Default maxConnections must match DEFAULT_MAX_CONNECTIONS",
        MemqCommonClient.DEFAULT_MAX_CONNECTIONS, ((Integer) f.get(c)).intValue());
    c.close();
  }

  @Test
  public void testMaxConnectionsConfigurableViaProperties() throws Exception {
    Properties props = new Properties();
    props.setProperty(MemqCommonClient.CONFIG_MAX_CONNECTIONS, "7");
    MemqCommonClient c = new MemqCommonClient(null, props);
    Field f = MemqCommonClient.class.getDeclaredField("maxConnections");
    f.setAccessible(true);
    assertEquals(7, ((Integer) f.get(c)).intValue());
    c.close();
  }

  @Test
  public void testMaxConnectionsZeroDisablesCap() throws Exception {
    Properties props = new Properties();
    props.setProperty(MemqCommonClient.CONFIG_MAX_CONNECTIONS, "0");
    MemqCommonClient c = new MemqCommonClient(null, props);
    Field f = MemqCommonClient.class.getDeclaredField("maxConnections");
    f.setAccessible(true);
    assertEquals("0 must be honored verbatim (means unbounded)", 0, ((Integer) f.get(c)).intValue());
    c.close();
  }

  @Test
  public void testRoutingBoostActiveAfterEvictionToTarget() throws Exception {
    // After the producer receives an eviction directive routing one slot to
    // a new target, the target must get a +1 routing weight on top of its
    // (now optimistically incremented) slotsOwned count, so the producer
    // over-routes there long enough for the target broker to actually
    // acquire the slot.
    setWriteEndpoints(client, new ArrayList<>(endpoints));
    client.getSlotsOwned().put("10.0.0.1", 11);
    client.getSlotsOwned().put("10.0.0.2", 1);
    client.getSlotsOwned().put("10.0.0.3", 1);

    long before = System.currentTimeMillis();
    InetSocketAddress sourceAddr = InetSocketAddress.createUnresolved("10.0.0.1", 9092);
    client.handleWriteResponse(new WriteResponsePacket("10.0.0.2", 1, 10), sourceAddr);

    // Target's effective routing weight must include the +1 boost.
    assertEquals("target gets slotsOwned (2 after merge) + 1 boost",
        3, client.effectiveWeight("10.0.0.2", before + 1));
    // Other brokers must not be affected.
    assertEquals(10, client.effectiveWeight("10.0.0.1", before + 1));
    assertEquals(1, client.effectiveWeight("10.0.0.3", before + 1));
  }

  @Test
  public void testRoutingBoostExpiresAfterTtl() throws Exception {
    // Re-create the client with a tiny TTL so the boost can expire in test time.
    Properties props = new Properties();
    props.setProperty("numWriteEndpoints", "3");
    props.setProperty(MemqCommonClient.CONFIG_POST_EVICTION_ROUTING_BOOST_MS, "50");
    MemqCommonClient c = new MemqCommonClient(null, props);
    List<Endpoint> eps = new ArrayList<>();
    eps.add(new Endpoint(InetSocketAddress.createUnresolved("10.0.0.1", 9092)));
    eps.add(new Endpoint(InetSocketAddress.createUnresolved("10.0.0.2", 9092)));
    c.initialize(eps);
    setWriteEndpoints(c, new ArrayList<>(eps));
    c.getSlotsOwned().put("10.0.0.1", 5);
    c.getSlotsOwned().put("10.0.0.2", 1);

    InetSocketAddress sourceAddr = InetSocketAddress.createUnresolved("10.0.0.1", 9092);
    c.handleWriteResponse(new WriteResponsePacket("10.0.0.2", 1, 4), sourceAddr);

    long withinTtl = System.currentTimeMillis();
    assertEquals("inside TTL: boost active", 3, c.effectiveWeight("10.0.0.2", withinTtl));

    Thread.sleep(80);
    long pastTtl = System.currentTimeMillis();
    assertEquals("after TTL: boost gone, only slotsOwned counts",
        2, c.effectiveWeight("10.0.0.2", pastTtl));

    c.close();
  }

  @Test
  public void testRoutingBoostClearedWhenBrokerBecomesEvictionSource() throws Exception {
    // Sequence:
    //   1) Eviction A -> B: B is boosted.
    //   2) Eviction B -> C: B just gave a slot away, its boost must be
    //      cleared, otherwise we'd over-route to a broker we just got
    //      told to drain. C gets the new boost.
    setWriteEndpoints(client, new ArrayList<>(endpoints));
    client.getSlotsOwned().put("10.0.0.1", 5);
    client.getSlotsOwned().put("10.0.0.2", 1);
    client.getSlotsOwned().put("10.0.0.3", 1);

    InetSocketAddress fromA = InetSocketAddress.createUnresolved("10.0.0.1", 9092);
    client.handleWriteResponse(new WriteResponsePacket("10.0.0.2", 1, 4), fromA);
    long now1 = System.currentTimeMillis();
    assertEquals("B is boosted after first eviction",
        3, client.effectiveWeight("10.0.0.2", now1));

    InetSocketAddress fromB = InetSocketAddress.createUnresolved("10.0.0.2", 9092);
    client.handleWriteResponse(new WriteResponsePacket("10.0.0.3", 1, 1), fromB);
    long now2 = System.currentTimeMillis();
    assertEquals("B's boost must be cleared once it acts as eviction source",
        1, client.effectiveWeight("10.0.0.2", now2));
    assertEquals("C gets the new boost (slotsOwned 2 + 1)",
        3, client.effectiveWeight("10.0.0.3", now2));
  }

  @Test
  public void testRoutingBoostNotAppliedWithoutEviction() throws Exception {
    // Plain non-eviction responses must not arm the boost on anyone.
    setWriteEndpoints(client, new ArrayList<>(endpoints));
    client.getSlotsOwned().put("10.0.0.1", 5);

    InetSocketAddress sourceAddr = InetSocketAddress.createUnresolved("10.0.0.1", 9092);
    client.handleWriteResponse(new WriteResponsePacket(null, 0, 7), sourceAddr);

    long now = System.currentTimeMillis();
    assertEquals("no eviction -> no boost",
        7, client.effectiveWeight("10.0.0.1", now));
  }

  @Test
  public void testRoutingBoostDisabledWhenTtlIsZero() throws Exception {
    Properties props = new Properties();
    props.setProperty("numWriteEndpoints", "3");
    props.setProperty(MemqCommonClient.CONFIG_POST_EVICTION_ROUTING_BOOST_MS, "0");
    MemqCommonClient c = new MemqCommonClient(null, props);
    List<Endpoint> eps = new ArrayList<>();
    eps.add(new Endpoint(InetSocketAddress.createUnresolved("10.0.0.1", 9092)));
    eps.add(new Endpoint(InetSocketAddress.createUnresolved("10.0.0.2", 9092)));
    c.initialize(eps);
    setWriteEndpoints(c, new ArrayList<>(eps));
    c.getSlotsOwned().put("10.0.0.1", 5);
    c.getSlotsOwned().put("10.0.0.2", 1);

    InetSocketAddress sourceAddr = InetSocketAddress.createUnresolved("10.0.0.1", 9092);
    c.handleWriteResponse(new WriteResponsePacket("10.0.0.2", 1, 4), sourceAddr);

    long now = System.currentTimeMillis();
    assertEquals("ttl=0 disables boost; weight is plain slotsOwned",
        2, c.effectiveWeight("10.0.0.2", now));
    c.close();
  }

  @Test
  public void testRoutingBoostMsConfigurableViaProperties() throws Exception {
    Properties props = new Properties();
    props.setProperty(MemqCommonClient.CONFIG_POST_EVICTION_ROUTING_BOOST_MS, "12345");
    MemqCommonClient c = new MemqCommonClient(null, props);
    Field f = MemqCommonClient.class.getDeclaredField("postEvictionRoutingBoostMs");
    f.setAccessible(true);
    assertEquals(12345L, ((Long) f.get(c)).longValue());
    c.close();
  }

  @Test
  public void testExplicitMaxConnectionsCapsConnectionGrowth() throws Exception {
    Properties props = new Properties();
    props.setProperty("numWriteEndpoints", "3");
    props.setProperty(MemqCommonClient.CONFIG_MAX_CONNECTIONS, "3");
    MemqCommonClient c = new MemqCommonClient(null, props);
    List<Endpoint> eps = new ArrayList<>();
    eps.add(new Endpoint(InetSocketAddress.createUnresolved("10.0.0.0", 9092)));
    eps.add(new Endpoint(InetSocketAddress.createUnresolved("10.0.0.1", 9092)));
    eps.add(new Endpoint(InetSocketAddress.createUnresolved("10.0.0.2", 9092)));
    c.initialize(eps);
    setWriteEndpoints(c, new ArrayList<>(eps));

    c.getSlotsOwned().put("10.0.0.0", 10);
    c.getSlotsOwned().put("10.0.0.1", 10);
    c.getSlotsOwned().put("10.0.0.2", 10);

    InetSocketAddress sourceAddr = InetSocketAddress.createUnresolved("10.0.0.2", 9092);
    WriteResponsePacket eviction = new WriteResponsePacket("10.0.0.4", 1, 9);
    c.handleWriteResponse(eviction, sourceAddr);

    Map<String, Integer> result = c.getSlotsOwned();
    assertEquals("Cap of 3 must trigger swap when adding 4th broker", 3, result.size());
    assertFalse("Evicting source must be dropped on cap-violating eviction",
        result.containsKey("10.0.0.2"));
    assertTrue("New target must be present after swap",
        result.containsKey("10.0.0.4"));
    c.close();
  }

  // ---- removeBroker (surgical REDIRECT path) ----

  @SuppressWarnings("unchecked")
  private static java.util.Map<String, Long> getRoutingBoostUntilMs(MemqCommonClient c)
      throws Exception {
    Field f = MemqCommonClient.class.getDeclaredField("routingBoostUntilMs");
    f.setAccessible(true);
    return (java.util.Map<String, Long>) f.get(c);
  }

  @SuppressWarnings("unchecked")
  private static java.util.Map<Endpoint, Integer> getFailureCounts(MemqCommonClient c)
      throws Exception {
    Field f = MemqCommonClient.class.getDeclaredField("failureCounts");
    f.setAccessible(true);
    return (java.util.Map<Endpoint, Integer>) f.get(c);
  }

  @SuppressWarnings("unchecked")
  private static List<Endpoint> getLocalityEndpoints(MemqCommonClient c) throws Exception {
    Field f = MemqCommonClient.class.getDeclaredField("localityEndpoints");
    f.setAccessible(true);
    return (List<Endpoint>) f.get(c);
  }

  private static java.util.TreeMap<Double, Endpoint> getWeightedEndpointMap(MemqCommonClient c)
      throws Exception {
    Field f = MemqCommonClient.class.getDeclaredField("weightedEndpointMap");
    f.setAccessible(true);
    @SuppressWarnings("unchecked")
    java.util.TreeMap<Double, Endpoint> m = (java.util.TreeMap<Double, Endpoint>) f.get(c);
    return m;
  }

  @Test
  public void testRemoveBrokerPurgesAllPerBrokerState() throws Exception {
    // Pre-populate every per-broker structure for X, then surgically remove
    // X and assert nothing about X is left behind. Survivors B and C must be
    // untouched (state, weights, endpoint memberships all preserved).
    setWriteEndpoints(client, new ArrayList<>(endpoints));

    client.getSlotsOwned().put("10.0.0.1", 5);
    client.getSlotsOwned().put("10.0.0.2", 3);
    client.getSlotsOwned().put("10.0.0.3", 2);

    // Arm a routing boost on X (10.0.0.1) to confirm it gets cleared.
    getRoutingBoostUntilMs(client).put("10.0.0.1",
        System.currentTimeMillis() + 60_000L);
    // Plant a failure-count entry for X to confirm it is also dropped.
    Endpoint xEp = new Endpoint(InetSocketAddress.createUnresolved("10.0.0.1", 9092));
    getFailureCounts(client).put(xEp, 1);

    client.rebuildWeightedMap();
    assertNotNull("weighted map should be active before removal",
        getWeightedEndpointMap(client));

    InetSocketAddress xAddr = InetSocketAddress.createUnresolved("10.0.0.1", 9092);
    client.removeBroker(xAddr);

    // Per-broker maps cleared for X.
    assertFalse("slotsOwned must drop X",
        client.getSlotsOwned().containsKey("10.0.0.1"));
    assertFalse("routingBoostUntilMs must drop X",
        getRoutingBoostUntilMs(client).containsKey("10.0.0.1"));
    assertTrue("failureCounts must drop every entry for X",
        getFailureCounts(client).keySet().stream()
            .noneMatch(e -> e.getAddress().getHostString().equals("10.0.0.1")));

    // Endpoint lists no longer contain X.
    assertTrue("writeEndpoints must drop X",
        client.currentWriteEndpoints().stream()
            .noneMatch(e -> e.getAddress().getHostString().equals("10.0.0.1")));
    assertTrue("localityEndpoints must drop X",
        getLocalityEndpoints(client).stream()
            .noneMatch(e -> e.getAddress().getHostString().equals("10.0.0.1")));

    // Survivors retained, weights preserved.
    assertEquals("B's slot count must be untouched",
        3, (int) client.getSlotsOwned().get("10.0.0.2"));
    assertEquals("C's slot count must be untouched",
        2, (int) client.getSlotsOwned().get("10.0.0.3"));

    // Weighted map renormalized over the survivors only.
    java.util.TreeMap<Double, Endpoint> wm = getWeightedEndpointMap(client);
    assertNotNull("weighted map must be rebuilt over survivors", wm);
    assertEquals("weighted map must contain only the two survivors",
        2, wm.size());
    assertTrue("weighted map keys should sum to 1.0 (last entry == 1.0)",
        wm.lastKey() > 0.999 && wm.lastKey() < 1.001);
    assertTrue("weighted map must not reference X",
        wm.values().stream()
            .noneMatch(e -> e.getAddress().getHostString().equals("10.0.0.1")));
  }

  @Test
  public void testRemoveBrokerThrowsWhenLocalityEmptied() throws Exception {
    // The producer's last surviving endpoint goes away — removeBroker must
    // throw WITHOUT mutating any state, so the caller (Request.java) can
    // still reach the dying broker for the fallback reconnect's metadata
    // call. If we mutated then threw, reconnect would short-circuit with
    // "Client not initialized yet" because localityEndpoints would be empty.
    Properties props = new Properties();
    MemqCommonClient solo = new MemqCommonClient(null, props);
    List<Endpoint> single = new ArrayList<>();
    single.add(new Endpoint(InetSocketAddress.createUnresolved("10.0.0.1", 9092)));
    solo.initialize(single);
    setWriteEndpoints(solo, new ArrayList<>(single));
    solo.getSlotsOwned().put("10.0.0.1", 5);

    InetSocketAddress only = InetSocketAddress.createUnresolved("10.0.0.1", 9092);
    try {
      solo.removeBroker(only);
      fail("expected removeBroker to throw when removal would empty locality");
    } catch (Exception e) {
      assertTrue("expected message about no endpoints; got: " + e.getMessage(),
          e.getMessage().contains("No endpoints available"));
    }

    // State must be intact so the fallback reconnect can still use it.
    assertEquals("writeEndpoints must NOT be mutated when removal would empty locality",
        1, solo.currentWriteEndpoints().size());
    assertEquals("locality endpoints must NOT be mutated",
        1, getLocalityEndpoints(solo).size());
    assertTrue("slotsOwned must NOT be mutated",
        solo.getSlotsOwned().containsKey("10.0.0.1"));
    solo.close();
  }

  @Test
  public void testRemoveBrokerNullAddressIsNoop() throws Exception {
    setWriteEndpoints(client, new ArrayList<>(endpoints));
    client.getSlotsOwned().put("10.0.0.1", 5);
    int writesBefore = client.currentWriteEndpoints().size();
    int slotsBefore = client.getSlotsOwned().size();

    client.removeBroker(null);

    assertEquals(writesBefore, client.currentWriteEndpoints().size());
    assertEquals(slotsBefore, client.getSlotsOwned().size());
  }

  @Test
  public void testRemoveBrokerIdempotent() throws Exception {
    // Calling removeBroker twice for the same address (e.g. multiple
    // in-flight REDIRECTs from the same broker) must be safe.
    setWriteEndpoints(client, new ArrayList<>(endpoints));
    client.getSlotsOwned().put("10.0.0.1", 5);
    client.getSlotsOwned().put("10.0.0.2", 3);
    client.getSlotsOwned().put("10.0.0.3", 2);

    InetSocketAddress xAddr = InetSocketAddress.createUnresolved("10.0.0.1", 9092);
    client.removeBroker(xAddr);
    int writesAfterFirst = client.currentWriteEndpoints().size();
    int slotsAfterFirst = client.getSlotsOwned().size();

    client.removeBroker(xAddr);
    assertEquals("second removal must not further mutate writeEndpoints",
        writesAfterFirst, client.currentWriteEndpoints().size());
    assertEquals("second removal must not further mutate slotsOwned",
        slotsAfterFirst, client.getSlotsOwned().size());
  }

  @Test
  public void testRemoveBrokerDoesNotAffectOtherBrokerWeights() throws Exception {
    // Survivor weights must continue to drive routing in the same proportions
    // they had before X was removed (modulo the larger denominator).
    setWriteEndpoints(client, new ArrayList<>(endpoints));
    client.getSlotsOwned().put("10.0.0.1", 6);
    client.getSlotsOwned().put("10.0.0.2", 3);
    client.getSlotsOwned().put("10.0.0.3", 1);
    client.rebuildWeightedMap();

    InetSocketAddress xAddr = InetSocketAddress.createUnresolved("10.0.0.1", 9092);
    client.removeBroker(xAddr);

    java.util.TreeMap<Double, Endpoint> wm = getWeightedEndpointMap(client);
    assertNotNull(wm);
    assertEquals(2, wm.size());

    long now = System.currentTimeMillis();
    assertEquals("B retains its raw weight of 3", 3, client.effectiveWeight("10.0.0.2", now));
    assertEquals("C retains its raw weight of 1", 1, client.effectiveWeight("10.0.0.3", now));
  }
}

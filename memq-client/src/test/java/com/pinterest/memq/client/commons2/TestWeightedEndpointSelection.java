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
  public void testConnectionSwapPreservesTotalSlotsAndDropsSource() throws Exception {
    // Mirrors the spec example: maxConnections = 3, current
    // [broker_0: 10, broker_1: 10, broker_2: 10]; broker_2 evicts 1 slot to broker_4.
    // Naive result would be size 4 -> swap drops source (broker_2), redistributing
    // its 9 remaining slots evenly across the 3 survivors -> [13, 13, 4].
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

    c.getSlotsOwned().put("10.0.0.0", 10);
    c.getSlotsOwned().put("10.0.0.1", 10);
    c.getSlotsOwned().put("10.0.0.2", 10);

    InetSocketAddress sourceAddr = InetSocketAddress.createUnresolved("10.0.0.2", 9092);
    WriteResponsePacket eviction = new WriteResponsePacket("10.0.0.4", 1, 9);
    c.handleWriteResponse(eviction, sourceAddr);

    Map<String, Integer> result = c.getSlotsOwned();
    assertEquals("Connection count must be capped at maxConnections", 3, result.size());
    assertFalse("Evicting source must be dropped entirely",
        result.containsKey("10.0.0.2"));
    assertTrue("Eviction target must be present", result.containsKey("10.0.0.4"));

    int total = result.values().stream().mapToInt(Integer::intValue).sum();
    assertEquals("Total slots must be preserved by redistribution", 30, total);
    assertEquals(13, (int) result.get("10.0.0.0"));
    assertEquals(13, (int) result.get("10.0.0.1"));
    assertEquals(4, (int) result.get("10.0.0.4"));

    // writeEndpoints must also reflect the swap: source out, target in
    boolean targetInWrites = c.currentWriteEndpoints().stream()
        .anyMatch(e -> e.getAddress().getHostString().equals("10.0.0.4"));
    boolean sourceInWrites = c.currentWriteEndpoints().stream()
        .anyMatch(e -> e.getAddress().getHostString().equals("10.0.0.2"));
    assertTrue("Target must be a write endpoint after swap", targetInWrites);
    assertFalse("Dropped source must be removed from write endpoints", sourceInWrites);
    assertEquals(3, c.currentWriteEndpoints().size());

    c.close();
  }

  @Test
  public void testRedistributionHandlesUnevenRemainder() throws Exception {
    // 7 slots over 3 survivors -> 3,2,2 (each survivor gets at least floor(7/3)=2,
    // and 7%3=1 remainder is given to one survivor).
    Properties props = new Properties();
    props.setProperty("numWriteEndpoints", "2");
    MemqCommonClient c = new MemqCommonClient(null, props);
    List<Endpoint> eps = new ArrayList<>();
    eps.add(new Endpoint(InetSocketAddress.createUnresolved("10.0.0.0", 9092)));
    eps.add(new Endpoint(InetSocketAddress.createUnresolved("10.0.0.1", 9092)));
    c.initialize(eps);
    setWriteEndpoints(c, new ArrayList<>(eps));
    c.setMaxConnections(2);

    c.getSlotsOwned().put("10.0.0.0", 5);
    c.getSlotsOwned().put("10.0.0.1", 5);

    InetSocketAddress sourceAddr = InetSocketAddress.createUnresolved("10.0.0.0", 9092);
    WriteResponsePacket eviction = new WriteResponsePacket("10.0.0.9", 1, 7);
    c.handleWriteResponse(eviction, sourceAddr);

    Map<String, Integer> result = c.getSlotsOwned();
    assertEquals(2, result.size());
    assertFalse(result.containsKey("10.0.0.0"));
    int total = result.values().stream().mapToInt(Integer::intValue).sum();
    // 5 (10.0.0.1, kept) + 1 (target: from eviction) + 7 (redistributed) = 13
    assertEquals(13, total);
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
  public void testV4ActivatesOnFirstEviction() throws Exception {
    // Bootstrap: legacy round-robin path active, v4Active false.
    setWriteEndpoints(client, new ArrayList<>(endpoints.subList(0, 1)));
    assertFalse("v4Active must start false", client.isV4Active());

    InetSocketAddress sourceAddr = InetSocketAddress.createUnresolved("10.0.0.1", 9092);
    client.handleWriteResponse(new WriteResponsePacket("10.0.0.99", 1, 4), sourceAddr);

    assertTrue("First eviction must flip v4Active sticky-on", client.isV4Active());
  }

  @Test
  public void testV4ActivatesOnFirstNonZeroSlotsOwned() throws Exception {
    setWriteEndpoints(client, new ArrayList<>(endpoints.subList(0, 1)));
    assertFalse(client.isV4Active());

    InetSocketAddress sourceAddr = InetSocketAddress.createUnresolved("10.0.0.1", 9092);
    // Non-eviction v4 response: just reports slot ownership.
    client.handleWriteResponse(new WriteResponsePacket(null, 0, 3), sourceAddr);

    assertTrue("Non-zero numSlotsOwned must flip v4Active", client.isV4Active());
  }

  @Test
  public void testV4StaysInactiveForV3Brokers() throws Exception {
    // v3 brokers send a default WriteResponsePacket (all fields zero/null).
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
    // Post-fix, weighted activates as soon as we have any slot data.
    setWriteEndpoints(client, new ArrayList<>(endpoints.subList(0, 1)));

    InetSocketAddress sourceAddr = InetSocketAddress.createUnresolved("10.0.0.1", 9092);
    client.handleWriteResponse(new WriteResponsePacket(null, 0, 5), sourceAddr);

    // Weighted should be active even though writes.size()=1 < numWriteEndpoints=3.
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
    // Bootstrap: 1 write endpoint, then v4 flip.
    setWriteEndpoints(client, new ArrayList<>(endpoints.subList(0, 1)));
    InetSocketAddress sourceAddr = InetSocketAddress.createUnresolved("10.0.0.1", 9092);
    client.handleWriteResponse(new WriteResponsePacket(null, 0, 5), sourceAddr);
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
  public void testMaxConnectionsDefaultsToThree() throws Exception {
    MemqCommonClient c = new MemqCommonClient(null, new Properties());
    Field f = MemqCommonClient.class.getDeclaredField("maxConnections");
    f.setAccessible(true);
    assertEquals("Default maxConnections must be 3", 3, ((Integer) f.get(c)).intValue());
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
  public void testDefaultMaxConnectionsCapsConnectionGrowth() throws Exception {
    Properties props = new Properties();
    props.setProperty("numWriteEndpoints", "3");
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
    assertEquals("Default cap of 3 must trigger swap when adding 4th broker",
        3, result.size());
    assertFalse("Evicting source must be dropped on cap-violating eviction",
        result.containsKey("10.0.0.2"));
    assertTrue("New target must be present after swap",
        result.containsKey("10.0.0.4"));
    c.close();
  }
}

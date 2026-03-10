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
    WriteResponsePacket evictionResponse = new WriteResponsePacket("10.0.0.4", 9092, 2, 3);
    client.handleWriteResponse(evictionResponse, sourceAddr);

    assertEquals(2, (int) client.getSlotsOwned().getOrDefault("10.0.0.4", 0));
    assertEquals(3, (int) client.getSlotsOwned().getOrDefault("10.0.0.1", 0));
  }

  @Test
  public void testNonEvictionResponseDoesNotCrash() {
    WriteResponsePacket normalResponse = new WriteResponsePacket(null, 0, 0, 5);
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
  public void testMaxConnectionsEnforcement() {
    client.setMaxConnections(2);
    client.getSlotsOwned().put("10.0.0.1", 5);
    client.getSlotsOwned().put("10.0.0.2", 3);
    client.getSlotsOwned().put("10.0.0.3", 1);

    InetSocketAddress sourceAddr = InetSocketAddress.createUnresolved("10.0.0.1", 9092);
    WriteResponsePacket evictionResponse = new WriteResponsePacket("10.0.0.4", 9092, 1, 4);
    client.handleWriteResponse(evictionResponse, sourceAddr);

    assertTrue("Expected at most 2 connections, got " + client.getSlotsOwned().size(),
        client.getSlotsOwned().size() <= 2);
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
}

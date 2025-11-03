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

import static org.junit.Assert.*;

import com.google.common.collect.ImmutableSet;
import com.pinterest.memq.commons.protocol.Broker;
import com.pinterest.memq.commons.protocol.Broker.BrokerType;
import com.pinterest.memq.commons.protocol.RequestPacket;
import com.pinterest.memq.commons.protocol.RequestType;
import com.pinterest.memq.commons.protocol.ResponseCodes;
import com.pinterest.memq.commons.protocol.ResponsePacket;
import com.pinterest.memq.commons.protocol.TopicAssignment;
import com.pinterest.memq.commons.protocol.TopicConfig;
import com.pinterest.memq.commons.protocol.TopicMetadata;
import com.pinterest.memq.commons.protocol.TopicMetadataRequestPacket;
import com.pinterest.memq.commons.protocol.TopicMetadataResponsePacket;

import io.netty.channel.ChannelHandlerContext;
import org.junit.Before;
import org.junit.Test;

import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;

public class TestMemqCommonClient {

  private static final ScheduledExecutorService scheduler = new ScheduledThreadPoolExecutor(1);
  private static final String LOCALHOST_STRING = "127.0.0.1";
  private int port = -1;

  private Endpoint commonEndpoint;

  @Before
  public void generateRandomPort() {
    int newPort = -1;
    while (port == newPort) {
      newPort = ThreadLocalRandom.current().nextInt(20000, 30000);
    }
    port = newPort;
    commonEndpoint = new Endpoint(InetSocketAddress.createUnresolved(LOCALHOST_STRING, port), "test");
  }

  @Test
  public void testInitialize() throws Exception {
    MemqCommonClient client = new MemqCommonClient("test", null, new Properties());
    try {
      client.initialize(Collections.emptyList());
      fail("should not initialize with empty list");
    } catch (Exception e) {
      assertEquals("No endpoints available", e.getMessage());
    }
    client.close();
  }

  @Test
  public void testSendRequestPacketAndReturnResponseFuture() throws Exception {
    MemqCommonClient client = new MemqCommonClient("test", null, new Properties());

    // not initialized
    try {
      client.sendRequestPacketAndReturnResponseFuture(null, "test", 10000);
      fail("should fail since not initialized");
    } catch (IllegalStateException ise) {
      // good
    } catch (Exception e) {
      fail("failed: " + e);
    }

    // no connection
    client.initialize(Collections.singletonList(commonEndpoint));
    RequestPacket request = new RequestPacket(RequestType.PROTOCOL_VERSION, 1, RequestType.TOPIC_METADATA, new TopicMetadataRequestPacket("test"));
    try {
      client.sendRequestPacketAndReturnResponseFuture(request, "test", 10000);
      fail("should fail since non connection");
    } catch (ExecutionException ee) {
      assertTrue(ee.getCause() instanceof ConnectException);
    } catch (Exception e) {
      fail("failed: " + e);
    }

    Map<RequestType, BiConsumer<ChannelHandlerContext, RequestPacket>> map = new HashMap<>();

    map.put(RequestType.TOPIC_METADATA, (ctx, req) -> {
      TopicMetadataRequestPacket mdPkt = (TopicMetadataRequestPacket) req.getPayload();
      TopicConfig topicConfig = new TopicConfig("test", "dev");
      TopicAssignment topicAssignment = new TopicAssignment(topicConfig, 100.0);
      Set<Broker>
          brokers =
          Collections.singleton(new Broker(LOCALHOST_STRING, (short) port, "n/a", "n/a", BrokerType.WRITE,
              Collections.singleton(topicAssignment)));
      ResponsePacket
          resp =
          new ResponsePacket(req.getProtocolVersion(), req.getClientRequestId(),
              req.getRequestType(),
              ResponseCodes.OK, new TopicMetadataResponsePacket(
              new TopicMetadata(mdPkt.getTopic(), brokers, ImmutableSet.of(), "dev", new Properties())));
      ctx.writeAndFlush(resp);
    });

    MockMemqServer mockServer = new MockMemqServer(port, map);
    mockServer.start();

    try {
      client.initialize(Collections.singletonList(commonEndpoint));
      Future<ResponsePacket> respFuture = client.sendRequestPacketAndReturnResponseFuture(request, "test", 10000);
      ResponsePacket resp = respFuture.get();
      assertEquals(ResponseCodes.OK, resp.getResponseCode());
    } catch (Exception e) {
      fail("failed: " + e);
    }

    client.close();
    mockServer.stop();
  }

  @Test
  public void testSendRequestPacketAndReturnResponseFutureFailAfterTimeout() throws Exception {
    MemqCommonClient client = new MemqCommonClient("test", null, new Properties());
    client.initialize(Collections.singletonList(commonEndpoint));

    Map<RequestType, BiConsumer<ChannelHandlerContext, RequestPacket>> map = new HashMap<>();
    map.put(RequestType.TOPIC_METADATA, (ctx, req) -> {
      try {
        Thread.sleep(2000);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
      TopicMetadataRequestPacket mdPkt = (TopicMetadataRequestPacket) req.getPayload();
      TopicConfig topicConfig = new TopicConfig("test", "dev");
      TopicAssignment topicAssignment = new TopicAssignment(topicConfig, 100.0);
      Set<Broker>
          brokers =
          Collections.singleton(new Broker(LOCALHOST_STRING, (short) port, "n/a", "n/a", BrokerType.WRITE,
              Collections.singleton(topicAssignment)));
      ResponsePacket
          resp =
          new ResponsePacket(req.getProtocolVersion(), req.getClientRequestId(),
              req.getRequestType(),
              ResponseCodes.OK, new TopicMetadataResponsePacket(
              new TopicMetadata(mdPkt.getTopic(), brokers, ImmutableSet.of(), "dev", new Properties())));
      ctx.writeAndFlush(resp);
    });

    MockMemqServer mockServer = new MockMemqServer(port, map);
    scheduler.schedule(() -> {
      try {
        mockServer.start();
      } catch (Exception e) {
        fail(e.getMessage());
      }
    }, 1200, TimeUnit.MILLISECONDS);

    RequestPacket request = new RequestPacket(RequestType.PROTOCOL_VERSION, 1, RequestType.TOPIC_METADATA, new TopicMetadataRequestPacket("test"));

    try {
      Future<ResponsePacket> respFuture = client.sendRequestPacketAndReturnResponseFuture(request, "test",3000);
      ResponsePacket resp = respFuture.get();
      fail("should throw timeout exception");
    } catch (ExecutionException ee) {
      assertTrue(ee.getCause() instanceof TimeoutException);
    }

    client.close();
    mockServer.stop();
  }

  @Test
  public void testSendRequestPacketAndReturnResponseFutureFailAfterRetry() throws Exception {
    MemqCommonClient client = new MemqCommonClient("test", null, new Properties());
    client.initialize(Collections.singletonList(commonEndpoint));

    Map<RequestType, BiConsumer<ChannelHandlerContext, RequestPacket>> map = new HashMap<>();
    map.put(RequestType.TOPIC_METADATA, (ctx, req) -> {
      TopicMetadataRequestPacket mdPkt = (TopicMetadataRequestPacket) req.getPayload();
      TopicConfig topicConfig = new TopicConfig("test", "dev");
      TopicAssignment topicAssignment = new TopicAssignment(topicConfig, 100.0);
      Set<Broker>
          brokers =
          Collections.singleton(new Broker(LOCALHOST_STRING, (short) port, "n/a", "n/a", BrokerType.WRITE,
              Collections.singleton(topicAssignment)));
      ResponsePacket
          resp =
          new ResponsePacket(req.getProtocolVersion(), req.getClientRequestId(),
              req.getRequestType(),
              ResponseCodes.OK, new TopicMetadataResponsePacket(
              new TopicMetadata(mdPkt.getTopic(), brokers, ImmutableSet.of(), "dev", new Properties())));
      ctx.writeAndFlush(resp);
    });

    MockMemqServer mockServer = new MockMemqServer(port, map);
    scheduler.schedule(() -> {
      try {
        mockServer.start();
      } catch (Exception e) {
        fail(e.getMessage());
      }
    }, 5000, TimeUnit.MILLISECONDS);

    RequestPacket request = new RequestPacket(RequestType.PROTOCOL_VERSION, 1, RequestType.TOPIC_METADATA, new TopicMetadataRequestPacket("test"));

    try {
      Future<ResponsePacket> respFuture = client.sendRequestPacketAndReturnResponseFuture(request, "test", 5000);
      respFuture.get();
      fail("should fail since connection is dropped");
    } catch (ExecutionException ee) {
      assertTrue(ee.getCause() instanceof ConnectException);
    } catch (Exception e) {
      fail("failed: " + e);
    }

    client.close();
    mockServer.stop();
  }

  @Test
  public void testGetLocalityEndpoints() throws Exception {
    MemqCommonClient client = new MemqCommonClient("test", null, new Properties());
    List<Endpoint> localityEndpoints = client.getLocalityEndpoints(Arrays.asList(
        new Endpoint(InetSocketAddress.createUnresolved(LOCALHOST_STRING, 9092), "test"),
        new Endpoint(InetSocketAddress.createUnresolved(LOCALHOST_STRING, 9093), "test2"),
        new Endpoint(InetSocketAddress.createUnresolved(LOCALHOST_STRING, 9094), "test3")
    ));
    assertEquals(1, localityEndpoints.size());
    assertEquals("test", localityEndpoints.get(0).getLocality());

    client.close();
  }

  @Test
  public void testGetTopicMetadata() throws Exception {
    Map<RequestType, BiConsumer<ChannelHandlerContext, RequestPacket>> map = new HashMap<>();

    map.put(RequestType.TOPIC_METADATA, (ctx, req) -> {
      TopicMetadataRequestPacket mdPkt = (TopicMetadataRequestPacket) req.getPayload();
      TopicConfig topicConfig = new TopicConfig("test", "dev");
      TopicAssignment topicAssignment = new TopicAssignment(topicConfig, 100.0);
      Set<Broker>
          brokers =
          Collections.singleton(new Broker(LOCALHOST_STRING, (short) port, "n/a", "n/a", BrokerType.WRITE,
              Collections.singleton(topicAssignment)));
      ResponsePacket
          resp =
          new ResponsePacket(req.getProtocolVersion(), req.getClientRequestId(),
              req.getRequestType(),
              ResponseCodes.OK, new TopicMetadataResponsePacket(
              new TopicMetadata(mdPkt.getTopic(), brokers, ImmutableSet.of(), "dev", new Properties())));
      ctx.writeAndFlush(resp);
    });

    MockMemqServer mockServer = new MockMemqServer(port, map);
    mockServer.start();

    MemqCommonClient client = new MemqCommonClient("test", null, new Properties());
    client.initialize(Collections.singletonList(commonEndpoint));
    TopicMetadata md = client.getTopicMetadata("test", 3000);
    assertEquals(1, md.getWriteBrokers().size());
    assertEquals("dev", md.getStorageHandlerName());

    client.close();
    mockServer.stop();  
  }

  @Test
  public void testReconnect() throws Exception {

    Map<RequestType, BiConsumer<ChannelHandlerContext, RequestPacket>> map = new HashMap<>();

    AtomicInteger count = new AtomicInteger(0);
    map.put(RequestType.TOPIC_METADATA, (ctx, req) -> {
      TopicMetadataRequestPacket mdPkt = (TopicMetadataRequestPacket) req.getPayload();
      TopicConfig topicConfig = new TopicConfig("test", "dev");
      TopicAssignment topicAssignment = new TopicAssignment(topicConfig, 100.0);
      Set<Broker> brokers = new HashSet<>();
      int currentCount = count.getAndIncrement();
      for (int i = 0; i <= currentCount; i++) {
        brokers.add(new Broker("127.0.0." + (i + 1), (short) port, "n/a", "n/a", BrokerType.WRITE, Collections.singleton(topicAssignment)));
      }
      ResponsePacket
          resp =
          new ResponsePacket(req.getProtocolVersion(), req.getClientRequestId(),
              req.getRequestType(),
              ResponseCodes.OK, new TopicMetadataResponsePacket(
              new TopicMetadata(mdPkt.getTopic(), brokers, ImmutableSet.of(), "dev", new Properties())));
      ctx.writeAndFlush(resp);
    });

    MockMemqServer mockServer = new MockMemqServer(port, map);
    mockServer.start();

    MemqCommonClient client = new MemqCommonClient("test", null, new Properties());
    client.initialize(Collections.singletonList(commonEndpoint));
    TopicMetadata md = client.getTopicMetadata("test", 3000);
    assertEquals(1, md.getWriteBrokers().size());
    assertEquals("dev", md.getStorageHandlerName());

    client.reconnect("test", false);
    List<Endpoint> endpoints = client.getEndpointsToTry();
    assertEquals(2, endpoints.size());
    assertNotEquals(endpoints.get(0), endpoints.get(1));
    client.close();
    mockServer.stop();
  }

  @Test
  public void testDeprioritizeAndRemoveDeadEndpointAfterTwoFailures() throws Exception {
    Properties networkProps = new Properties();
    networkProps.setProperty(MemqCommonClient.CONFIG_NUM_WRITE_ENDPOINTS, "2");
    MemqCommonClient client = new MemqCommonClient("test", null, networkProps);

    Endpoint dead = new Endpoint(InetSocketAddress.createUnresolved(LOCALHOST_STRING, port), "test");
    Endpoint e2 = new Endpoint(InetSocketAddress.createUnresolved(LOCALHOST_STRING, port + 1), "test");
    Endpoint e3 = new Endpoint(InetSocketAddress.createUnresolved(LOCALHOST_STRING, port + 2), "test");
    client.initialize(Arrays.asList(dead, e2, e3));

    // First deprioritization: moved to end, still present
    client.deprioritizeDeadEndpoint(dead, "topic");
    List<Endpoint> orderAfterFirst = client.getEndpointsToTry();
    assertTrue(orderAfterFirst.contains(dead));
    assertFalse(orderAfterFirst.get(0).equals(dead));

    // Second deprioritization: removed from consideration
    client.deprioritizeDeadEndpoint(dead, "topic");
    List<Endpoint> orderAfterSecond = client.getEndpointsToTry();
    assertFalse(orderAfterSecond.contains(dead));
    assertEquals(2, orderAfterSecond.size());

    client.close();
  }

  @Test
  public void testStickyWriteEndpointsAndRoundRobinRotation() throws Exception {
    Properties networkProps = new Properties();
    networkProps.setProperty(MemqCommonClient.CONFIG_NUM_WRITE_ENDPOINTS, "2");
    MemqCommonClient client = new MemqCommonClient("test", null, networkProps);

    Endpoint e1 = new Endpoint(InetSocketAddress.createUnresolved(LOCALHOST_STRING, port), "test");
    Endpoint e2 = new Endpoint(InetSocketAddress.createUnresolved(LOCALHOST_STRING, port + 1), "test");
    Endpoint e3 = new Endpoint(InetSocketAddress.createUnresolved(LOCALHOST_STRING, port + 2), "test");
    client.initialize(Arrays.asList(e1, e2, e3));

    // Start servers on all three endpoints (all alive)
    Map<RequestType, BiConsumer<ChannelHandlerContext, RequestPacket>> map = new HashMap<>();
    map.put(RequestType.TOPIC_METADATA, (ctx, req) -> {
      TopicMetadataRequestPacket mdPkt = (TopicMetadataRequestPacket) req.getPayload();
      TopicConfig topicConfig = new TopicConfig("test", "dev");
      TopicAssignment topicAssignment = new TopicAssignment(topicConfig, 100.0);
      Set<Broker> brokers = new HashSet<>();
      brokers.add(new Broker(LOCALHOST_STRING, (short) port, "n/a", "n/a", BrokerType.WRITE, Collections.singleton(topicAssignment)));
      brokers.add(new Broker(LOCALHOST_STRING, (short) (port + 1), "n/a", "n/a", BrokerType.WRITE, Collections.singleton(topicAssignment)));
      brokers.add(new Broker(LOCALHOST_STRING, (short) (port + 2), "n/a", "n/a", BrokerType.WRITE, Collections.singleton(topicAssignment)));
      ResponsePacket resp = new ResponsePacket(req.getProtocolVersion(), req.getClientRequestId(),
          req.getRequestType(), ResponseCodes.OK,
          new TopicMetadataResponsePacket(new TopicMetadata(mdPkt.getTopic(), brokers,
              ImmutableSet.of(), "dev", new Properties())));
      ctx.writeAndFlush(resp);
    });
    MockMemqServer s1 = new MockMemqServer(port, map);
    MockMemqServer s2 = new MockMemqServer(port + 1, map);
    MockMemqServer s3 = new MockMemqServer(port + 2, map);
    s1.start();
    s2.start();
    s3.start();

    // Send a few requests to register two working write endpoints
    RequestPacket request = new RequestPacket(RequestType.PROTOCOL_VERSION, 1,
        RequestType.TOPIC_METADATA, new TopicMetadataRequestPacket("test"));
    for (int i = 0; i < 4; i++) {
      client.sendRequestPacketAndReturnResponseFuture(request, "test", 3000).get();
    }

    List<Endpoint> sticky = new ArrayList<>(client.currentWriteEndpoints());
    assertEquals(2, sticky.size());

    // Ensure they remain sticky across additional sends
    for (int i = 0; i < 10; i++) {
      client.sendRequestPacketAndReturnResponseFuture(request, "test", 3000).get();
      List<Endpoint> current = client.currentWriteEndpoints();
      assertEquals(new HashSet<>(sticky), new HashSet<>(current));
    }

    // Verify round-robin rotation among sticky endpoints
    List<Endpoint> first = client.getEndpointsToTry();
    assertTrue(first.get(0).equals(sticky.get(0)) || first.get(0).equals(sticky.get(1)));
    List<Endpoint> second = client.getEndpointsToTry();
    // The first position should rotate to the other sticky endpoint
    assertNotEquals(first.get(0), second.get(0));
    assertTrue(new HashSet<>(Arrays.asList(first.get(0), second.get(0))).containsAll(sticky));

    client.close();
    s1.stop();
    s2.stop();
    s3.stop();
  }

  @Test
  public void testWriteEndpointsSelectionRandomizedAcrossRuns() throws Exception {
    // Probabilistic: across multiple runs, the chosen sticky endpoints should vary at least once
    int runs = 8;
    Set<Set<Integer>> selections = new HashSet<>();

    for (int r = 0; r < runs; r++) {
      int base = port + 10 + (r * 10);
      Properties networkProps = new Properties();
      networkProps.setProperty(MemqCommonClient.CONFIG_NUM_WRITE_ENDPOINTS, "2");
      MemqCommonClient client = new MemqCommonClient("test", null, networkProps);

      Endpoint e1 = new Endpoint(InetSocketAddress.createUnresolved(LOCALHOST_STRING, base), "test");
      Endpoint e2 = new Endpoint(InetSocketAddress.createUnresolved(LOCALHOST_STRING, base + 1), "test");
      Endpoint e3 = new Endpoint(InetSocketAddress.createUnresolved(LOCALHOST_STRING, base + 2), "test");
      client.initialize(Arrays.asList(e1, e2, e3));

      Map<RequestType, BiConsumer<ChannelHandlerContext, RequestPacket>> map = new HashMap<>();
      map.put(RequestType.TOPIC_METADATA, (ctx, req) -> {
        TopicMetadataRequestPacket mdPkt = (TopicMetadataRequestPacket) req.getPayload();
        TopicConfig topicConfig = new TopicConfig("test", "dev");
        TopicAssignment topicAssignment = new TopicAssignment(topicConfig, 100.0);
        Set<Broker> brokers = new HashSet<>();
        brokers.add(new Broker(LOCALHOST_STRING, (short) base, "n/a", "n/a", BrokerType.WRITE, Collections.singleton(topicAssignment)));
        brokers.add(new Broker(LOCALHOST_STRING, (short) (base + 1), "n/a", "n/a", BrokerType.WRITE, Collections.singleton(topicAssignment)));
        brokers.add(new Broker(LOCALHOST_STRING, (short) (base + 2), "n/a", "n/a", BrokerType.WRITE, Collections.singleton(topicAssignment)));
        ResponsePacket resp = new ResponsePacket(req.getProtocolVersion(), req.getClientRequestId(),
            req.getRequestType(), ResponseCodes.OK,
            new TopicMetadataResponsePacket(new TopicMetadata(mdPkt.getTopic(), brokers,
                ImmutableSet.of(), "dev", new Properties())));
        ctx.writeAndFlush(resp);
      });
      MockMemqServer s1 = new MockMemqServer(base, map);
      MockMemqServer s2 = new MockMemqServer(base + 1, map);
      MockMemqServer s3 = new MockMemqServer(base + 2, map);
      s1.start();
      s2.start();
      s3.start();

      RequestPacket request = new RequestPacket(RequestType.PROTOCOL_VERSION, 1,
          RequestType.TOPIC_METADATA, new TopicMetadataRequestPacket("test"));
      for (int i = 0; i < 3; i++) {
        client.sendRequestPacketAndReturnResponseFuture(request, "test", 3000).get();
      }
      List<Endpoint> sticky = client.currentWriteEndpoints();
      selections.add(new HashSet<>(Arrays.asList(sticky.get(0).getAddress().getPort(), sticky.get(1).getAddress().getPort())));

      client.close();
      s1.stop();
      s2.stop();
      s3.stop();
    }

    // Expect at least two distinct selections across runs to indicate random choice
    assertTrue("Expected at least two distinct sticky endpoint selections across runs", selections.size() >= 2);
  }

  @Test
  public void testSendFailureRefreshesWriteEndpoints() throws Exception {
    // Use real MemqCommonClient and force first attempt to target a dead endpoint by rotating write endpoints
    Properties networkProps = new Properties();
    networkProps.setProperty(MemqCommonClient.CONFIG_NUM_WRITE_ENDPOINTS, "2");
    MemqCommonClient client = new MemqCommonClient("test", null, networkProps);

    // Prepare endpoints: include a dead endpoint and a live endpoint
    Endpoint dead = new Endpoint(InetSocketAddress.createUnresolved(LOCALHOST_STRING, port), "test");
    Endpoint alive = new Endpoint(InetSocketAddress.createUnresolved(LOCALHOST_STRING, port + 1), "test");
    client.initialize(Arrays.asList(dead, alive));

    // Start server on the alive endpoint
    Map<RequestType, BiConsumer<ChannelHandlerContext, RequestPacket>> map = new HashMap<>();
    map.put(RequestType.TOPIC_METADATA, (ctx, req) -> {
      TopicMetadataRequestPacket mdPkt = (TopicMetadataRequestPacket) req.getPayload();
      TopicConfig topicConfig = new TopicConfig("test", "dev");
      TopicAssignment topicAssignment = new TopicAssignment(topicConfig, 100.0);
      Set<Broker> brokers = Collections.singleton(new Broker(LOCALHOST_STRING, (short) (port + 1), "n/a",
          "n/a", BrokerType.WRITE, Collections.singleton(topicAssignment)));
      ResponsePacket resp = new ResponsePacket(req.getProtocolVersion(), req.getClientRequestId(),
          req.getRequestType(), ResponseCodes.OK,
          new TopicMetadataResponsePacket(new TopicMetadata(mdPkt.getTopic(), brokers,
              ImmutableSet.of(), "dev", new Properties())));
      ctx.writeAndFlush(resp);
    });
    MockMemqServer server = new MockMemqServer(port + 1, map);
    server.start();

    // Rotate write endpoints so the first endpoint to try is the dead one
    List<Endpoint> endpointsToTry = client.getEndpointsToTry();
    int attempts = 0;
    while (endpointsToTry.get(0).getAddress().getPort() != port && attempts++ < 5) {
      endpointsToTry = client.getEndpointsToTry();
    }
    assertEquals(port, endpointsToTry.get(0).getAddress().getPort());

    // First attempt should hit the dead endpoint (connect failure) → triggers refreshWriteEndpoints; next attempt hits alive and succeeds
    RequestPacket request = new RequestPacket(RequestType.PROTOCOL_VERSION, 1,
        RequestType.TOPIC_METADATA, new TopicMetadataRequestPacket("test"));

    ResponsePacket resp = client.sendRequestPacketAndReturnResponseFuture(request, "test", 5000).get();
    assertEquals(ResponseCodes.OK, resp.getResponseCode());

    // Verify write endpoints were refreshed to prefer the alive endpoint now
    List<Endpoint> writeEndpoints = client.currentWriteEndpoints();
    assertEquals(alive.getAddress(), writeEndpoints.get(0).getAddress());

    client.close();
    server.stop();
  }
}
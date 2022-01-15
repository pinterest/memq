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
      assertEquals("Failed to initialize, no endpoints available", e.getMessage());
    }

  }

  @Test
  public void testSendRequestPacketAndReturnResponseFuture() throws Exception {
    MemqCommonClient client = new MemqCommonClient("test", null, new Properties());

    // not initialized
    try {
      client.sendRequestPacketAndReturnResponseFuture(null, 10000);
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
      client.sendRequestPacketAndReturnResponseFuture(request, 10000);
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
      Future<ResponsePacket> respFuture = client.sendRequestPacketAndReturnResponseFuture(request, 10000);
      ResponsePacket resp = respFuture.get();
      assertEquals(ResponseCodes.OK, resp.getResponseCode());
    } catch (Exception e) {
      fail("failed: " + e);
    }

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
      Future<ResponsePacket> respFuture = client.sendRequestPacketAndReturnResponseFuture(request, 3000);
      ResponsePacket resp = respFuture.get();
      fail("should throw timeout exception");
    } catch (ExecutionException ee) {
      assertTrue(ee.getCause() instanceof TimeoutException);
    }

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
      Future<ResponsePacket> respFuture = client.sendRequestPacketAndReturnResponseFuture(request, 5000);
      respFuture.get();
      fail("should fail since connection is dropped");
    } catch (ExecutionException ee) {
      assertTrue(ee.getCause() instanceof ConnectException);
    } catch (Exception e) {
      fail("failed: " + e);
    }

    mockServer.stop();
  }

  @Test
  public void testGetEndpointsToTry() throws Exception {
    Endpoint commonEndpoint = new Endpoint(InetSocketAddress.createUnresolved(LOCALHOST_STRING, 9092), "test");
    MemqCommonClient client = new MemqCommonClient("test", null, new Properties());
    client.initialize(
        Arrays.asList(
            commonEndpoint,
            new Endpoint(InetSocketAddress.createUnresolved(LOCALHOST_STRING, 9093), "test2"),
            new Endpoint(InetSocketAddress.createUnresolved(LOCALHOST_STRING, 9094), "test3")
        )
    );
    assertEquals(1, client.getEndpointsToTry().size());

    MemqCommonClient client2 = new MemqCommonClient("test", null, new Properties());
    client.initialize(
        Arrays.asList(
            commonEndpoint,
            new Endpoint(InetSocketAddress.createUnresolved(LOCALHOST_STRING, 9093), "test"),
            new Endpoint(InetSocketAddress.createUnresolved(LOCALHOST_STRING, 9094), "test")
        )
    );
    client.setCurrentEndpoint(commonEndpoint);
    List<Endpoint> endpointsToTry = client.getEndpointsToTry();
    assertEquals(4, endpointsToTry.size());
    assertEquals(endpointsToTry.get(0), commonEndpoint);
    assertNotEquals(endpointsToTry.get(1), endpointsToTry.get(2));
    assertNotEquals(endpointsToTry.get(2), endpointsToTry.get(3));
    assertNotEquals(endpointsToTry.get(3), endpointsToTry.get(1));
  }

  @Test
  public void testGetPreferredEndpoints() throws Exception {
    MemqCommonClient client = new MemqCommonClient("test", null, new Properties());
    List<Endpoint> preferred = client.getPreferredEndpoints(Arrays.asList(
        new Endpoint(InetSocketAddress.createUnresolved(LOCALHOST_STRING, 9092), "test"),
        new Endpoint(InetSocketAddress.createUnresolved(LOCALHOST_STRING, 9093), "test2"),
        new Endpoint(InetSocketAddress.createUnresolved(LOCALHOST_STRING, 9094), "test3")
    ));
    assertEquals(1, preferred.size());
    assertEquals("test", preferred.get(0).getLocality());
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
  }
}
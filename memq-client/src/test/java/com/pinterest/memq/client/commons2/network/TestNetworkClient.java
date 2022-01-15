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
package com.pinterest.memq.client.commons2.network;

import static org.junit.Assert.*;

import com.google.common.collect.ImmutableSet;
import com.pinterest.memq.client.commons2.MockMemqServer;
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

import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.util.ResourceLeakDetector;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;

public class TestNetworkClient {
  private static final ScheduledExecutorService scheduler = new ScheduledThreadPoolExecutor(1);
  private static final String LOCALHOST_STRING = "127.0.0.1";
  private int port = -1;

  @BeforeClass
  public static void setup() {
    ResourceLeakDetector.setLevel(ResourceLeakDetector.Level.PARANOID);
  }

  @Before
  public void generateRandomPort() {
    int newPort = -1;
    while (port == newPort) {
      newPort = ThreadLocalRandom.current().nextInt(10000, 20000);
    }
    port = newPort;
  }

  @Test
  public void testSendSimple() throws Exception {
    Map<RequestType, BiConsumer<ChannelHandlerContext, RequestPacket>> map = new HashMap<>();

    map.put(RequestType.TOPIC_METADATA, (ctx, req) -> {
      TopicMetadataRequestPacket mdPkt = (TopicMetadataRequestPacket) req.getPayload();
      TopicConfig topicConfig = new TopicConfig("test", "dev");
      TopicAssignment topicAssignment = new TopicAssignment(topicConfig, 100.0);
      Set<Broker> brokers = Collections.singleton(new Broker(LOCALHOST_STRING, (short) port, "n/a",
          "n/a", BrokerType.WRITE, Collections.singleton(topicAssignment)));
      ResponsePacket resp = new ResponsePacket(req.getProtocolVersion(), req.getClientRequestId(),
          req.getRequestType(), ResponseCodes.OK,
          new TopicMetadataResponsePacket(new TopicMetadata(mdPkt.getTopic(), brokers,
              ImmutableSet.of(), "dev", new Properties())));
      ctx.writeAndFlush(resp);
    });

    MockMemqServer mockServer = new MockMemqServer(port, map);
    mockServer.start();

    RequestPacket mdPkt = new RequestPacket(RequestType.PROTOCOL_VERSION, 1,
        RequestType.TOPIC_METADATA, new TopicMetadataRequestPacket("test"));
    NetworkClient client = new NetworkClient();
    Future<ResponsePacket> pktFuture = client
        .send(InetSocketAddress.createUnresolved(LOCALHOST_STRING, port), mdPkt);
    ResponsePacket resp = pktFuture.get();
    assertEquals(ResponseCodes.OK, resp.getResponseCode());
    assertEquals(RequestType.TOPIC_METADATA, resp.getRequestType());
    mockServer.stop();
  }

  @Test
  public void testSendTimeout() throws Exception {
    Map<RequestType, BiConsumer<ChannelHandlerContext, RequestPacket>> map = new HashMap<>();
    map.put(RequestType.TOPIC_METADATA, (ctx, req) -> {
      // do nothing (simulating hanging server)
    });

    MockMemqServer mockServer = new MockMemqServer(port, map);
    mockServer.start();

    NetworkClient client = new NetworkClient();
    RequestPacket mdPkt1 = new RequestPacket(RequestType.PROTOCOL_VERSION, 1,
        RequestType.TOPIC_METADATA, new TopicMetadataRequestPacket("test"));
    Future<ResponsePacket> pktFuture = client.send(
        InetSocketAddress.createUnresolved(LOCALHOST_STRING, port), mdPkt1,
        Duration.ofMillis(1000));

    try {
      pktFuture.get();
      fail("should throw timeout exception");
    } catch (ExecutionException ee) {
      assertTrue(ee.getCause() instanceof TimeoutException);
    } catch (Exception e) {
      fail("failed: " + e);
    }
  }

  @Test
  public void testSendReconnectAfterConnectionClose() throws Exception {
    Map<RequestType, BiConsumer<ChannelHandlerContext, RequestPacket>> map = new HashMap<>();
    map.put(RequestType.TOPIC_METADATA, (ctx, req) -> {
      // only third request will go through, others will be dropped
      if (req.getClientRequestId() != 3) {
        try {
          Thread.sleep(1000);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
        ctx.close();
        return;
      }
      TopicMetadataRequestPacket mdPkt = (TopicMetadataRequestPacket) req.getPayload();
      TopicConfig topicConfig = new TopicConfig("test", "dev");
      TopicAssignment topicAssignment = new TopicAssignment(topicConfig, 100.0);
      Set<Broker> brokers = Collections.singleton(new Broker(LOCALHOST_STRING, (short) port, "n/a",
          "n/a", BrokerType.WRITE, Collections.singleton(topicAssignment)));
      ResponsePacket resp = new ResponsePacket(req.getProtocolVersion(), req.getClientRequestId(),
          req.getRequestType(), ResponseCodes.OK,
          new TopicMetadataResponsePacket(new TopicMetadata(mdPkt.getTopic(), brokers,
              ImmutableSet.of(), "dev", new Properties())));
      ctx.writeAndFlush(resp);
    });

    MockMemqServer mockServer = new MockMemqServer(port, map);
    mockServer.start();

    NetworkClient client = new NetworkClient();
    RequestPacket mdPkt1 = new RequestPacket(RequestType.PROTOCOL_VERSION, 1,
        RequestType.TOPIC_METADATA, new TopicMetadataRequestPacket("test"));
    RequestPacket mdPkt2 = new RequestPacket(RequestType.PROTOCOL_VERSION, 2,
        RequestType.TOPIC_METADATA, new TopicMetadataRequestPacket("test"));
    RequestPacket mdPkt3 = new RequestPacket(RequestType.PROTOCOL_VERSION, 3,
        RequestType.TOPIC_METADATA, new TopicMetadataRequestPacket("test"));
    Future<ResponsePacket> pktFuture1 = client
        .send(InetSocketAddress.createUnresolved(LOCALHOST_STRING, port), mdPkt1);
    Future<ResponsePacket> pktFuture2 = client
        .send(InetSocketAddress.createUnresolved(LOCALHOST_STRING, port), mdPkt2);

    try {
      pktFuture1.get();
      fail("should throw connection closed related IOException");
    } catch (ExecutionException ee) {
      assertTrue(ee.getCause() instanceof IOException); // exception will be either connect reset by
                                                        // peer or closed connection
    } catch (Exception e) {
      fail("failed: " + e);
    }

    try {
      pktFuture2.get();
      fail("should throw connection closed related IOException");
    } catch (ExecutionException ee) {
      assertTrue(ee.getCause() instanceof IOException);
    } catch (Exception e) {
      fail("failed: " + e);
    }

    Thread.sleep(1500);
    Future<ResponsePacket> pktFuture3 = client
        .send(InetSocketAddress.createUnresolved(LOCALHOST_STRING, port), mdPkt3);

    ResponsePacket resp3 = pktFuture3.get();
    assertEquals(ResponseCodes.OK, resp3.getResponseCode());
    assertEquals(RequestType.TOPIC_METADATA, resp3.getRequestType());
    mockServer.stop();
  }

  @Test
  public void testSendDrop() throws Exception {
    Map<RequestType, BiConsumer<ChannelHandlerContext, RequestPacket>> map = new HashMap<>();

    AtomicInteger count = new AtomicInteger(0);
    map.put(RequestType.TOPIC_METADATA, (ctx, req) -> {
      int currentCount = count.getAndIncrement();
      if (currentCount == 0) {
        ctx.close();
        return;
      } else if (currentCount == 1) {
        return;
      }
      try {
        Thread.sleep(500);
      } catch (Exception e) {
        // no-op
      }
      TopicMetadataRequestPacket mdPkt = (TopicMetadataRequestPacket) req.getPayload();
      TopicConfig topicConfig = new TopicConfig("test", "dev");
      TopicAssignment topicAssignment = new TopicAssignment(topicConfig, 100.0);
      Set<Broker> brokers = Collections.singleton(new Broker(LOCALHOST_STRING, (short) port, "n/a",
          "n/a", BrokerType.WRITE, Collections.singleton(topicAssignment)));
      ResponsePacket resp = new ResponsePacket(req.getProtocolVersion(), req.getClientRequestId(),
          req.getRequestType(), ResponseCodes.OK,
          new TopicMetadataResponsePacket(new TopicMetadata(mdPkt.getTopic(), brokers,
              ImmutableSet.of(), "dev", new Properties())));
      ctx.writeAndFlush(resp);
    });

    MockMemqServer mockServer = new MockMemqServer(port, map);
    mockServer.start();

    RequestPacket mdPkt = new RequestPacket(RequestType.PROTOCOL_VERSION, 1,
        RequestType.TOPIC_METADATA, new TopicMetadataRequestPacket("test"));

    Properties props = new Properties();
    props.setProperty(NetworkClient.CONFIG_IRRESPONSIVE_TIMEOUT_MS, "3000");
    NetworkClient client = new NetworkClient(props);

    // server closed connection
    Future<ResponsePacket> pktFuture = client
        .send(InetSocketAddress.createUnresolved(LOCALHOST_STRING, port), mdPkt);
    try {
      Thread.sleep(500);
      assertEquals(0, client.getInflightRequestCount());
      pktFuture.get(1000, TimeUnit.MILLISECONDS);
      fail("should throw closed connection exception");
    } catch (ExecutionException e) {
      assertTrue(e.getCause() instanceof ClosedConnectionException);
    } catch (Exception e) {
      fail("failed: " + e);
    }

    // Server taking a long time to respond (dropping requests)
    mdPkt.setClientRequestId(2);
    Future<ResponsePacket> pktFuture1 = client
        .send(InetSocketAddress.createUnresolved(LOCALHOST_STRING, port), mdPkt);
    try {
      Thread.sleep(500);
      assertEquals(1, client.getInflightRequestCount());
      pktFuture1.get(4500, TimeUnit.MILLISECONDS);
      fail("should throw timeout exception");
    } catch (ExecutionException e) {
      assertEquals(0, client.getInflightRequestCount());
      assertTrue(e.getCause() instanceof TimeoutException);
    } catch (Exception e) {
      fail("failed: " + e);
    }

    // happy path
    mdPkt.setClientRequestId(3);
    Future<ResponsePacket> pktFuture2 = client
        .send(InetSocketAddress.createUnresolved(LOCALHOST_STRING, port), mdPkt);
    Thread.sleep(100);
    assertEquals(1, client.getInflightRequestCount());
    ResponsePacket resp2 = pktFuture2.get();
    assertEquals(0, client.getInflightRequestCount());
    assertEquals(ResponseCodes.OK, resp2.getResponseCode());
    assertEquals(RequestType.TOPIC_METADATA, resp2.getRequestType());
    mockServer.stop();
  }

  @Test
  public void testSendMultipleRequests() throws Exception {
    Map<RequestType, BiConsumer<ChannelHandlerContext, RequestPacket>> map = new HashMap<>();

    map.put(RequestType.TOPIC_METADATA, (ctx, req) -> {
      try {
        Thread.sleep(500);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
      TopicMetadataRequestPacket mdPkt = (TopicMetadataRequestPacket) req.getPayload();
      TopicConfig topicConfig = new TopicConfig("test", "dev");
      TopicAssignment topicAssignment = new TopicAssignment(topicConfig, 100.0);
      Set<Broker> brokers = Collections.singleton(new Broker(LOCALHOST_STRING, (short) port, "n/a",
          "n/a", BrokerType.WRITE, Collections.singleton(topicAssignment)));
      ResponsePacket resp = new ResponsePacket(req.getProtocolVersion(), req.getClientRequestId(),
          req.getRequestType(), ResponseCodes.OK,
          new TopicMetadataResponsePacket(new TopicMetadata(mdPkt.getTopic(), brokers,
              ImmutableSet.of(), "dev", new Properties())));
      ctx.writeAndFlush(resp);
    });

    MockMemqServer mockServer = new MockMemqServer(port, map);
    mockServer.start();

    // each request will hang for 500 ms before getting a response back
    RequestPacket mdPkt = new RequestPacket(RequestType.PROTOCOL_VERSION, 1,
        RequestType.TOPIC_METADATA, new TopicMetadataRequestPacket("test"));
    RequestPacket mdPkt2 = new RequestPacket(RequestType.PROTOCOL_VERSION, 2,
        RequestType.TOPIC_METADATA, new TopicMetadataRequestPacket("test"));
    RequestPacket mdPkt3 = new RequestPacket(RequestType.PROTOCOL_VERSION, 3,
        RequestType.TOPIC_METADATA, new TopicMetadataRequestPacket("test"));
    NetworkClient client = new NetworkClient();
    Future<ResponsePacket> pktFuture = client
        .send(InetSocketAddress.createUnresolved(LOCALHOST_STRING, port), mdPkt);
    Future<ResponsePacket> pktFuture2 = client
        .send(InetSocketAddress.createUnresolved(LOCALHOST_STRING, port), mdPkt2);
    Future<ResponsePacket> pktFuture3 = client
        .send(InetSocketAddress.createUnresolved(LOCALHOST_STRING, port), mdPkt3);
    Thread.sleep(200);
    assertEquals(3, client.getInflightRequestCount());
    ResponsePacket resp = pktFuture.get();
    ResponsePacket resp2 = pktFuture2.get();
    ResponsePacket resp3 = pktFuture3.get();
    assertEquals(ResponseCodes.OK, resp.getResponseCode());
    assertEquals(mdPkt.getClientRequestId(), resp.getClientRequestId());
    assertEquals(RequestType.TOPIC_METADATA, resp.getRequestType());
    assertEquals(ResponseCodes.OK, resp2.getResponseCode());
    assertEquals(mdPkt2.getClientRequestId(), resp2.getClientRequestId());
    assertEquals(RequestType.TOPIC_METADATA, resp2.getRequestType());
    assertEquals(ResponseCodes.OK, resp3.getResponseCode());
    assertEquals(mdPkt3.getClientRequestId(), resp3.getClientRequestId());
    assertEquals(RequestType.TOPIC_METADATA, resp3.getRequestType());
    mockServer.stop();
  }

  @Test
  public void testSendDifferentServer() throws Exception {
    Map<RequestType, BiConsumer<ChannelHandlerContext, RequestPacket>> map = new HashMap<>();

    map.put(RequestType.TOPIC_METADATA, (ctx, req) -> {
      try {
        Thread.sleep(500);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
      TopicMetadataRequestPacket mdPkt = (TopicMetadataRequestPacket) req.getPayload();
      TopicConfig topicConfig = new TopicConfig("test", "dev");
      TopicAssignment topicAssignment = new TopicAssignment(topicConfig, 100.0);
      Set<Broker> brokers = Collections.singleton(new Broker(LOCALHOST_STRING, (short) port, "n/a",
          "n/a", BrokerType.WRITE, Collections.singleton(topicAssignment)));
      ResponsePacket resp = new ResponsePacket(req.getProtocolVersion(), req.getClientRequestId(),
          req.getRequestType(), ResponseCodes.OK,
          new TopicMetadataResponsePacket(new TopicMetadata(mdPkt.getTopic(), brokers,
              ImmutableSet.of(), "dev", new Properties())));
      ctx.writeAndFlush(resp);
    });

    MockMemqServer mockServer = new MockMemqServer(port, map);
    MockMemqServer mockServer2 = new MockMemqServer(port + 1, map);
    mockServer.start();
    mockServer2.start();

    RequestPacket mdPkt = new RequestPacket(RequestType.PROTOCOL_VERSION, 1,
        RequestType.TOPIC_METADATA, new TopicMetadataRequestPacket("test"));
    RequestPacket mdPkt2 = new RequestPacket(RequestType.PROTOCOL_VERSION, 2,
        RequestType.TOPIC_METADATA, new TopicMetadataRequestPacket("test"));
    NetworkClient client = new NetworkClient();
    Future<ResponsePacket> pktFuture = client
        .send(InetSocketAddress.createUnresolved(LOCALHOST_STRING, port), mdPkt);
    Thread.sleep(200);
    Future<ResponsePacket> pktFuture2 = client
        .send(InetSocketAddress.createUnresolved(LOCALHOST_STRING, port + 1), mdPkt2);

    try {
      pktFuture.get();
      fail("should throw IO exception");
    } catch (ExecutionException ee) {
      assertTrue(ee.getCause() instanceof IOException);
    } catch (Exception e) {
      fail("failed: " + e);
    }

    ResponsePacket resp2 = pktFuture2.get();
    assertEquals(ResponseCodes.OK, resp2.getResponseCode());
    assertEquals(mdPkt2.getClientRequestId(), resp2.getClientRequestId());
    mockServer.stop();
    mockServer2.stop();
  }

  @Test
  public void testAcquireChannelNoConnection() throws Exception {
    NetworkClient client = new NetworkClient();
    try {
      client.acquireChannel(InetSocketAddress.createUnresolved(LOCALHOST_STRING, port));
      fail("should throw connect exception");
    } catch (ExecutionException ee) {
      assertNotNull(ee.getCause());
      assertTrue(ee.getCause() instanceof ConnectException);
    } catch (Exception e) {
      fail(e.getMessage());
    }
  }

  @Test
  public void testAcquireChannelSimple() throws Exception {
    NetworkClient client = new NetworkClient();
    ChannelFuture channel;

    MockMemqServer server = new MockMemqServer(port, Collections.emptyMap());
    server.start();
    try {
      channel = client.acquireChannel(InetSocketAddress.createUnresolved(LOCALHOST_STRING, port));
    } catch (Exception e) {
      fail("failed: " + e);
    }
    server.stop();
  }

  @Test
  public void testAcquireChannelRetries() throws Exception {
    Properties props = new Properties();
    props.setProperty(NetworkClient.CONFIG_CONNECT_TIMEOUT_MS, "500");
    props.setProperty(NetworkClient.CONFIG_INITIAL_RETRY_INTERVAL_MS, "300");
    NetworkClient client = new NetworkClient(props);
    ChannelFuture channel;

    MockMemqServer server = new MockMemqServer(port, Collections.emptyMap());
    scheduler.schedule(() -> {
      try {
        server.start();
      } catch (Exception e) {
        fail("failed: " + e);
      }
    }, 800, TimeUnit.MILLISECONDS);
    try {
      channel = client.acquireChannel(InetSocketAddress.createUnresolved(LOCALHOST_STRING, port));
    } catch (Exception e) {
      fail(e.getMessage());
    }
    server.stop();
  }

  @Test
  public void testAcquireChannelFailAfterRetries() throws Exception {
    Properties props = new Properties();
    props.setProperty(NetworkClient.CONFIG_CONNECT_TIMEOUT_MS, "500");
    props.setProperty(NetworkClient.CONFIG_INITIAL_RETRY_INTERVAL_MS, "300");
    NetworkClient client = new NetworkClient(props);
    ChannelFuture channel;

    MockMemqServer server = new MockMemqServer(port, Collections.emptyMap());
    scheduler.schedule(() -> {
      try {
        server.start();
      } catch (Exception e) {
        fail("failed: " + e);
      }
    }, 3000, TimeUnit.MILLISECONDS);
    try {
      channel = client.acquireChannel(InetSocketAddress.createUnresolved(LOCALHOST_STRING, port));
      fail("should throw connect exception");
    } catch (ExecutionException ee) {
      assertTrue(ee.getCause() instanceof ConnectException);
    } catch (Exception e) {
      fail("failed: " + e);
    }
    server.stop();
  }

  @Test
  public void testStartAndClose() throws Exception {
    NetworkClient client = new NetworkClient();

    MockMemqServer server = new MockMemqServer(port, Collections.emptyMap());
    server.start();
    try {
      client.acquireChannel(InetSocketAddress.createUnresolved(LOCALHOST_STRING, port));
    } catch (Exception e) {
      fail(e.getMessage());
    }
    client.close();
    Thread.sleep(100); // wait some time for the connection to close
    assertNull(client.getConnectFuture());

    RequestPacket mdPkt = new RequestPacket(RequestType.PROTOCOL_VERSION, 1,
        RequestType.TOPIC_METADATA, new TopicMetadataRequestPacket("test"));
    client = new NetworkClient();
    Future<ResponsePacket> pktFuture = client
        .send(InetSocketAddress.createUnresolved(LOCALHOST_STRING, port), mdPkt);

    client.close();
    try {
      pktFuture.get();
      fail("should throw closed connection exception");
    } catch (ExecutionException ee) {
      assertTrue(ee.getCause() instanceof ClosedConnectionException || ee.getCause() instanceof ClientClosedException);
    } catch (Exception e) {
      fail("failed: " + e);
    }

    server.stop();
  }

  @Test
  public void testReset() throws Exception {
    Map<RequestType, BiConsumer<ChannelHandlerContext, RequestPacket>> map = new HashMap<>();

    AtomicInteger count = new AtomicInteger(0);
    map.put(RequestType.TOPIC_METADATA, (ctx, req) -> {
      int currentCount = count.getAndIncrement();
      // first request will hang
      if (currentCount == 0) {
        return;
      }
      TopicMetadataRequestPacket mdPkt = (TopicMetadataRequestPacket) req.getPayload();
      TopicConfig topicConfig = new TopicConfig("test", "dev");
      TopicAssignment topicAssignment = new TopicAssignment(topicConfig, 100.0);
      Set<Broker> brokers = Collections.singleton(new Broker(LOCALHOST_STRING, (short) port, "n/a",
          "n/a", BrokerType.WRITE, Collections.singleton(topicAssignment)));
      ResponsePacket resp = new ResponsePacket(req.getProtocolVersion(), req.getClientRequestId(),
          req.getRequestType(), ResponseCodes.OK,
          new TopicMetadataResponsePacket(new TopicMetadata(mdPkt.getTopic(), brokers,
              ImmutableSet.of(), "dev", new Properties())));
      ctx.writeAndFlush(resp);
    });

    MockMemqServer mockServer = new MockMemqServer(port, map);
    mockServer.start();

    RequestPacket mdPkt = new RequestPacket(RequestType.PROTOCOL_VERSION, 1,
        RequestType.TOPIC_METADATA, new TopicMetadataRequestPacket("test"));

    Properties props = new Properties();
    props.setProperty(NetworkClient.CONFIG_IRRESPONSIVE_TIMEOUT_MS, "5000");
    NetworkClient client = new NetworkClient(props);

    // server no response
    Future<ResponsePacket> pktFuture = client
        .send(InetSocketAddress.createUnresolved(LOCALHOST_STRING, port), mdPkt);
    try {
      Thread.sleep(1000);
      client.reset();
      pktFuture.get(2000, TimeUnit.MILLISECONDS);
      fail("should throw closed connection exception");
    } catch (ExecutionException e) {
      assertTrue(e.getCause() instanceof ClosedConnectionException);
    } catch (Exception e) {
      fail("failed: " + e);
    }

    // happy path
    mdPkt.setClientRequestId(2);
    Future<ResponsePacket> pktFuture2 = client
        .send(InetSocketAddress.createUnresolved(LOCALHOST_STRING, port), mdPkt);
    ResponsePacket resp2 = pktFuture2.get();
    assertEquals(ResponseCodes.OK, resp2.getResponseCode());
    assertEquals(RequestType.TOPIC_METADATA, resp2.getRequestType());
    mockServer.stop();
  }

//  @Test
  // load test
  public void testLoadedRequest() throws Exception {
    Map<RequestType, BiConsumer<ChannelHandlerContext, RequestPacket>> map = new HashMap<>();
    TopicConfig topicConfig = new TopicConfig("test", "dev");
    TopicAssignment topicAssignment = new TopicAssignment(topicConfig, 100.0);
    Set<Broker> brokers = Collections.singleton(new Broker(LOCALHOST_STRING, (short) port, "n/a",
        "n/a", BrokerType.WRITE, Collections.singleton(topicAssignment)));
    TopicMetadataResponsePacket respPkt = new TopicMetadataResponsePacket(
        new TopicMetadata("test", brokers, ImmutableSet.of(), "dev", new Properties()));
    map.put(RequestType.TOPIC_METADATA, (ctx, req) -> {
      ResponsePacket resp = new ResponsePacket(req.getProtocolVersion(), req.getClientRequestId(),
          req.getRequestType(), ResponseCodes.OK, respPkt);
      ctx.writeAndFlush(resp);
    });

    MockMemqServer mockServer = new MockMemqServer(port, map);
    mockServer.start();

    ExecutorService es = Executors.newSingleThreadExecutor();

    NetworkClient client = new NetworkClient();
    long startMs = System.currentTimeMillis();
    BlockingQueue<Future<ResponsePacket>> q = new ArrayBlockingQueue<>(100);
    AtomicBoolean done = new AtomicBoolean(false);

    int NUM_OF_REQUESTS = 10_000_000;

    AtomicInteger count = new AtomicInteger(0);
    Future<?> task = es.submit(() -> {
      while (!done.get() || !q.isEmpty()) {
        try {
          ResponsePacket resp = q.take().get();
          assertEquals(ResponseCodes.OK, resp.getResponseCode());
          assertEquals(RequestType.TOPIC_METADATA, resp.getRequestType());
          if (count.incrementAndGet() % 100_000 == 0) {
            System.out.println("" + count.get() + "/" + NUM_OF_REQUESTS + " , elapsed: "
                + (System.currentTimeMillis() - startMs));
          }
        } catch (Exception e) {
          fail("fail: " + e);
        }
      }
    });

    for (int i = 0; i < NUM_OF_REQUESTS; i++) {
      RequestPacket mdPkt = new RequestPacket(RequestType.PROTOCOL_VERSION, i,
          RequestType.TOPIC_METADATA, new TopicMetadataRequestPacket("test"));
      Future<ResponsePacket> pktFuture = client
          .send(InetSocketAddress.createUnresolved(LOCALHOST_STRING, port), mdPkt);
      q.put(pktFuture);
    }
    done.set(true);
    task.get();
    assertEquals(10_000_000, count.get());

    mockServer.stop();
  }

}
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
package com.pinterest.memq.client.producer2;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.pinterest.memq.client.commons.Compression;
import com.pinterest.memq.client.commons.MemqMessageHeader;
import com.pinterest.memq.client.commons.serde.ByteArraySerializer;
import com.pinterest.memq.client.commons2.MemqCommonClient;
import com.pinterest.memq.client.commons2.MockMemqServer;
import com.pinterest.memq.client.commons2.retry.UniformRetryStrategy;
import com.pinterest.memq.client.producer.MemqWriteResult;
import com.pinterest.memq.commons.protocol.Broker;
import com.pinterest.memq.commons.protocol.RequestPacket;
import com.pinterest.memq.commons.protocol.RequestType;
import com.pinterest.memq.commons.protocol.ResponseCodes;
import com.pinterest.memq.commons.protocol.ResponsePacket;
import com.pinterest.memq.commons.protocol.TopicAssignment;
import com.pinterest.memq.commons.protocol.TopicConfig;
import com.pinterest.memq.commons.protocol.TopicMetadata;
import com.pinterest.memq.commons.protocol.TopicMetadataRequestPacket;
import com.pinterest.memq.commons.protocol.TopicMetadataResponsePacket;
import com.pinterest.memq.commons.protocol.WriteResponsePacket;
import com.pinterest.memq.commons.protocol.Broker.BrokerType;
import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.ImmutableSet;

import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.ChannelHandlerContext;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;

public class TestMemqProducer extends TestMemqProducerBase {

  @Test
  public void testMemoizeProducers() throws Exception {
    AtomicInteger writeCount = new AtomicInteger();
    MockMemqServer mockServer = newSimpleTestServer(writeCount);
    mockServer.start();
    Properties networkProperties = new Properties();
    MemqProducer.Builder<byte[], byte[]> builderTmpl = new MemqProducer.Builder<>();
    builderTmpl.cluster("prototype").topic("test").bootstrapServers(LOCALHOST_STRING + ":" + port)
        .keySerializer(new ByteArraySerializer()).valueSerializer(new ByteArraySerializer())
        .networkProperties(networkProperties);

    MemqProducer.Builder<byte[], byte[]> builder1 = new MemqProducer.Builder<>(builderTmpl);

    MemqProducer<byte[], byte[]> producer1 = builder1.memoize().build();
    MemqProducer<byte[], byte[]> producer2 = builder1.memoize().build();

    assertEquals(producer1, producer2);
    producer1.close();

    MemqProducer<byte[], byte[]> producer3 = builder1.memoize().build();
    assertNotEquals(producer1, producer3);

    MemqProducer.Builder<byte[], byte[]> builder2 = new MemqProducer.Builder<>(builderTmpl);

    builder2.topic("test2");
    MemqProducer<byte[], byte[]> producer4 = builder2.memoize().build();
    assertNotEquals(producer2, producer4);

    mockServer.stop();
  }

  @Test
  public void testProducerInitializationFailure() throws Exception {
    Properties networkProperties = new Properties();
    MemqCommonClient client = new MemqCommonClient("n/a", null, networkProperties);
    MemqProducer.Builder<byte[], byte[]> builderTmpl = new MemqProducer.Builder<>();
    builderTmpl.cluster("prototype").topic("test").bootstrapServers(LOCALHOST_STRING + ":" + port)
        .keySerializer(new ByteArraySerializer()).valueSerializer(new ByteArraySerializer())
        .networkProperties(networkProperties).injectClient(client);
    try {
      builderTmpl.build();
      fail("Should fail since server is offline");
    } catch (Exception e) {
      assertTrue(client.isClosed());
    }
  }

  @Test
  public void testTooLargePayload() throws Exception {
    AtomicInteger writeCount = new AtomicInteger();
    MockMemqServer mockServer = newSimpleTestServer(writeCount);
    mockServer.start();

    Properties networkProperties = new Properties();
    MemqProducer.Builder<byte[], byte[]> builder = new MemqProducer.Builder<>();
    builder.cluster("prototype").topic("test").bootstrapServers(LOCALHOST_STRING + ":" + port)
        .keySerializer(new ByteArraySerializer()).valueSerializer(new ByteArraySerializer())
        .maxPayloadBytes(MemqMessageHeader.getHeaderLength() + RawRecord
            .newInstance(null, null, null,
                new byte["test message that has 32 bytes 1".length() - 1], 0)
            .calculateEncodedLogMessageLength())
        .networkProperties(networkProperties);

    MemqProducer<byte[], byte[]> producer = builder.build();
    Future<MemqWriteResult> r = producer.write(null, "test message that has 32 bytes 1".getBytes());

    producer.flush();

    assertNull(r);
    producer.close();
    assertEquals(producer.getAvailablePermits(), 30);

    mockServer.stop();
  }

  @Test
  public void testSimpleWrite() throws Exception {
    AtomicInteger writeCount = new AtomicInteger();
    MockMemqServer mockServer = newSimpleTestServer(writeCount);
    mockServer.start();

    Properties networkProperties = new Properties();
    MemqProducer.Builder<byte[], byte[]> builder = new MemqProducer.Builder<>();
    builder.cluster("prototype").topic("test").bootstrapServers(LOCALHOST_STRING + ":" + port)
        .keySerializer(new ByteArraySerializer()).valueSerializer(new ByteArraySerializer())
        .networkProperties(networkProperties);

    MemqProducer<byte[], byte[]> producer = builder.build();
    Future<MemqWriteResult> r = producer.write(null, "test".getBytes());

    producer.flush();

    r.get();
    producer.close();

    assertEquals(1, writeCount.get());
    assertEquals(producer.getAvailablePermits(), 30);
    mockServer.stop();
  }

  @Test
  public void testSequentialWrites() throws Exception {
    AtomicInteger writeCount = new AtomicInteger();
    MockMemqServer mockServer = newSimpleTestServer(writeCount);
    mockServer.start();

    Properties networkProperties = new Properties();
    MemqProducer.Builder<byte[], byte[]> builder = new MemqProducer.Builder<>();
    builder.cluster("prototype").topic("test").bootstrapServers(LOCALHOST_STRING + ":" + port)
        .keySerializer(new ByteArraySerializer()).valueSerializer(new ByteArraySerializer())
        .networkProperties(networkProperties);

    MemqProducer<byte[], byte[]> producer = builder.build();

    Future<MemqWriteResult> r0 = producer.write(null, "test1".getBytes());

    Future<MemqWriteResult> r1 = producer.write(null, "test2".getBytes());

    Future<MemqWriteResult> r2 = producer.write(null, "test3".getBytes());

    producer.flush();

    assertEquals(r0, r1);
    assertEquals(r1, r2);

    r0.get();
    producer.close();

    assertEquals(1, writeCount.get());
    assertEquals(producer.getAvailablePermits(), 30);
    mockServer.stop();
  }

  @Test
  public void testConcurrentWrites() throws Exception {
    AtomicInteger writeCount = new AtomicInteger(0);
    MockMemqServer mockServer = newSimpleTestServer(writeCount);
    mockServer.start();

    Properties networkProperties = new Properties();
    MemqProducer.Builder<byte[], byte[]> builder = new MemqProducer.Builder<>();
    builder.cluster("prototype").topic("test").bootstrapServers(LOCALHOST_STRING + ":" + port)
        .keySerializer(new ByteArraySerializer()).valueSerializer(new ByteArraySerializer())
        .networkProperties(networkProperties);

    MemqProducer<byte[], byte[]> producer = builder.build();

    ExecutorService es = Executors.newFixedThreadPool(3);
    Future<?>[] results = new Future[3];
    Future<?>[] tasks = new Future[3];

    for (int i = 0; i < 3; i++) {
      final int idx = i;
      Future<?> task = es.submit(() -> {
        try {
          Future<MemqWriteResult> r = producer.write(null, ("test" + idx).getBytes());
          results[idx] = r;
        } catch (Exception e) {
          fail("Should not fail: " + e);
        }
      });
      tasks[i] = task;
    }

    for (Future<?> f : tasks) {
      f.get();
    }

    assertNotNull(results[0]);
    assertEquals(results[0], results[2]);
    assertEquals(results[0], results[1]);
    producer.flush();

    for (Future<?> f : results) {
      f.get();
    }
    producer.close();

    assertEquals(1, writeCount.get());
    assertEquals(producer.getAvailablePermits(), 30);
    mockServer.stop();
  }

  @Test
  public void testMultipleDispatchedRequests() throws Exception {
    AtomicInteger writeCount = new AtomicInteger(0);
    MockMemqServer mockServer = newSimpleTestServer(writeCount);
    mockServer.start();

    Properties networkProperties = new Properties();
    MemqProducer.Builder<byte[], byte[]> builder = new MemqProducer.Builder<>();
    builder.cluster("prototype").topic("test").bootstrapServers(LOCALHOST_STRING + ":" + port)
        .keySerializer(new ByteArraySerializer()).valueSerializer(new ByteArraySerializer())
        .maxPayloadBytes(MemqMessageHeader.getHeaderLength() + RawRecord
            .newInstance(null, null, null, new byte["test message that has 32 bytes 1".length()], 0)
            .calculateEncodedLogMessageLength())
        .compression(Compression.NONE).networkProperties(networkProperties);

    MemqProducer<byte[], byte[]> producer = builder.build();

    Future<MemqWriteResult> r0 = producer.write(null,
        "test message that has 32 bytes 1".getBytes());

    Future<MemqWriteResult> r1 = producer.write(null,
        "test message that has 32 bytes 2".getBytes());

    Future<MemqWriteResult> r2 = producer.write(null,
        "test message that has 32 bytes 3".getBytes());

    producer.flush();

    assertNotEquals(r0, r1);
    assertNotEquals(r1, r2);
    assertNotEquals(r2, r0);

    r0.get();
    r1.get();
    r2.get();
    producer.close();

    assertEquals(3, writeCount.get());
    assertEquals(producer.getAvailablePermits(), 30);
    mockServer.stop();
  }

  @Test
  public void testMultipleDispatchedRequestsClose() throws Exception {
    AtomicInteger writeCount = new AtomicInteger(0);
    MockMemqServer mockServer = newSimpleTestServer(writeCount);
    mockServer.start();

    Properties networkProperties = new Properties();
    MemqProducer.Builder<byte[], byte[]> builder = new MemqProducer.Builder<>();
    builder.cluster("prototype").topic("test").bootstrapServers(LOCALHOST_STRING + ":" + port)
        .keySerializer(new ByteArraySerializer()).valueSerializer(new ByteArraySerializer())
        .maxPayloadBytes(MemqMessageHeader.getHeaderLength() + RawRecord
            .newInstance(null, null, null, new byte["test message that has 32 bytes 1".length()], 0)
            .calculateEncodedLogMessageLength())
        .compression(Compression.NONE).networkProperties(networkProperties);

    MemqProducer<byte[], byte[]> producer = builder.build();

    Future<MemqWriteResult> r0 = producer.write(null,
        "test message that has 32 bytes 1".getBytes());

    producer.close();

    try {
      Future<MemqWriteResult> r1 = producer.write(null,
          "test message that has 32 bytes 2".getBytes());
      fail("Should throw exception");
    } catch (Exception e) {
      assertTrue(e instanceof IOException);
      assertEquals("Cannot write to topic test when client is closed", e.getMessage());
    }

    try {
      r0.get();
    } catch (Exception e) {
      assertTrue(e instanceof ExecutionException);
      assertTrue(e.getCause() instanceof IllegalStateException);
      assertEquals("Cannot send since client is closed", e.getCause().getMessage());
    }
    assertEquals(producer.getAvailablePermits(), 30);

    mockServer.stop();
  }

  @Test
  public void testRedirect() throws Exception {
    AtomicInteger writeCount = new AtomicInteger(0);
    AtomicInteger redirectCount = new AtomicInteger(0);
    Map<RequestType, BiConsumer<ChannelHandlerContext, RequestPacket>> map = new HashMap<>();

    setupSimpleTestServerTopicMetadataHandler(map);
    map.put(RequestType.WRITE, (ctx, req) -> {
      ResponsePacket resp;
      int currentCount = writeCount.getAndIncrement();
      if (currentCount % 2 == 0 || currentCount >= 4) { // redirect first request
        redirectCount.getAndIncrement();
        resp = new ResponsePacket(req.getProtocolVersion(), req.getClientRequestId(),
            req.getRequestType(), ResponseCodes.REDIRECT, new WriteResponsePacket());
      } else {
        resp = new ResponsePacket(req.getProtocolVersion(), req.getClientRequestId(),
            req.getRequestType(), ResponseCodes.OK, new WriteResponsePacket());
      }
      ctx.writeAndFlush(resp);
    });

    MockMemqServer mockServer = new MockMemqServer(port, map);
    mockServer.start();

    Properties networkProperties = new Properties();
    MemqProducer<byte[], byte[]> producer = new MemqProducer.Builder<byte[], byte[]>()
        .cluster("prototype").topic("test").bootstrapServers(LOCALHOST_STRING + ":" + port)
        .keySerializer(new ByteArraySerializer()).valueSerializer(new ByteArraySerializer())
        .maxPayloadBytes(MemqMessageHeader.getHeaderLength() + RawRecord
            .newInstance(null, null, null, new byte["test message that has 32 bytes 1".length()], 0)
            .calculateEncodedLogMessageLength())
        .compression(Compression.NONE).networkProperties(networkProperties).build();

    Future<MemqWriteResult> r0 = producer.write(null,
        "test message that has 32 bytes 1".getBytes());
    r0.get();

    Future<MemqWriteResult> r1 = producer.write(null,
        "test message that has 32 bytes 2".getBytes());
    r1.get();

    Future<MemqWriteResult> r2 = producer.write(null,
        "test message that has 32 bytes 3".getBytes());

    producer.flush();

    try {
      r2.get();
      fail("Should fail since more than 1 redirection");
    } catch (ExecutionException ee) {
      assertEquals("Write request failed after multiple attempts", ee.getCause().getMessage());
    } catch (Exception e) {
      fail("should throw execution exception");
    }
    producer.close();

    assertEquals(2 + 2 + 3, writeCount.get());
    assertEquals(1 + 1 + 3, redirectCount.get());
    assertEquals(producer.getAvailablePermits(), 30);
    mockServer.stop();
  }

  @Test
  public void testRedirectToDifferentServer() throws Exception {
    Map<RequestType, BiConsumer<ChannelHandlerContext, RequestPacket>> map1 = new HashMap<>();

    AtomicInteger metadataCount1 = new AtomicInteger();
    map1.put(RequestType.TOPIC_METADATA, (ctx, req) -> {
      metadataCount1.incrementAndGet();
      TopicMetadataRequestPacket mdPkt = (TopicMetadataRequestPacket) req.getPayload();
      TopicConfig topicConfig = new TopicConfig("test", "dev");
      TopicAssignment topicAssignment = new TopicAssignment(topicConfig, 100.0);
      Set<Broker> brokers = Collections.singleton(new Broker(LOCALHOST_STRING, (short) (port + 1),
          "n/a", "n/a", BrokerType.WRITE, Collections.singleton(topicAssignment)));
      ResponsePacket resp = new ResponsePacket(req.getProtocolVersion(), req.getClientRequestId(),
          req.getRequestType(), ResponseCodes.OK,
          new TopicMetadataResponsePacket(new TopicMetadata(mdPkt.getTopic(), brokers,
              ImmutableSet.of(), "dev", new Properties())));
      ctx.writeAndFlush(resp);
    });
    AtomicInteger writeCount1 = new AtomicInteger();
    map1.put(RequestType.WRITE, (ctx, req) -> {
      writeCount1.getAndIncrement();

      ctx.writeAndFlush(new ResponsePacket(req.getProtocolVersion(), req.getClientRequestId(),
          req.getRequestType(), ResponseCodes.REDIRECT, new WriteResponsePacket()));
    });

    Map<RequestType, BiConsumer<ChannelHandlerContext, RequestPacket>> map2 = new HashMap<>();

    map2.put(RequestType.TOPIC_METADATA, (ctx, req) -> {
      TopicMetadataRequestPacket mdPkt = (TopicMetadataRequestPacket) req.getPayload();
      TopicConfig topicConfig = new TopicConfig("test", "dev");
      TopicAssignment topicAssignment = new TopicAssignment(topicConfig, 100.0);
      Set<Broker> brokers = Collections.singleton(new Broker(LOCALHOST_STRING, (short) (port + 1),
          "n/a", "n/a", BrokerType.WRITE, Collections.singleton(topicAssignment)));
      ResponsePacket resp = new ResponsePacket(req.getProtocolVersion(), req.getClientRequestId(),
          req.getRequestType(), ResponseCodes.OK,
          new TopicMetadataResponsePacket(new TopicMetadata(mdPkt.getTopic(), brokers,
              ImmutableSet.of(), "dev", new Properties())));
      ctx.writeAndFlush(resp);
    });
    AtomicInteger writeCount2 = new AtomicInteger();
    map2.put(RequestType.WRITE, (ctx, req) -> {
      writeCount2.getAndIncrement();

      ctx.writeAndFlush(new ResponsePacket(req.getProtocolVersion(), req.getClientRequestId(),
          req.getRequestType(), ResponseCodes.OK, new WriteResponsePacket()));
    });

    MockMemqServer mockserver1 = new MockMemqServer(port, map1);
    mockserver1.start();
    MockMemqServer mockserver2 = new MockMemqServer(port + 1, map2);
    mockserver2.start();

    Properties networkProperties = new Properties();
    MemqProducer<byte[], byte[]> producer = new MemqProducer.Builder<byte[], byte[]>()
        .cluster("prototype").topic("test").bootstrapServers(LOCALHOST_STRING + ":" + port)
        .keySerializer(new ByteArraySerializer()).valueSerializer(new ByteArraySerializer())
        .maxPayloadBytes(MemqMessageHeader.getHeaderLength() + RawRecord
            .newInstance(null, null, null, new byte["test message that has 32 bytes 1".length()], 0)
            .calculateEncodedLogMessageLength())
        .compression(Compression.NONE).networkProperties(networkProperties).build();

    Future<MemqWriteResult> r0 = producer.write(null,
        "test message that has 32 bytes 1".getBytes());

    Future<MemqWriteResult> r1 = producer.write(null,
        "test message that has 32 bytes 2".getBytes());

    Future<MemqWriteResult> r2 = producer.write(null,
        "test message that has 32 bytes 3".getBytes());

    producer.flush();

    try {
      r0.get();
      r1.get();
      r2.get();
    } catch (Exception e) {
      fail("should all pass");
    }
    producer.close();

    assertEquals(1, metadataCount1.get());
    assertEquals(0, writeCount1.get());
    assertEquals(3, writeCount2.get());
    assertEquals(producer.getAvailablePermits(), 30);
    mockserver1.stop();
    mockserver2.stop();
  }

  @Test
  public void testDisconnectionRetryAndTimeout() throws Exception {
    AtomicInteger writeCount = new AtomicInteger(0);
    Map<RequestType, BiConsumer<ChannelHandlerContext, RequestPacket>> map = new HashMap<>();

    setupSimpleTestServerTopicMetadataHandler(map);
    map.put(RequestType.WRITE, (ctx, req) -> {
      int current = writeCount.getAndIncrement();
      if ((current >= 1 && current < 4) || current >= 5) {
        ctx.close();
        return;
      }
      ResponsePacket resp = new ResponsePacket(req.getProtocolVersion(), req.getClientRequestId(),
          req.getRequestType(), ResponseCodes.OK, new WriteResponsePacket());
      ctx.writeAndFlush(resp);
    });

    MockMemqServer mockServer = new MockMemqServer(port, map);
    mockServer.start();

    Properties networkProperties = new Properties();
    MemqProducer<byte[], byte[]> producer = new MemqProducer.Builder<byte[], byte[]>()
        .cluster("prototype").topic("test").bootstrapServers(LOCALHOST_STRING + ":" + port)
        .keySerializer(new ByteArraySerializer()).valueSerializer(new ByteArraySerializer())
        .retryStrategy(new UniformRetryStrategy())
        .maxPayloadBytes(MemqMessageHeader.getHeaderLength() + RawRecord
            .newInstance(null, null, null, new byte["test message that has 32 bytes 1".length()], 0)
            .calculateEncodedLogMessageLength())
        .compression(Compression.NONE).networkProperties(networkProperties).build();

    Future<MemqWriteResult> r0 = producer.write(null,
        "test message that has 32 bytes 1".getBytes());
    r0.get();

    Future<MemqWriteResult> r1 = producer.write(null,
        "test message that has 32 bytes 2".getBytes());
    assertNotEquals(r0, r1);

    r1.get();

    Future<MemqWriteResult> r2 = producer.write(null,
        "test message that has 32 bytes 3".getBytes());

    producer.flush();

    assertNotEquals(r1, r2);
    assertNotEquals(r2, r0);

    try {
      r2.get();
      fail("Should timeout");
    } catch (ExecutionException ee) {
      assertTrue(ee.getCause() instanceof TimeoutException);
    } catch (Exception e) {
      fail("Should throw timeout exception wrapped in execution exception");
    }
    assertEquals(producer.getAvailablePermits(), 30);
    producer.close();
    mockServer.stop();
  }

  @Test
  public void testAlignedMemoryLoad() throws Exception {
    AtomicInteger requestCount = new AtomicInteger(0);
    AtomicInteger redirectCount = new AtomicInteger(0);
    AtomicInteger closeCount = new AtomicInteger(0);
    AtomicInteger successCount = new AtomicInteger(0);
    Map<RequestType, BiConsumer<ChannelHandlerContext, RequestPacket>> map = new HashMap<>();

    setupSimpleTestServerTopicMetadataHandler(map);
    map.put(RequestType.WRITE, (ctx, req) -> {
      ResponsePacket resp;
      int current = requestCount.getAndIncrement();
      if (current % 10000 == 9999) {
        closeCount.getAndIncrement();
        ctx.close();
        return;
      } else if (current % 1000 == 999) {
        redirectCount.getAndIncrement();
        resp = new ResponsePacket(req.getProtocolVersion(), req.getClientRequestId(),
            req.getRequestType(), ResponseCodes.REDIRECT, new WriteResponsePacket());
      } else {
        successCount.getAndIncrement();
        resp = new ResponsePacket(req.getProtocolVersion(), req.getClientRequestId(),
            req.getRequestType(), ResponseCodes.OK, new WriteResponsePacket());
      }
      ctx.writeAndFlush(resp);
    });

    MockMemqServer mockServer = new MockMemqServer(port, map);
    mockServer.start();

    MemqProducer.Builder<byte[], byte[]> builder = new MemqProducer.Builder<>();
    Properties networkProperties = new Properties();
    byte[] sampleValue = new byte[1024];
    builder.cluster("prototype").topic("test").bootstrapServers(LOCALHOST_STRING + ":" + port)
        .keySerializer(new ByteArraySerializer()).valueSerializer(new ByteArraySerializer())
        .maxPayloadBytes(MemqMessageHeader.getHeaderLength() + RawRecord
            .newInstance(null, null, null, sampleValue, 0).calculateEncodedLogMessageLength())
        .compression(Compression.NONE).networkProperties(networkProperties)
        .metricRegistry(new MetricRegistry());

    MemqProducer<byte[], byte[]> producer = builder.build();

    ExecutorService es = Executors.newSingleThreadExecutor(r -> {
      Thread t = new Thread(r);
      t.setName("test");
      return t;
    });
    int NUM_OF_WRITES = 1_000_000;
    AtomicBoolean done = new AtomicBoolean(false);
    final BlockingQueue<Future<MemqWriteResult>> results = new ArrayBlockingQueue<>(10);
    Set<Future<MemqWriteResult>> resultSet = new HashSet<>();
    AtomicInteger resultCount = new AtomicInteger(0);

    Future<?> consumptionTask = es.submit(() -> {
      while (!results.isEmpty() || !done.get()) {
        try {
          results.take().get();
          resultCount.incrementAndGet();
        } catch (Exception e) {
          System.err.println(e);
        }
      }
      if (!done.get()) {
        fail("Not done yet");
      } else if (!results.isEmpty()) {
        fail("results are not empty");
      }
    });

    for (int i = 0; i < NUM_OF_WRITES; i++) {
      if (i % 1_000 == 0) {
        System.out.println("[" + i + "/" + NUM_OF_WRITES + "]" + " inflight requests: "
            + results.size() + " netty off-heap usage (KB): "
            + PooledByteBufAllocator.DEFAULT.metric().usedDirectMemory() / 1024
            + " netty heap usage (KB): "
            + PooledByteBufAllocator.DEFAULT.metric().usedHeapMemory() / 1024);
      }
      byte[] value = new byte[sampleValue.length];
      ThreadLocalRandom.current().nextBytes(value);
      Future<MemqWriteResult> r = producer.write(null, value);
      if (resultSet.add(r)) {
        results.put(r);
      }
    }
    producer.flush();
    done.set(true);

    consumptionTask.get();

    producer.close();

    assertEquals(
        producer.getMetricRegistry().getCounters().get("requests.success.count").getCount(),
        resultCount.get());
    assertEquals(resultSet.size(), resultCount.get());
    assertEquals(producer.getAvailablePermits(), 30);
    mockServer.stop();
  }

  @Test
  public void testNonAlignedMemoryLoad() throws Exception {
    AtomicInteger requestCount = new AtomicInteger(0);
    AtomicInteger redirectCount = new AtomicInteger(0);
    AtomicInteger closeCount = new AtomicInteger(0);
    AtomicInteger successCount = new AtomicInteger(0);
    Map<RequestType, BiConsumer<ChannelHandlerContext, RequestPacket>> map = new HashMap<>();

    setupSimpleTestServerTopicMetadataHandler(map);
    map.put(RequestType.WRITE, (ctx, req) -> {
      ResponsePacket resp;
      int current = requestCount.getAndIncrement();
      if (current % 1000 == 999) {
        closeCount.getAndIncrement();
        ctx.close();
        return;
      } else if (current % 100 == 99) {
        redirectCount.getAndIncrement();
        resp = new ResponsePacket(req.getProtocolVersion(), req.getClientRequestId(),
            req.getRequestType(), ResponseCodes.REDIRECT, new WriteResponsePacket());
      } else {
        successCount.getAndIncrement();
        resp = new ResponsePacket(req.getProtocolVersion(), req.getClientRequestId(),
            req.getRequestType(), ResponseCodes.OK, new WriteResponsePacket());
      }
      ctx.writeAndFlush(resp);
    });

    MockMemqServer mockServer = new MockMemqServer(port, map);
    mockServer.start();

    MemqProducer.Builder<byte[], byte[]> builder = new MemqProducer.Builder<>();
    Properties networkProperties = new Properties();
    byte[] sampleValue = new byte[4 * 1024];
    builder.cluster("prototype").topic("test").bootstrapServers(LOCALHOST_STRING + ":" + port)
        .keySerializer(new ByteArraySerializer()).valueSerializer(new ByteArraySerializer())
        .maxPayloadBytes(4 * 1024 * 1024).compression(Compression.NONE)
        .networkProperties(networkProperties).metricRegistry(new MetricRegistry());

    MemqProducer<byte[], byte[]> producer = builder.build();

    ExecutorService es = Executors.newSingleThreadExecutor(r -> {
      Thread t = new Thread(r);
      t.setName("test");
      return t;
    });
    int NUM_OF_WRITES = 1_000_000;
    AtomicBoolean done = new AtomicBoolean(false);
    final BlockingQueue<Future<MemqWriteResult>> results = new ArrayBlockingQueue<>(10);
    Set<Future<MemqWriteResult>> resultSet = new HashSet<>();
    AtomicInteger resultCount = new AtomicInteger(0);

    Future<?> consumptionTask = es.submit(() -> {
      while (!results.isEmpty() || !done.get()) {
        try {
          results.take().get();
        } catch (Exception e) {
          throw new RuntimeException(e.getCause());
        }
      }
      if (!done.get()) {
        fail("Not done yet");
      } else if (!results.isEmpty()) {
        fail("results are not empty");
      }
    });

    for (int i = 0; i < NUM_OF_WRITES; i++) {
      if (i % 10_000 == 0) {
        System.out.println("[" + i + "/" + NUM_OF_WRITES + "]" + " inflight requests: "
            + results.size() + ", netty off-heap usage (KB): "
            + PooledByteBufAllocator.DEFAULT.metric().usedDirectMemory() / 1024
            + ", netty heap usage (KB): "
            + PooledByteBufAllocator.DEFAULT.metric().usedHeapMemory() / 1024);
      }
      byte[] value = new byte[sampleValue.length];
      ThreadLocalRandom.current().nextBytes(value);
      Future<MemqWriteResult> r = producer.write(null, value);
      if (resultSet.add(r)) {
        resultCount.incrementAndGet();
        results.put(r);
      }
    }
    producer.flush();
    done.set(true);

    consumptionTask.get();

    producer.close();

    assertEquals(
        producer.getMetricRegistry().getCounters().get("requests.success.count").getCount(),
        successCount.get());
    assertEquals(resultSet.size(), resultCount.get());
    assertEquals(producer.getAvailablePermits(), 30);
    mockServer.stop();
  }
}
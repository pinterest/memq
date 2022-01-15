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

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import io.netty.buffer.ByteBufInputStream;
import io.netty.channel.Channel;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.Test;

import com.codahale.metrics.MetricRegistry;
import com.pinterest.memq.client.commons.Compression;
import com.pinterest.memq.client.commons.MemqMessageHeader;
import com.pinterest.memq.client.commons.serde.ByteArraySerializer;
import com.pinterest.memq.client.producer.MemqWriteResult;
import com.pinterest.memq.client.producer.TaskRequest;
import com.pinterest.memq.client.producer.netty.MemqNettyProducer;
import com.pinterest.memq.client.producer.netty.MemqNettyRequest;
import com.pinterest.memq.commons.protocol.RequestPacket;
import com.pinterest.memq.commons.protocol.RequestType;
import com.pinterest.memq.commons.protocol.TopicConfig;
import com.pinterest.memq.commons.protocol.WriteRequestPacket;
import com.pinterest.memq.commons.storage.DelayedDevNullStorageHandler;
import com.pinterest.memq.commons.storage.StorageHandler;
import com.pinterest.memq.commons.storage.WriteFailedException;
import com.pinterest.memq.core.MemqManager;
import com.pinterest.memq.core.clustering.MemqGovernor;
import com.pinterest.memq.core.commons.Message;
import com.pinterest.memq.core.commons.MessageBufferInputStream;
import com.pinterest.memq.core.config.EnvironmentProvider;
import com.pinterest.memq.core.config.MemqConfig;
import com.pinterest.memq.core.config.NettyServerConfig;
import com.pinterest.memq.core.integration.TestEnvironmentProvider;
import com.pinterest.memq.core.rpc.MemqNettyServer;
import com.pinterest.memq.core.rpc.TestAuditor;
import com.pinterest.memq.core.utils.DaemonThreadFactory;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

public class TestBucketingTopicProcessor {
  @Test
  public void testFullBatches() throws Exception {
    MemqConfig configuration = new MemqConfig();
    NettyServerConfig nettyServerConfig = new NettyServerConfig();
    nettyServerConfig.setPort((short) 23434);
    configuration.setNettyServerConfig(nettyServerConfig);
    TopicConfig topicConfig = new TopicConfig("test", "delayeddevnull");
    topicConfig.setEnableBucketing2Processor(true);
    topicConfig.setOutputParallelism(60);
    topicConfig.setBufferSize(1024 * 1024);
    topicConfig.setBatchSizeMB(2);
    topicConfig.setRingBufferSize(4);
    topicConfig.setTickFrequencyMillis(10);
    topicConfig.setBatchMilliSeconds(2000);
    Properties outputHandlerConfig = new Properties();
    outputHandlerConfig.setProperty("delay.min.millis", "5000");
    outputHandlerConfig.setProperty("delay.max.millis", "6000");
    topicConfig.setStorageHandlerConfig(outputHandlerConfig);
    configuration.setTopicConfig(new TopicConfig[] { topicConfig });
    MemqManager memqManager = new MemqManager(null, configuration, new HashMap<>());
    memqManager.init();

    EnvironmentProvider provider = new TestEnvironmentProvider();
    MemqGovernor governor = new MemqGovernor(memqManager, configuration, provider);
    MemqNettyServer server = new MemqNettyServer(configuration, memqManager, governor,
        new HashMap<>(), null);
    server.initialize();

    Properties auditConfigs = new Properties();
    auditConfigs.setProperty("class", "com.pinterest.memq.core.rpc.TestAuditor");
    // run tests
    MemqNettyProducer<byte[], byte[]> producer = new MemqNettyProducer<>("testcluster",
        new InetSocketAddress("localhost", 23434), "test", 10, 1000000, Compression.NONE, false, 10,
        60_000, "local", 60_000, auditConfigs, null);
    producer.setKeySerializer(new ByteArraySerializer());
    producer.setValueSerializer(new ByteArraySerializer());
    byte[] value = UUID.randomUUID().toString().getBytes();
    Set<Future<MemqWriteResult>> futures = new LinkedHashSet<>();
    for (int i = 0; i < 1_000_000; i++) {
      Future<MemqWriteResult> writeToTopic = producer.writeToTopic(null, value);
      futures.add(writeToTopic);
    }
    producer.finalizeRequest();
    for (Future<MemqWriteResult> future : futures) {
      future.get(30, TimeUnit.SECONDS);
    }
    System.out.println("Completed all writes");
    Thread.sleep(500);
    producer.close();

    // server should have written 55 batches
    assertEquals(futures.size(), DelayedDevNullStorageHandler.getCounter());
    assertEquals(futures.size(), TestAuditor.getAuditMessageList().size());

    assertEquals(MemqNettyRequest.getByteCounter(), DelayedDevNullStorageHandler.getByteCounter());
    assertEquals(MemqNettyRequest.getByteCounter(),
        DelayedDevNullStorageHandler.getInputStreamCounter());
    assertEquals(MemqNettyRequest.getAckedByteCounter(), MemqNettyRequest.getByteCounter());

    server.getChildGroup().shutdownGracefully().sync();
    server.getParentGroup().shutdownGracefully().sync();
    server.getServerChannelFuture().channel().closeFuture().sync();
  }

  @Test
  public void testTimeBasedBatch() throws InterruptedException {
    MetricRegistry registry = new MetricRegistry();
    TopicConfig topicConfig = new TopicConfig();
    topicConfig.setTopic("test");
    topicConfig.setBatchMilliSeconds(100);
    topicConfig.setBatchSizeMB(10);
    topicConfig.setBufferSize(1024 * 1024);
    topicConfig.setRingBufferSize(100);
    final AtomicInteger totalBytes = new AtomicInteger();
    StorageHandler outputHandler = new StorageHandler() {

      @Override
      public void writeOutput(int sizeInBytes,
                              int checksum,
                              List<Message> messages) throws WriteFailedException {
        MessageBufferInputStream mis = new MessageBufferInputStream(messages, null);
        @SuppressWarnings("unused")
        int b;
        try {
          int counter = 0;
          while ((b = mis.read()) != -1) {
            counter++;
          }
          totalBytes.addAndGet(counter);
          System.out.println(counter + " " + sizeInBytes + " " + messages.size());
        } catch (IOException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
        }
      }

      @Override
      public String getReadUrl() {
        return null;
      }

    };
    ScheduledExecutorService timerService = Executors.newScheduledThreadPool(1,
        new DaemonThreadFactory());
    BucketingTopicProcessor processor = new BucketingTopicProcessor(registry, topicConfig,
        outputHandler, timerService, null);
    int bytesWritten = 0;
    try {
      for (int i = 0; i < 20; i++) {
        StringBuilder builder = new StringBuilder();
        for (int k = 0; k < ThreadLocalRandom.current().nextInt(1,20); k++) {
          builder.append(UUID.randomUUID().toString());
        }
        byte[] bytes = builder.toString().getBytes();
        ByteBuf buf = Unpooled.wrappedBuffer(bytes);
        WriteRequestPacket payload = new WriteRequestPacket(true, "test".getBytes(), false, 0, buf);
        RequestPacket packet = new RequestPacket(RequestType.PROTOCOL_VERSION,
            ThreadLocalRandom.current().nextLong(), RequestType.WRITE, payload);
        processor.write(packet, payload, getTestContext());
        bytesWritten += bytes.length;
        Thread.sleep(500);
      }
    } catch (Exception e) {
    }
    processor.stopAndAwait();
    assertEquals(0, registry.counter("batching.sizedbasedbatch").getCount());
    assertEquals(20, registry.counter("batching.timebasedbatch").getCount());
    assertEquals(0, registry.counter("batching.countbasedbatch").getCount());
    assertEquals(bytesWritten, totalBytes.get());
  }

  @Test
  public void testCountBasedBatch() throws InterruptedException {
    MetricRegistry registry = new MetricRegistry();
    TopicConfig topicConfig = new TopicConfig();
    topicConfig.setTopic("test");
    topicConfig.setBatchMilliSeconds(100);
    topicConfig.setBatchSizeMB(10);
    topicConfig.setMaxDispatchCount(1);
    topicConfig.setBufferSize(1024 * 1024);
    topicConfig.setRingBufferSize(100);
    topicConfig.setEnableServerHeaderValidation(true);
    final AtomicInteger totalBytes = new AtomicInteger();
    StorageHandler outputHandler = new StorageHandler() {

      @Override
      public void writeOutput(int sizeInBytes,
                              int checksum,
                              List<Message> messages) throws WriteFailedException { @SuppressWarnings("unused")
        int b;
        try {
          int counter = 0;
          for (Message m : messages) {
            ByteBufInputStream bbis = new ByteBufInputStream(m.getBuf());
            while ((b = bbis.read()) != -1) {
              counter++;
            }
            bbis.close();
          }
          totalBytes.addAndGet(counter);
          System.out.println(counter + " " + sizeInBytes + " " + messages.size());
        } catch (IOException e) {
          e.printStackTrace();
        }
      }

      @Override
      public String getReadUrl() {
        return null;
      }

    };
    ScheduledExecutorService timerService = Executors.newScheduledThreadPool(1,
        new DaemonThreadFactory());
    BucketingTopicProcessor processor = new BucketingTopicProcessor(registry, topicConfig,
        outputHandler, timerService, null);
    int bytesWritten = 0;
    try {
      for (int i = 0; i < 20; i++) {
        StringBuilder builder = new StringBuilder();
        for (int k = 0; k < ThreadLocalRandom.current().nextInt(1,20); k++) {
          builder.append(UUID.randomUUID().toString());
        }
        builder.append((char)0xff);
        byte[] bytes = builder.toString().getBytes();
        TaskRequest tr = new TestTaskRequest("test", i, 20000);
        MemqMessageHeader header = new MemqMessageHeader(tr);
        ByteBuf payloadBytes = Unpooled.buffer();
        payloadBytes.writerIndex(MemqMessageHeader.getHeaderLength());
        payloadBytes.writeBytes(bytes);
        header.writeHeader(payloadBytes);
        WriteRequestPacket payload = new WriteRequestPacket(true, "test".getBytes(), false, 0,
            payloadBytes);
        RequestPacket packet = new RequestPacket(RequestType.PROTOCOL_VERSION,
            ThreadLocalRandom.current().nextLong(), RequestType.WRITE, payload);

        processor.write(packet, payload, getTestContext());
        bytesWritten += payloadBytes.readableBytes();
        Thread.sleep(200);
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
    processor.stopAndAwait();
    assertEquals(0, registry.counter("batching.sizedbasedbatch").getCount());
    assertEquals(0, registry.counter("batching.timebasedbatch").getCount());
    assertEquals(20, registry.counter("batching.countbasedbatch").getCount());
    assertEquals(bytesWritten, totalBytes.get());
  }

  @Test
  public void testWriteCompleteness() throws InterruptedException {
    MetricRegistry registry = new MetricRegistry();
    TopicConfig topicConfig = new TopicConfig();
    topicConfig.setTopic("test");
    topicConfig.setBatchMilliSeconds(10000);
    topicConfig.setBatchSizeMB(10);
    topicConfig.setBufferSize(1024 * 1024);
    topicConfig.setRingBufferSize(100);
    final AtomicInteger totalBytes = new AtomicInteger();
    StorageHandler outputHandler = new StorageHandler() {

      @Override
      public void writeOutput(int sizeInBytes,
                              int checksum,
                              List<Message> messages) throws WriteFailedException {
        MessageBufferInputStream mis = new MessageBufferInputStream(messages, null);
        @SuppressWarnings("unused")
        int b;
        try {
          int counter = 0;
          while ((b = mis.read()) != -1) {
            counter++;
          }
          totalBytes.addAndGet(counter);
          System.out.println(counter + " " + sizeInBytes + " " + messages.size());
        } catch (IOException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
        }
      }

      @Override
      public String getReadUrl() {
        return null;
      }

    };
    ScheduledExecutorService timerService = Executors.newScheduledThreadPool(1,
        new DaemonThreadFactory());
    BucketingTopicProcessor processor = new BucketingTopicProcessor(registry, topicConfig,
        outputHandler, timerService, null);
    int bytesWritten = 0;
    try {
      for (int i = 0; i < 20; i++) {
        StringBuilder builder = new StringBuilder();
        for (int k = 0; k < ThreadLocalRandom.current().nextInt(1,20); k++) {
          builder.append(UUID.randomUUID().toString());
        }
        byte[] bytes = builder.toString().getBytes();
        ByteBuf buf = Unpooled.wrappedBuffer(bytes);
        WriteRequestPacket payload = new WriteRequestPacket(true, "test".getBytes(), false, 0, buf);
        RequestPacket packet = new RequestPacket(RequestType.PROTOCOL_VERSION,
            ThreadLocalRandom.current().nextLong(), RequestType.WRITE, payload);
        processor.write(packet, payload, getTestContext());
        bytesWritten += bytes.length;
      }
      processor.forceDispatch();
    } catch (Exception e) {
    }
    processor.stopAndAwait();
    assertEquals(bytesWritten, totalBytes.get());
  }

  @Test
  public void testHeaderValidation() throws InterruptedException {
    MetricRegistry registry = new MetricRegistry();
    TopicConfig topicConfig = new TopicConfig();
    topicConfig.setTopic("test");
    topicConfig.setBatchMilliSeconds(100);
    topicConfig.setBatchSizeMB(10);
    topicConfig.setMaxDispatchCount(1);
    topicConfig.setBufferSize(1024 * 1024);
    topicConfig.setRingBufferSize(100);
    topicConfig.setEnableServerHeaderValidation(true);
    final AtomicInteger totalBytes = new AtomicInteger();
    StorageHandler outputHandler = new StorageHandler() {

      @Override
      public void writeOutput(int sizeInBytes,
                              int checksum,
                              List<Message> messages) throws WriteFailedException {
        MessageBufferInputStream mis = new MessageBufferInputStream(messages, null);
        @SuppressWarnings("unused")
        int b;
        try {
          int counter = 0;
          while ((b = mis.read()) != -1) {
            counter++;
          }
          totalBytes.addAndGet(counter);
          System.out.println(counter + " " + sizeInBytes + " " + messages.size());
        } catch (IOException e) {
          e.printStackTrace();
        }
      }

      @Override
      public String getReadUrl() {
        return null;
      }

    };
    ScheduledExecutorService timerService = Executors.newScheduledThreadPool(1,
        new DaemonThreadFactory());
    BucketingTopicProcessor processor = new BucketingTopicProcessor(registry, topicConfig,
        outputHandler, timerService, null);
    int bytesWritten = 0;
    try {
      for (int i = 0; i < 20; i++) {
        StringBuilder builder = new StringBuilder();
        for (int k = 0; k < ThreadLocalRandom.current().nextInt(1,20); k++) {
          builder.append(UUID.randomUUID().toString());
        }
        byte[] bytes = builder.toString().getBytes();
        TaskRequest tr = new TestTaskRequest("test", i, 20000);
        MemqMessageHeader header = new MemqMessageHeader(tr);

        ByteBuf payloadBytes = Unpooled.buffer();
        for (int j = 0; j < MemqMessageHeader.getHeaderLength(); j++) {
          payloadBytes.writeByte(0x7F);
        }
        payloadBytes.writeBytes(bytes);
        WriteRequestPacket payload = new WriteRequestPacket(true, "test".getBytes(), false, 0,
            payloadBytes);
        RequestPacket packet = new RequestPacket(RequestType.PROTOCOL_VERSION,
            ThreadLocalRandom.current().nextLong(), RequestType.WRITE, payload);
        processor.write(packet, payload, getTestContext());
        bytesWritten += payloadBytes.readableBytes();
        Thread.sleep(500);
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
    processor.stopAndAwait();
    long invalidCount = registry.counter("tp.message.invalid.header.message_length_too_large")
        .getCount()
        + registry.counter("tp.message.invalid.header.message_length_negative").getCount()
        + registry.counter("tp.message.invalid.header.exception").getCount();
    assertEquals(20L, invalidCount);
    assertEquals(bytesWritten, totalBytes.get());
  }

  private ChannelHandlerContext getTestContext() {
    Channel ch = new EmbeddedChannel();
    ch.pipeline().addLast(new ChannelDuplexHandler());
    return ch.pipeline().firstContext();
  }

  private static class TestTaskRequest extends TaskRequest {

    public TestTaskRequest(String topicName,
                           long currentRequestId,
                           int maxPayLoadBytes) throws IOException {
      super(topicName, currentRequestId, Compression.NONE, null, false, maxPayLoadBytes, 10000,
          null, 10000);
    }

    @Override
    protected MemqWriteResult runRequest() throws Exception {
      return null;
    }

    @Override
    public long getEpoch() {
      return 0;
    }
  }

}

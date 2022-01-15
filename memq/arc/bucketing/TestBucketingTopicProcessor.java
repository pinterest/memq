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
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;

import com.codahale.metrics.MetricRegistry;
import com.pinterest.memq.commons.protocol.RequestPacket;
import com.pinterest.memq.commons.protocol.RequestType;
import com.pinterest.memq.commons.protocol.TopicConfig;
import com.pinterest.memq.commons.protocol.WriteRequestPacket;
import com.pinterest.memq.commons.storage.WriteFailedException;
import com.pinterest.memq.commons.storage.StorageHandler;
import com.pinterest.memq.core.commons.Message;
import com.pinterest.memq.core.commons.MessageBufferInputStream;
import com.pinterest.memq.core.processing.TopicProcessor;
import com.pinterest.memq.core.utils.DaemonThreadFactory;

import io.netty.buffer.Unpooled;

public class TestBucketingTopicProcessor {

  @Test
  public void testConcurrencyIssue() throws Exception {
    MetricRegistry registry = new MetricRegistry();
    TopicConfig topicConfig = new TopicConfig();
    topicConfig.setTopic("test");
    topicConfig.setBatchMilliSeconds(10000);
    topicConfig.setBatchSizeMB(10);
    topicConfig.setBufferSize(1024 * 1024);
    topicConfig.setRingBufferSize(50);
    final AtomicInteger byteCounter = new AtomicInteger();
    StorageHandler outputHandler = new CountingHandler(byteCounter, 2500, false);
    ScheduledExecutorService timerService = Executors.newScheduledThreadPool(1,
        new DaemonThreadFactory());
    BucketingTopicProcessor processor = new BucketingTopicProcessor(registry, topicConfig,
        outputHandler, timerService, null);
    int bytesWritten = 0;
    int numWrites = 0;
    try {
      for (numWrites = 0; numWrites < 200; numWrites++) {
        StringBuilder builder = new StringBuilder();
        for (int k = 0; k < ThreadLocalRandom.current().nextInt(20); k++) {
          builder.append(UUID.randomUUID().toString());
        }
        byte[] bytes = builder.toString().getBytes();
        WriteRequestPacket payload = new WriteRequestPacket(true, "test".getBytes(), false, 0,
            Unpooled.wrappedBuffer(bytes));
        RequestPacket packet = new RequestPacket(RequestType.PROTOCOL_VERSION,
            ThreadLocalRandom.current().nextLong(), RequestType.WRITE, payload);
        processor.write(packet, payload, null);
        bytesWritten += bytes.length;
      }
      fail("Exception must be thrown since we tried to push more data than the available capacity");
    } catch (Exception e) {
    }
    assertEquals(50, numWrites);
    processor.stopAndAwait();
    assertEquals(bytesWritten, byteCounter.get());
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
        byte b;
        try {
          int counter = 0;
          while ((b = (byte) mis.read()) != -1) {
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
        for (int k = 0; k < ThreadLocalRandom.current().nextInt(20); k++) {
          builder.append(UUID.randomUUID().toString());
        }
        byte[] bytes = builder.toString().getBytes();
        WriteRequestPacket payload = new WriteRequestPacket(true, "test".getBytes(), false, 0,
            Unpooled.wrappedBuffer(bytes));
        RequestPacket packet = new RequestPacket(RequestType.PROTOCOL_VERSION,
            ThreadLocalRandom.current().nextLong(), RequestType.WRITE, payload);
        processor.write(packet, payload, null);
        bytesWritten += bytes.length;
      }
      processor.forceDispatch();
    } catch (Exception e) {
    }
    processor.stopAndAwait();
    assertEquals(bytesWritten, totalBytes.get());
  }

  @Test
  public void testBufferOverrun() {
    MetricRegistry registry = new MetricRegistry();
    TopicConfig topicConfig = new TopicConfig();
    topicConfig.setTopic("test");
    topicConfig.setBatchMilliSeconds(10000);
    topicConfig.setBatchSizeMB(10);
    topicConfig.setBufferSize(1024 * 1024);
    topicConfig.setRingBufferSize(99);
    StorageHandler outputHandler = new StorageHandler() {

      @Override
      public void writeOutput(int sizeInBytes,
                              int checksum,
                              List<Message> messages) throws WriteFailedException {
        System.out.println(messages.size());
        try {
          Thread.sleep(1000);
        } catch (InterruptedException e) {
        }
      }

      @Override
      public String getReadUrl() {
        return null;
      }
    };
    ScheduledExecutorService timerService = Executors.newScheduledThreadPool(1,
        new ThreadFactory() {

          @Override
          public Thread newThread(Runnable r) {
            Thread th = new Thread(r);
            th.setDaemon(true);
            return th;
          }
        });
    TopicProcessor processor = new BucketingTopicProcessor(registry, topicConfig, outputHandler,
        timerService, null);
    try {
      for (int i = 0; i < 200; i++) {
        byte[] bytes = "test".getBytes();
        WriteRequestPacket payload = new WriteRequestPacket(true, "test".getBytes(), false, 0,
            Unpooled.wrappedBuffer(bytes));
        RequestPacket packet = new RequestPacket(RequestType.PROTOCOL_VERSION,
            ThreadLocalRandom.current().nextLong(), RequestType.WRITE, payload);
        processor.write(packet, payload, null);
      }
      fail("Must through service unavailable exception");
    } catch (Exception e) {
    }
    processor.stopNow();
  }

  public final class CountingHandler implements StorageHandler {
    private final AtomicInteger totalBytes;
    private int latency;
    private boolean throwExceptions;

    public CountingHandler(AtomicInteger totalBytes, int latency, boolean throwExceptions) {
      this.totalBytes = totalBytes;
      this.latency = latency;
      this.throwExceptions = throwExceptions;
    }

    @Override
    public void writeOutput(int sizeInBytes,
                            int checksum,
                            List<Message> messages) throws WriteFailedException {
      MessageBufferInputStream mis = new MessageBufferInputStream(messages, null);
      try {
        Thread.sleep(latency);
      } catch (InterruptedException e1) {
        // TODO Auto-generated catch block
        e1.printStackTrace();
      }
      if (throwExceptions) {
        throw new WriteFailedException("Output failed");
      }
      @SuppressWarnings("unused")
      byte b;
      try {
        int counter = 0;
        while ((b = (byte) mis.read()) != -1) {
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
  }

}

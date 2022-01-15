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
package com.pinterest.memq.client.consumer;

import static com.pinterest.memq.client.commons.ConsumerConfigs.DRY_RUN_KEY;
import static com.pinterest.memq.client.commons.ConsumerConfigs.KEY_DESERIALIZER_CLASS_KEY;
import static com.pinterest.memq.client.commons.ConsumerConfigs.VALUE_DESERIALIZER_CLASS_KEY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MemoryRecordsBuilder;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.utils.ByteBufferOutputStream;
import org.junit.Test;

import com.codahale.metrics.MetricRegistry;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.pinterest.memq.client.commons.Compression;
import com.pinterest.memq.client.commons.MemqLogMessageIterator;
import com.pinterest.memq.client.commons.TestUtils;
import com.pinterest.memq.client.commons.serde.ByteArrayDeserializer;
import com.pinterest.memq.commons.BatchHeader;
import com.pinterest.memq.commons.BatchHeader.IndexEntry;
import com.pinterest.memq.commons.MemqLogMessage;
import com.pinterest.memq.commons.storage.StorageHandler;
import com.pinterest.memq.commons.storage.WriteFailedException;
import com.pinterest.memq.core.commons.Message;

public class TestMemqConsumer {

  @Test(expected = RuntimeException.class)
  public void testIteratorRemove() throws IOException {
    Iterator<MemqLogMessage<String, String>> iterator = new MemqLogMessageIterator<>("test", "test",
        new DataInputStream(new ByteArrayInputStream(new byte[0])), null, null, null, null, true,
        null);
    iterator.remove(); // assert exception
  }

  @Test
  public void testIteratorHasNext() throws Exception {
    int count = 5000;
    BiFunction<String, Integer, byte[]> getLogMessageBytes = (base, k) -> base.getBytes();
    Iterator<MemqLogMessage<byte[], byte[]>> iterator = TestUtils.getTestDataIterator("hello world",
        getLogMessageBytes, count, 1, Compression.NONE, true);
    for (int i = 0; i < 2 * count; i++) {
      assertTrue(iterator.hasNext()); // assert idempotence before calling any next()
    }
    for (int i = 0; i < count; i++) {
      assertTrue(iterator.hasNext()); // assert iterator.hasNext() returns true when i < count
      iterator.next();
    }
    for (int i = 0; i < 2 * count; i++) {
      assertFalse("Index:" + i, iterator.hasNext()); // assert hasNext() idempotence after iterator
                                                     // finishes
    }
  }

  @Test
  public void testIteratorNextContentEquals() throws Exception {
    int count = 5000;
    BiFunction<String, Integer, byte[]> getLogMessageBytes = (base, k) -> base.getBytes();
    Iterator<MemqLogMessage<byte[], byte[]>> iterator = TestUtils.getTestDataIterator("hello world",
        getLogMessageBytes, count, 1, Compression.NONE, false);
    for (int i = 0; i < count; i++) {
      assertEquals("hello world", new String(iterator.next().getValue()));
    }
  }

  @Test(expected = RuntimeException.class)
  public void testIteratorNextExceptionHandling() throws Exception {
    int count = 5000;
    BiFunction<String, Integer, byte[]> getLogMessageBytes = (base, k) -> base.getBytes();
    Iterator<MemqLogMessage<byte[], byte[]>> iterator = TestUtils.getTestDataIterator("hello world",
        getLogMessageBytes, count, 1, Compression.NONE, true);
    for (int i = 0; i < count; i++) {
      iterator.next();
    }
    iterator.next(); // should throw exception
  }

  @Test
  public void testIteratorNextPerf() throws Exception {
    int count = 5_00_000;
    StringBuilder payload = new StringBuilder();
    for (int i = 0; i < 10; i++) {
      payload.append(UUID.randomUUID().toString());
    }
    System.out.println("Measuring perf");
    int msgs = 1;
    buildAndRunMemQRecordBench(count, payload, msgs);
    buildAndRunKafkaRecordBench(count, payload, msgs);
  }

  @Test
  public void testIteratorErrorHandling() throws Exception {
    Properties mcProps = new Properties();
    mcProps.put(KEY_DESERIALIZER_CLASS_KEY, ByteArrayDeserializer.class.getName());
    mcProps.put(VALUE_DESERIALIZER_CLASS_KEY, ByteArrayDeserializer.class.getName());
    mcProps.put(DRY_RUN_KEY, "true");
    final AtomicInteger condition = new AtomicInteger();
    BiFunction<String, Integer, byte[]> getLogMessageBytes = (base, k) -> base.getBytes();
    byte[] memqBatchData = TestUtils.getMemqBatchData("hello world", getLogMessageBytes, 100, 1,
        false, Compression.NONE, null, false);
    MemqInput input = new MemqInput() {
      @Override
      public InputStream fetchBatchStreamForNotification(JsonObject nextNotificationToProcess) throws IOException {
        if (condition.getAndIncrement() == 0) {
          throw new IOException("xyz");
        }
        try {
          return new ByteArrayInputStream(memqBatchData);
        } catch (Exception e) {
          e.printStackTrace();
        }
        return null;
      }

      @Override
      public DataInputStream fetchMessageAtIndex(JsonObject objectNotification,
                                                 IndexEntry index) throws IOException {
        return null;
      }

      @Override
      public BatchHeader fetchHeaderForBatch(JsonObject nextNotificationToProcess) throws IOException {
        return null;
      }

      @Override
      public void initReader(Properties properties, MetricRegistry registry) throws Exception {
      }

    };
    MemqConsumer<byte[], byte[]> mc = new MemqConsumer<>(mcProps, input);
    mc.subscribe(Arrays.asList("test"));
    JsonObject obj = new JsonObject();
    obj.addProperty(MemqLogMessage.INTERNAL_FIELD_TOPIC, "test");
    obj.addProperty(MemqLogMessage.INTERNAL_FIELD_OBJECT_SIZE, memqBatchData.length);
    obj.addProperty(MemqLogMessage.INTERNAL_FIELD_NOTIFICATION_PARTITION_ID, 1);
    obj.addProperty(MemqLogMessage.INTERNAL_FIELD_NOTIFICATION_PARTITION_OFFSET, 1);
    obj.addProperty(MemqLogMessage.INTERNAL_FIELD_NOTIFICATION_READ_TIMESTAMP,
        System.currentTimeMillis());
    mc.getNotificationQueue().add(obj);
    Iterator<MemqLogMessage<byte[], byte[]>> itr = mc.poll(Duration.ofSeconds(3));
    if (itr.hasNext()) {
      itr.next();
    }
    mc.close();
  }

  private void buildAndRunMemQRecordBench(int count,
                                          StringBuilder payload,
                                          int msgs) throws Exception, IOException {

    BiFunction<String, Integer, byte[]> getLogMessageBytes = (base, k) -> base.getBytes();
    byte[] rawData = TestUtils.getMemqBatchData(payload.toString(), getLogMessageBytes, count, msgs,
        false, Compression.ZSTD, null, false);
    int c = 0;
    long ns = System.nanoTime();
    for (int i = 0; i < 2; i++) {
      ByteArrayInputStream in = new ByteArrayInputStream(rawData);
      DataInputStream stream = new DataInputStream(in);
      JsonObject obj = new JsonObject();
      obj.addProperty(MemqLogMessage.INTERNAL_FIELD_TOPIC, "test");
      obj.addProperty(MemqLogMessage.INTERNAL_FIELD_OBJECT_SIZE, rawData.length);
      obj.addProperty(MemqLogMessage.INTERNAL_FIELD_NOTIFICATION_PARTITION_ID, 1);
      obj.addProperty(MemqLogMessage.INTERNAL_FIELD_NOTIFICATION_PARTITION_OFFSET, 1);
      obj.addProperty(MemqLogMessage.INTERNAL_FIELD_NOTIFICATION_READ_TIMESTAMP,
          System.currentTimeMillis());
      MemqLogMessageIterator<byte[], byte[]> iterator = new MemqLogMessageIterator<>("test", "test",
          stream, obj, new ByteArrayDeserializer(), new ByteArrayDeserializer(),
          new MetricRegistry(), false, null);
      for (int p = 0; p < count * msgs; p++) {
        iterator.next();
        c++;
      }
    }
    ns = System.nanoTime() - ns;
    System.out.println("Memq:" + ns / 1000 / 1000 + "ms to execute:" + c);
  }

  private void buildAndRunKafkaRecordBench(int count, StringBuilder payload, int msgs) {
    int c;
    long ns;
    ByteBufferOutputStream out = new ByteBufferOutputStream(1024 * 1024);
    MemoryRecordsBuilder builder = new MemoryRecordsBuilder(out, RecordBatch.MAGIC_VALUE_V2,
        CompressionType.ZSTD, TimestampType.CREATE_TIME, 0L, System.currentTimeMillis(),
        System.currentTimeMillis(), (short) 0, 0, false, false,
        (int) (System.currentTimeMillis() / 1000), 100);
    byte[] bytes = payload.toString().getBytes();
    for (int i = 0; i < count * msgs; i++) {
      builder.append(System.currentTimeMillis(), null, bytes);
    }
    builder.close();
    ns = System.nanoTime();
    MemoryRecords records = builder.build();
    c = 0;
    for (int i = 0; i < 2; i++) {
      for (Record record : records.records()) {
        record.value();
        c++;
      }
    }
    ns = System.nanoTime() - ns;
    System.out.println("Kafka:" + ns / 1000 / 1000 + "ms to execute:" + c);
  }

  @Test(expected = NoTopicsSubscribedException.class)
  public void testNoTopicsSubscribedPoll() throws Exception {
    Properties mcProps = new Properties();
    mcProps.put(DRY_RUN_KEY, "true");
    MemqInput input = new MemqInput() {
      @Override
      public InputStream fetchBatchStreamForNotification(JsonObject nextNotificationToProcess) {
        BiFunction<String, Integer, byte[]> getLogMessageBytes = (base, k) -> base.getBytes();
        try {
          return new ByteArrayInputStream(TestUtils.getMemqBatchData("hello world",
              getLogMessageBytes, 100, 1, false, Compression.NONE, null, false));
        } catch (Exception e) {
          e.printStackTrace();
        }
        return null;
      }

      @Override
      public DataInputStream fetchMessageAtIndex(JsonObject objectNotification,
                                                 IndexEntry index) throws IOException {
        return null;
      }

      @Override
      public BatchHeader fetchHeaderForBatch(JsonObject nextNotificationToProcess) throws IOException {
        return null;
      }

      @Override
      public void initReader(Properties properties, MetricRegistry registry) throws Exception {
      }

    };
    MemqConsumer<byte[], byte[]> mc = new MemqConsumer<byte[], byte[]>(mcProps, input);
    mc.poll(Duration.ofSeconds(3)); // should throw NoTopicsSubscribedException
    mc.close();
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testMultiTopicSubscribeException() throws Exception {
    Properties mcProps = new Properties();
    mcProps.put(DRY_RUN_KEY, "true");
    MemqInput input = new MemqInput() {

      @Override
      public InputStream fetchBatchStreamForNotification(JsonObject nextNotificationToProcess) {
        BiFunction<String, Integer, byte[]> getLogMessageBytes = (base, k) -> base.getBytes();
        try {
          return new ByteArrayInputStream(TestUtils.getMemqBatchData("hello world",
              getLogMessageBytes, 100, 1, false, Compression.NONE, null, false));
        } catch (Exception e) {
          e.printStackTrace();
        }
        return null;
      }

      @Override
      public DataInputStream fetchMessageAtIndex(JsonObject objectNotification,
                                                 IndexEntry index) throws IOException {
        return null;
      }

      @Override
      public BatchHeader fetchHeaderForBatch(JsonObject nextNotificationToProcess) throws IOException {
        return null;
      }

      @Override
      public void initReader(Properties properties, MetricRegistry registry) throws Exception {
      }

    };
    MemqConsumer<byte[], byte[]> mc = new MemqConsumer<byte[], byte[]>(mcProps, input);
    mc.subscribe(Collections.singleton("test_topic"));
    mc.subscribe(Collections.singleton("not_this_topic")); // should throw
                                                           // UnsupportedOperationException
    mc.close();
  }

  @Test
  public void testSubscribeAndUnsubscribe() throws Exception {
    MockConsumer<String, String> consumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
    Properties mcProps = new Properties();
    mcProps.put(KEY_DESERIALIZER_CLASS_KEY, ByteArrayDeserializer.class.getName());
    mcProps.put(VALUE_DESERIALIZER_CLASS_KEY, ByteArrayDeserializer.class.getName());
    mcProps.put(DRY_RUN_KEY, "true");
    MemqInput input = new MemqInput() {

      @Override
      public InputStream fetchBatchStreamForNotification(JsonObject nextNotificationToProcess) {
        BiFunction<String, Integer, byte[]> getLogMessageBytes = (base, k) -> base.getBytes();
        try {
          return new ByteArrayInputStream(TestUtils.getMemqBatchData("hello world",
              getLogMessageBytes, 100, 1, false, Compression.NONE, null, false));
        } catch (Exception e) {
          e.printStackTrace();
        }
        return null;
      }

      @Override
      public DataInputStream fetchMessageAtIndex(JsonObject objectNotification,
                                                 IndexEntry index) throws IOException {
        return null;
      }

      @Override
      public BatchHeader fetchHeaderForBatch(JsonObject nextNotificationToProcess) throws IOException {
        return null;
      }

    };
    MemqConsumer<byte[], byte[]> mc = new MemqConsumer<byte[], byte[]>(mcProps, input);
    KafkaNotificationSource notificationSource = new KafkaNotificationSource(consumer);
    notificationSource.setParentConsumer(mc);
    mc.setNotificationSource(notificationSource);
    mc.subscribe(Collections.singleton("test_topic"));

    consumer.assign(Collections.singletonList(new TopicPartition("test_notification_topic", 0)));
    Map<TopicPartition, Long> beginningOffsets = new HashMap<>();
    beginningOffsets.put(new TopicPartition("test_notification_topic", 0), 0L);
    consumer.updateBeginningOffsets(beginningOffsets);

    int numNotifications = 10;
    for (int i = 0; i < numNotifications; i++) {
      JsonObject notification = new JsonObject();
      if (i % 2 == 0) {
        notification.addProperty("topic", "test_topic");
      } else {
        notification.addProperty("topic", "not_this_topic");
      }
      consumer.addRecord(new ConsumerRecord<>("test_notification_topic", 0, i, "key",
          new Gson().toJson(notification)));
    }
    assertEquals(0, mc.getNotificationQueue().size());
    mc.poll(Duration.ofSeconds(3));
    // numNotifications / 2 - 1
    assertEquals(0, mc.getNotificationQueue().size());

    for (int i = 0; i < numNotifications; i++) {
      JsonObject notification = new JsonObject();
      if (i % 2 == 0) {
        notification.addProperty("topic", "test_topic");
      } else {
        notification.addProperty("topic", "not_this_topic");
      }
      consumer.addRecord(new ConsumerRecord<>("test_notification_topic", 0, i + numNotifications,
          "key", new Gson().toJson(notification)));
    }
    // numNotifications / 2 - 1
    assertEquals(0, mc.getNotificationQueue().size());
    mc.poll(Duration.ofSeconds(3));
    // numNotifications - 2
    assertEquals(0, mc.getNotificationQueue().size());
    // should have twice the amount of notifications as before

    mc.unsubscribe();
    mc.subscribe(Collections.singleton("test_topic")); // subscribe again
    for (int i = 0; i < numNotifications; i++) {
      JsonObject notification = new JsonObject();
      if (i % 2 == 0) {
        notification.addProperty("topic", "test_topic");
      } else {
        notification.addProperty("topic", "not_this_topic");
      }
      consumer.addRecord(new ConsumerRecord<>("test_notification_topic", 0,
          i + (2 * numNotifications), "key", new Gson().toJson(notification)));
    }

    for (int i = 0; i < numNotifications; i++) {
      JsonObject notification = new JsonObject();
      if (i % 2 == 0) {
        notification.addProperty("topic", "test_topic");
      } else {
        notification.addProperty("topic", "not_this_topic");
      }
      consumer.addRecord(new ConsumerRecord<>("test_notification_topic", 0,
          i + (3 * numNotifications), "key", new Gson().toJson(notification)));
    }
    mc.poll(Duration.ofSeconds(3));
    // ((numNotifications / 2) * 4 - 3
    assertEquals(0, mc.getNotificationQueue().size());
    // mc should have picked up where it left off from previous successful poll

    mc.subscribe(Collections.singleton("test_topic")); // shouldn't do anything
    for (int i = 0; i < numNotifications; i++) {
      JsonObject notification = new JsonObject();
      if (i % 2 == 0) {
        notification.addProperty("topic", "test_topic");
      } else {
        notification.addProperty("topic", "not_this_topic");
      }
      consumer.addRecord(new ConsumerRecord<>("test_notification_topic", 0,
          i + (4 * numNotifications), "key", new Gson().toJson(notification)));
    }
    // mc should have picked up where it left off from previous successful poll
    mc.poll(Duration.ofSeconds(3));
    // (numNotifications / 2) * 5 - 4
    assertEquals(0, mc.getNotificationQueue().size());
    mc.close();
  }

  @Test
  public void testSkipToLastLogMessage() throws Exception {
    int count = 10241;
    String data = "xyza33245245234534bcs";
    String ary = data;
    for (int i = 0; i < 2; i++) {
      ary += ary;
    }
    BiFunction<String, Integer, byte[]> getLogMessageBytes = (base, k) -> (base + k).getBytes();
    String baseLogMessage = ary + "i:";
    MemqLogMessageIterator<byte[], byte[]> iterator = (MemqLogMessageIterator<byte[], byte[]>) TestUtils
        .getTestDataIterator(baseLogMessage, getLogMessageBytes, count, 5, Compression.GZIP, false);
    for (int i = 0; i < 501; i++) {
      assertTrue(iterator.hasNext()); // assert idempotence
    }
    iterator.skipToLastLogMessage();
    int i = 0;
    try {
      while (iterator.hasNext()) {
        iterator.next();
        i++;
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail("Failed at:" + i + " " + e.getMessage());
    }
    assertEquals("Skip to last MUST yield only 1 message", 1, i);
  }

  public abstract class MemqInput implements StorageHandler {

    @Override
    public String getReadUrl() {
      return null;
    }

    @Override
    public void writeOutput(int sizeInBytes,
                            int checksum,
                            List<Message> messages) throws WriteFailedException {
    }

  }
}

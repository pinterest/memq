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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.function.BiFunction;

import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.io.Files;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.pinterest.memq.client.commons.Compression;
import com.pinterest.memq.client.commons.ConsumerConfigs;
import com.pinterest.memq.client.commons.TestUtils;
import com.pinterest.memq.client.commons.audit.KafkaBackedAuditor;
import com.pinterest.memq.commons.BatchHeader;
import com.pinterest.memq.commons.BatchHeader.IndexEntry;
import com.pinterest.memq.commons.MemqLogMessage;
import com.pinterest.memq.commons.storage.StorageHandler;
import com.pinterest.memq.commons.storage.WriteFailedException;
import com.pinterest.memq.core.commons.Message;
import com.salesforce.kafka.test.junit4.SharedKafkaTestResource;
import com.salesforce.kafka.test.listeners.PlainListener;

public class TestConsumerIntegration {

  private static final String TOPIC = "topic";
  public static final String KEY = "key";
  public static final String BUCKET = "bucket";
  public static final String SIZE = "objectSize";
  public static final String HEADER_SIZE = "headerSize";
  private static final String NUMBER_OF_MESSAGES_IN_BATCH = "numBatchMessages";

  @ClassRule
  public static final SharedKafkaTestResource sharedKafkaTestResource = new SharedKafkaTestResource()
      .withBrokers(1).registerListener(new PlainListener().onPorts(9092));

  @Before
  public void before() {
    String kafkaConnectString = "localhost:9092";
    Properties adminProps = new Properties();
    adminProps.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaConnectString);
    adminProps.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
        ByteArrayDeserializer.class.getCanonicalName());
    adminProps.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
        ByteArrayDeserializer.class.getCanonicalName());
    adminProps.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "test12");
    adminProps.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    AdminClient admin = AdminClient.create(adminProps);
    String notificationTopic = "notify_topic_1";
    admin.createTopics(Arrays.asList(new NewTopic(notificationTopic, 1, (short) 1)));
    admin.createTopics(Arrays.asList(new NewTopic("auditTopic", 1, (short) 1)));
    admin.close();
  }
  
  @Test
  public void testSeekAndRead() throws Exception {
    Properties props = new Properties();
    props.setProperty(ConsumerConfigs.CLUSTER, "test");
    Properties notificationProps = new Properties();
    notificationProps.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    notificationProps.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "test_1");
    notificationProps.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    notificationProps.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
        StringSerializer.class.getCanonicalName());
    notificationProps.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
        StringSerializer.class.getCanonicalName());
    String notificationTopic = "notify_topic_1";
    notificationProps.setProperty(KafkaNotificationSource.NOTIFICATION_TOPIC_NAME_KEY,
        notificationTopic);
    props.put(ConsumerConfigs.NOTIFICATION_SOURCE_PROPS_KEY, notificationProps);

    String topic = "topic1";

    BiFunction<String, Integer, byte[]> getLogMessageBytes = (base, k) -> base.getBytes();
    List<byte[]> hashes = new ArrayList<>();
    final byte[] memqBatchData = TestUtils.getMemqBatchData("test1231231", getLogMessageBytes, 1000,
        10, true, Compression.GZIP, hashes, true);

    Gson gson = new Gson();
    KafkaProducer<String, String> notificationProducer = new KafkaProducer<>(notificationProps);
    JsonObject payload = new JsonObject();
    payload.addProperty(BUCKET, "local1");
    payload.addProperty(KEY, "local1");
    payload.addProperty(SIZE, memqBatchData.length);
    payload.addProperty(TOPIC, topic);
    payload.addProperty(HEADER_SIZE, 1024);
    payload.addProperty(NUMBER_OF_MESSAGES_IN_BATCH, 1000);
    notificationProducer
        .send(new ProducerRecord<String, String>(notificationTopic, gson.toJson(payload)));
    notificationProducer
        .send(new ProducerRecord<String, String>(notificationTopic, gson.toJson(payload)));
    notificationProducer.close();

    MemqConsumer<byte[], byte[]> consumer = getConsumer(props, memqBatchData);
    consumer.subscribe(Lists.newArrayList(topic));
    consumer.assign(Lists.newArrayList(0));
    consumer.seek(ImmutableMap.of(0, 1L));
    MutableInt reads = new MutableInt();
    Iterator<MemqLogMessage<byte[], byte[]>> poll = consumer.poll(Duration.ofSeconds(10), reads);
    assertEquals(1, (int) reads.getValue());
    int count = 0;
    while (poll.hasNext()) {
      poll.next();
      count++;
    }
    assertEquals(10000, count);
    consumer.close();
    
    Files.write("localhost:9092".getBytes(), new File("target/testconsumeraudit"));

    props.setProperty("auditor.bootstrap.servers", "localhost:9092");
    props.setProperty("auditor.class", KafkaBackedAuditor.class.getCanonicalName());
    props.setProperty("auditor.topic", "auditTopic");
    props.setProperty("auditor.enabled", "true");
    notificationProps.setProperty(KafkaNotificationSource.NOTIFICATION_TOPIC_NAME_KEY,
        notificationTopic);
    props.put(ConsumerConfigs.NOTIFICATION_SOURCE_PROPS_KEY, notificationProps);
    consumer = getConsumer(props, memqBatchData);
    consumer.subscribe(Lists.newArrayList(topic));
    reads = new MutableInt();
    poll = consumer.poll(Duration.ofSeconds(10), reads);
    assertEquals(2, (int) reads.getValue());
    count = 0;
    while (poll.hasNext()) {
      poll.next();
      count++;
    }
    // 2(notification events)x10(Messages)x1000(Message per event)=20000 messages
    assertEquals(20000, count);

    // this section of the code validates whether or not auditing is working on the
    // consumer
    Properties auditConfig = new Properties();
    auditConfig.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    auditConfig.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
        ByteArrayDeserializer.class.getCanonicalName());
    auditConfig.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
        ByteArrayDeserializer.class.getCanonicalName());
    auditConfig.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "auditConfig_1");
    auditConfig.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    KafkaConsumer<byte[], byte[]> auditConsumer = new KafkaConsumer<>(auditConfig);
    auditConsumer.subscribe(Arrays.asList("auditTopic"));

    ConsumerRecords<byte[], byte[]> pollAudit = auditConsumer.poll(Duration.ofSeconds(10));
    Iterator<ConsumerRecord<byte[], byte[]>> itr = pollAudit.iterator();
    int auditCount = 0;
    List<byte[]> outputHashes = new ArrayList<>();
    while (itr.hasNext()) {
      ConsumerRecord<byte[], byte[]> next = itr.next();
      ByteBuffer wrap = ByteBuffer.wrap(next.value());
      wrap.position(wrap.limit() - 13);
      byte[] hash = new byte[8];
      wrap.get(hash);
      outputHashes.add(hash);
      auditCount++;
    }
    auditConsumer.close();
    // validate that audit event count is correct
    // there were 2 notification events sent, each notification event / Batch has 10
    // Messages and each message has 1000 LogMessages
    // Audit event is generated for every Message so there should 2x10 = 20 audit
    // events
    assertEquals(20, auditCount);
    // Now we validate audit event hashes to ensure that messageId hashed for all
    // 10000x2 events is correct and all events generated were received
    for (int i = 0; i < outputHashes.size(); i++) {
      assertArrayEquals(hashes.get(i % hashes.size()), outputHashes.get(i));
    }
  }

  private MemqConsumer<byte[], byte[]> getConsumer(Properties props,
                                                   final byte[] memqBatchData) throws Exception {
    StorageHandler input = new StorageHandler() {
      
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
      public InputStream fetchBatchStreamForNotification(JsonObject nextNotificationToProcess) {
        return new ByteArrayInputStream(memqBatchData);
      }

      @Override
      public void writeOutput(int sizeInBytes,
                              int checksum,
                              List<Message> messages) throws WriteFailedException {
      }

      @Override
      public String getReadUrl() {
        return null;
      }
    };
    return new MemqConsumer<byte[], byte[]>(props, input);
  }

}

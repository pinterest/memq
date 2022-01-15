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
package com.pinterest.memq.client.commons.audit;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.junit.ClassRule;
import org.junit.Test;

import com.google.common.io.Files;
import com.pinterest.memq.core.utils.MemqUtils;
import com.salesforce.kafka.test.junit4.SharedKafkaTestResource;
import com.salesforce.kafka.test.listeners.PlainListener;

public class TestKafkaBackedAuditor {

  @ClassRule
  public static final SharedKafkaTestResource sharedKafkaTestResource = new SharedKafkaTestResource()
      .withBrokers(1).registerListener(new PlainListener().onPorts(9092));

  @Test
  public void testAudit() throws Exception {
    String kafkaConnectString = "localhost:9092";
    String pathname = "target/testserverset1";
    Properties adminProps = new Properties();
    adminProps.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaConnectString);
    adminProps.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
        ByteArrayDeserializer.class.getCanonicalName());
    adminProps.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
        ByteArrayDeserializer.class.getCanonicalName());
    adminProps.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "test12");
    adminProps.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    AdminClient admin = AdminClient.create(adminProps);
    String auditTopic = "testaudittopic";
    admin.createTopics(Arrays.asList(new NewTopic(auditTopic, 3, (short) 1)));
    admin.close();
    Files.write(kafkaConnectString.getBytes(), new File(pathname));
    Auditor kafkaAuditor = new KafkaBackedAuditor();
    Properties props = new Properties();
    props.setProperty("serverset", pathname);
    props.setProperty("topic", auditTopic);
    kafkaAuditor.init(props);
    long epoch = System.currentTimeMillis();
    for (int i = 0; i < 100; i++) {
      kafkaAuditor.auditMessage("test".getBytes(), "test1".getBytes(), MemqUtils.HOST_IPV4_ADDRESS,
          epoch, i, new byte[16], 10, false, "producer");
    }
    kafkaAuditor.close();

    KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(adminProps);
    consumer.subscribe(Arrays.asList(auditTopic));
    Set<Long> idSet = new HashSet<>();
    for (ConsumerRecord<byte[], byte[]> consumerRecord : consumer.poll(Duration.ofMillis(10000))) {
      ByteBuffer buf = ByteBuffer.wrap(consumerRecord.value());

      assertEquals(20, consumerRecord.key().length);
      ByteBuffer keyBuf = ByteBuffer.wrap(consumerRecord.key());

      assertEquals(4, buf.getShort());
      byte[] tmp = new byte[4];
      buf.get(tmp);
      assertEquals("test", new String(tmp));

      assertEquals(5, buf.getShort());
      tmp = new byte[5];
      buf.get(tmp);
      assertEquals("test1", new String(tmp));

      assertEquals(4, buf.getShort());
      tmp = new byte[4];
      buf.get(tmp);

      byte[] keyTmp = new byte[4];
      keyBuf.get(keyTmp);
      assertArrayEquals(tmp, keyTmp);

      assertEquals(epoch, buf.getLong());
      long count = buf.getLong();
      idSet.add(count);

      assertEquals(epoch, keyBuf.getLong());
      assertEquals(count, keyBuf.getLong());

      assertEquals(16, buf.getShort());
      tmp = new byte[16];
      buf.get(tmp);
    }
    assertEquals(100L, idSet.size());
    assertTrue(idSet.containsAll(LongStream.range(0, 100).boxed().collect(Collectors.toList())));
    consumer.close();
  }

  @Test
  public void testSingleton() throws Exception {
    Properties props = new Properties();
    props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    KafkaProducer<byte[], byte[]> p1 = KafkaBackedAuditor.getProducer("test", props);
    KafkaProducer<byte[], byte[]> p2 = KafkaBackedAuditor.getProducer("test", props);
    assertSame(p1, p2);

    Properties props1 = new Properties();
    props1.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9093");
    KafkaProducer<byte[], byte[]> p3 = KafkaBackedAuditor.getProducer("test1", props1);
    assertNotSame(p1, p3);
  }
}

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
package com.pinterest.memq.core.integration;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.time.Duration;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.pinterest.memq.client.commons.Compression;
import com.pinterest.memq.client.commons.ConsumerConfigs;
import com.pinterest.memq.client.commons.serde.ByteArraySerializer;
import com.pinterest.memq.client.consumer.MemqConsumer;
import com.pinterest.memq.client.producer.MemqWriteResult;
import com.pinterest.memq.client.producer.netty.MemqNettyProducer;
import com.pinterest.memq.client.producer.netty.MemqNettyRequest;
import com.pinterest.memq.commons.CloseableIterator;
import com.pinterest.memq.commons.MemqLogMessage;
import com.pinterest.memq.commons.protocol.Broker.BrokerType;
import com.pinterest.memq.commons.protocol.TopicConfig;
import com.pinterest.memq.commons.storage.fs.FileSystemStorageHandler;
import com.pinterest.memq.commons.storage.s3.KafkaNotificationSink;
import com.pinterest.memq.core.MemqManager;
import com.pinterest.memq.core.clustering.MemqGovernor;
import com.pinterest.memq.core.config.EnvironmentProvider;
import com.pinterest.memq.core.config.MemqConfig;
import com.pinterest.memq.core.config.NettyServerConfig;
import com.pinterest.memq.core.rpc.MemqNettyServer;
import com.pinterest.memq.core.rpc.TestAuditor;
import com.pinterest.memq.core.utils.DaemonThreadFactory;
import com.pinterest.memq.core.utils.MiscUtils;
import com.salesforce.kafka.test.junit4.SharedKafkaTestResource;
import com.salesforce.kafka.test.listeners.PlainListener;

public class TestMemqReadBrokers {

  @ClassRule
  public static final SharedKafkaTestResource sharedKafkaTestResource = new SharedKafkaTestResource()
      .withBrokers(1).registerListener(new PlainListener().onPorts(9092));

  @Rule
  public TemporaryFolder folder = new TemporaryFolder();

  @Test
  public void testProduceConsumeWithReadBrokers() throws Exception {
    String notificationTopic = "notify_topic_1";
    TestMemqClientServerIntegration.createTopic(notificationTopic, "localhost:9092",
        "audit_topic_1");
    Files.write(new File("target/test_broker_read_producer_consumer").toPath(),
        "localhost:9092".getBytes());

    long startMs = System.currentTimeMillis();
    MemqConfig configuration = new MemqConfig();
    NettyServerConfig nettyServerConfig = new NettyServerConfig();
    nettyServerConfig.setPort((short) 23434);
    configuration.setNettyServerConfig(nettyServerConfig);
    configuration.setBrokerType(BrokerType.READ_WRITE);
    TopicConfig topicConfig = new TopicConfig("test", "filesystem");
    topicConfig.setBufferSize(1024 * 1024);
    topicConfig.setBatchSizeMB(2);
    topicConfig.setRingBufferSize(128);
    topicConfig.setTickFrequencyMillis(10);
    topicConfig.setBatchMilliSeconds(2000);
    Properties outputHandlerConfig = new Properties();
    outputHandlerConfig.setProperty(FileSystemStorageHandler.STORAGE_DIRS,
        folder.newFolder().getAbsolutePath());
    outputHandlerConfig.setProperty(FileSystemStorageHandler.NOTIFICATIONS_DISABLE, "false");
    outputHandlerConfig.setProperty(KafkaNotificationSink.NOTIFICATION_SERVERSET,
        "target/test_broker_read_producer_consumer");
    outputHandlerConfig.setProperty("read.local.enabled", "false");
    outputHandlerConfig.setProperty(KafkaNotificationSink.NOTIFICATION_TOPIC, notificationTopic);

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
    int timeout = 60_000;
    MemqNettyProducer<byte[], byte[]> producer = new MemqNettyProducer<>("testcluster",
        new InetSocketAddress("localhost", nettyServerConfig.getPort()), "test", 10, 1000000,
        Compression.GZIP, false, 10, timeout, "local", timeout, auditConfigs, null);
    producer.setKeySerializer(new ByteArraySerializer());
    producer.setValueSerializer(new ByteArraySerializer());
    byte[] value = UUID.randomUUID().toString().getBytes();
    Set<Future<MemqWriteResult>> futures = new LinkedHashSet<>();
    Timer timer = MiscUtils.oneMinuteWindowTimer(new MetricRegistry(), "requests.write.time");
    for (int i = 0; i < 1_000_000; i++) {
      Timer.Context ctx = timer.time();
      Future<MemqWriteResult> writeToTopic = producer.writeToTopic(null, value);
      ctx.stop();
      futures.add(writeToTopic);
    }
    System.out.println("Sent all writes: " + (System.currentTimeMillis() - startMs));
    producer.finalizeRequest();
    for (Future<MemqWriteResult> future : futures) {
      future.get(30, TimeUnit.SECONDS);
    }
    System.out.println("Completed all writes: " + (System.currentTimeMillis() - startMs));
    Thread.sleep(500);
    producer.close();

    // server should have written 55 batches
    assertEquals(MemqNettyRequest.getAckedByteCounter(), MemqNettyRequest.getByteCounter());
    TestAuditor.reset();

    producer = new MemqNettyProducer<>("testcluster",
        new InetSocketAddress("localhost", nettyServerConfig.getPort()), topicConfig.getTopic(), 10,
        1000000, Compression.NONE, true, 10, timeout, "local", timeout, auditConfigs, null);
    producer.setKeySerializer(new ByteArraySerializer());
    producer.setValueSerializer(new ByteArraySerializer());
    value = UUID.randomUUID().toString().getBytes();
    futures = new LinkedHashSet<>();
    timer = MiscUtils.oneMinuteWindowTimer(new MetricRegistry(), "requests.write.time");
    for (int i = 0; i < 1_000_000; i++) {
      Timer.Context ctx = timer.time();
      Future<MemqWriteResult> writeToTopic = producer.writeToTopic(null, value);
      ctx.stop();
      futures.add(writeToTopic);
    }
    System.out.println("Sent all writes: " + (System.currentTimeMillis() - startMs));
    producer.finalizeRequest();
    for (Future<MemqWriteResult> future : futures) {
      future.get(30, TimeUnit.SECONDS);
    }
    System.out.println("Completed all writes: " + (System.currentTimeMillis() - startMs));
    Thread.sleep(500);
    producer.close();

    assertEquals(futures.size(), TestAuditor.getAuditMessageList().size());
    assertEquals(MemqNettyRequest.getAckedByteCounter(), MemqNettyRequest.getByteCounter());

    Properties props = new Properties();
    props.setProperty(ConsumerConfigs.BOOTSTRAP_SERVERS,
        "localhost:" + nettyServerConfig.getPort());
    props.setProperty(ConsumerConfigs.DIRECT_CONSUMER, "false");
    props.setProperty(ConsumerConfigs.GROUP_ID, "test_111_" + System.currentTimeMillis());

    MemqConsumer<byte[], byte[]> consumer = new MemqConsumer<byte[], byte[]>(props);
    // subscribe to the topic and attempt a poll
    consumer.subscribe(topicConfig.getTopic());

    int c = 0;
    CloseableIterator<MemqLogMessage<byte[], byte[]>> poll = consumer.poll(Duration.ofSeconds(5));
    while (poll.hasNext()) {
      poll.next();
      c++;
    }
    assertEquals(2_000_000, c);

    assertNotNull(consumer.getStorageHandler());

    consumer.close();
    server.stop();
  }

  @Test
  public void testProduceConsumeWithReadBrokersSendFile() throws Exception {
    String notificationTopic = "notify_topic_2";
    TestMemqClientServerIntegration.createTopic(notificationTopic, "localhost:9092",
        "audit_topic_2");
    Files.write(new File("target/test_broker_read_producer_consumer_sendfile").toPath(),
        "localhost:9092".getBytes());

    long startMs = System.currentTimeMillis();
    MemqConfig configuration = new MemqConfig();
    NettyServerConfig nettyServerConfig = new NettyServerConfig();
    nettyServerConfig.setPort((short) 23435);
    configuration.setNettyServerConfig(nettyServerConfig);
    configuration.setBrokerType(BrokerType.READ_WRITE);
    TopicConfig topicConfig = new TopicConfig("test", "filesystem");
    topicConfig.setBufferSize(1024 * 1024);
    topicConfig.setBatchSizeMB(2);
    topicConfig.setRingBufferSize(128);
    topicConfig.setTickFrequencyMillis(10);
    topicConfig.setBatchMilliSeconds(2000);
    Properties storageHandlerConfig = new Properties();
    storageHandlerConfig.setProperty(FileSystemStorageHandler.STORAGE_DIRS,
        folder.newFolder().getAbsolutePath());
    storageHandlerConfig.setProperty(FileSystemStorageHandler.NOTIFICATIONS_DISABLE, "false");
    storageHandlerConfig.setProperty(KafkaNotificationSink.NOTIFICATION_SERVERSET,
        "target/test_broker_read_producer_consumer_sendfile");
    storageHandlerConfig.setProperty("read.local.enabled", "false");
    storageHandlerConfig.setProperty(FileSystemStorageHandler.OPTIMIZATION_SENDFILE, "true");
    storageHandlerConfig.setProperty(KafkaNotificationSink.NOTIFICATION_TOPIC, notificationTopic);

    topicConfig.setStorageHandlerConfig(storageHandlerConfig);
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
    int timeout = 60_000;
    MemqNettyProducer<byte[], byte[]> producer = new MemqNettyProducer<>("testcluster",
        new InetSocketAddress("localhost", nettyServerConfig.getPort()), "test", 10, 1000000,
        Compression.GZIP, false, 10, timeout, "local", timeout, auditConfigs, null);
    producer.setKeySerializer(new ByteArraySerializer());
    producer.setValueSerializer(new ByteArraySerializer());
    byte[] value = UUID.randomUUID().toString().getBytes();
    Set<Future<MemqWriteResult>> futures = new LinkedHashSet<>();
    Timer timer = MiscUtils.oneMinuteWindowTimer(new MetricRegistry(), "requests.write.time");
    for (int i = 0; i < 1_00_000; i++) {
      Timer.Context ctx = timer.time();
      Future<MemqWriteResult> writeToTopic = producer.writeToTopic(null, value);
      ctx.stop();
      futures.add(writeToTopic);
    }
    System.out.println("Sent all writes: " + (System.currentTimeMillis() - startMs));
    producer.finalizeRequest();
    for (Future<MemqWriteResult> future : futures) {
      future.get(30, TimeUnit.SECONDS);
    }
    System.out.println("Completed all writes: " + (System.currentTimeMillis() - startMs));
    Thread.sleep(500);
    producer.close();

    Properties props = new Properties();
    props.setProperty(ConsumerConfigs.BOOTSTRAP_SERVERS,
        "localhost:" + nettyServerConfig.getPort());
    props.setProperty(ConsumerConfigs.DIRECT_CONSUMER, "false");
    props.setProperty(ConsumerConfigs.GROUP_ID, "test_111_" + System.currentTimeMillis());

    MemqConsumer<byte[], byte[]> consumer = new MemqConsumer<byte[], byte[]>(props);
    // subscribe to the topic and attempt a poll
    consumer.subscribe(topicConfig.getTopic());

    int c = 0;
    CloseableIterator<MemqLogMessage<byte[], byte[]>> poll = consumer.poll(Duration.ofSeconds(5));
    while (poll.hasNext()) {
      poll.next();
      c++;
    }
    assertEquals(1_00_000, c);

    assertNotNull(consumer.getStorageHandler());

    consumer.close();

    server.stop();
  }

  @Test
  public void testReadBrokerHeaders() throws Exception {
    String notificationTopic = "notify_topic_3";
    TestMemqClientServerIntegration.createTopic(notificationTopic, "localhost:9092",
        "audit_topic_3");
    Files.write(new File("target/test_broker_read_headers").toPath(), "localhost:9092".getBytes());

    long startMs = System.currentTimeMillis();
    MemqConfig configuration = new MemqConfig();
    NettyServerConfig nettyServerConfig = new NettyServerConfig();
    nettyServerConfig.setPort((short) 23435);
    configuration.setNettyServerConfig(nettyServerConfig);
    configuration.setBrokerType(BrokerType.READ_WRITE);
    TopicConfig topicConfig = new TopicConfig("test", "filesystem");
    topicConfig.setBufferSize(1024 * 1024);
    topicConfig.setBatchSizeMB(2);
    topicConfig.setRingBufferSize(128);
    topicConfig.setTickFrequencyMillis(10);
    topicConfig.setBatchMilliSeconds(2000);
    Properties storageHandlerConfig = new Properties();
    storageHandlerConfig.setProperty(FileSystemStorageHandler.STORAGE_DIRS,
        folder.newFolder().getAbsolutePath());
    storageHandlerConfig.setProperty(FileSystemStorageHandler.NOTIFICATIONS_DISABLE, "false");
    storageHandlerConfig.setProperty(KafkaNotificationSink.NOTIFICATION_SERVERSET,
        "target/test_broker_read_headers");
    storageHandlerConfig.setProperty("read.local.enabled", "false");
    storageHandlerConfig.setProperty(FileSystemStorageHandler.OPTIMIZATION_SENDFILE, "true");
    storageHandlerConfig.setProperty(KafkaNotificationSink.NOTIFICATION_TOPIC, notificationTopic);

    topicConfig.setStorageHandlerConfig(storageHandlerConfig);
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
    int timeout = 60_000;
    MemqNettyProducer<byte[], byte[]> producer = new MemqNettyProducer<>("testcluster",
        new InetSocketAddress("localhost", nettyServerConfig.getPort()), "test", 10, 1000000,
        Compression.GZIP, false, 10, timeout, "local", timeout, auditConfigs, null);
    producer.setKeySerializer(new ByteArraySerializer());
    producer.setValueSerializer(new ByteArraySerializer());
    byte[] value = UUID.randomUUID().toString().getBytes();
    Set<Future<MemqWriteResult>> futures = new LinkedHashSet<>();
    Timer timer = MiscUtils.oneMinuteWindowTimer(new MetricRegistry(), "requests.write.time");
    for (int i = 0; i < 1_00_000; i++) {
      Timer.Context ctx = timer.time();
      Future<MemqWriteResult> writeToTopic = producer.writeToTopic(null, value);
      ctx.stop();
      futures.add(writeToTopic);
    }
    System.out.println("Sent all writes: " + (System.currentTimeMillis() - startMs));
    producer.finalizeRequest();
    for (Future<MemqWriteResult> future : futures) {
      future.get(30, TimeUnit.SECONDS);
    }
    System.out.println("Completed all writes: " + (System.currentTimeMillis() - startMs));
    Thread.sleep(500);
    producer.close();

    Properties props = new Properties();
    props.setProperty(ConsumerConfigs.BOOTSTRAP_SERVERS,
        "localhost:" + nettyServerConfig.getPort());
    props.setProperty(ConsumerConfigs.DIRECT_CONSUMER, "false");
    props.setProperty(ConsumerConfigs.GROUP_ID, "test_111_" + System.currentTimeMillis());

    MemqConsumer<byte[], byte[]> consumer = new MemqConsumer<byte[], byte[]>(props);
    // subscribe to the topic and attempt a poll
    consumer.subscribe(topicConfig.getTopic());

    ExecutorService es = Executors.newSingleThreadExecutor(new DaemonThreadFactory());
    List<MemqLogMessage<byte[], byte[]>> messages = consumer.getLogMessagesAtOffsets(
        Duration.ofMillis(5000), new int[] { 0 }, new long[] { 0 }, new int[] { 0 },
        new int[] { 0 }, es);

    assertTrue(messages.size() > 0);

    consumer.close();

    es.shutdown();
    server.stop();
  }

}

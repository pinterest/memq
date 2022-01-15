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
import static org.junit.Assert.fail;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.security.KeyStore;
import java.security.KeyStore.PrivateKeyEntry;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.google.gson.JsonObject;
import com.pinterest.memq.client.commons.Compression;
import com.pinterest.memq.client.commons.ConsumerConfigs;
import com.pinterest.memq.client.commons.MemqCommonClient;
import com.pinterest.memq.client.commons.MemqNettyClientSideResponseHandler;
import com.pinterest.memq.client.commons.ProducerConfigs;
import com.pinterest.memq.client.commons.ResponseHandler;
import com.pinterest.memq.client.commons.serde.ByteArraySerializer;
import com.pinterest.memq.client.consumer.MemqConsumer;
import com.pinterest.memq.client.producer.MemqProducer;
import com.pinterest.memq.client.producer.MemqWriteResult;
import com.pinterest.memq.client.producer.netty.MemqNettyProducer;
import com.pinterest.memq.client.producer.netty.MemqNettyRequest;
import com.pinterest.memq.commons.BatchHeader;
import com.pinterest.memq.commons.BatchHeader.IndexEntry;
import com.pinterest.memq.commons.config.SSLConfig;
import com.pinterest.memq.commons.protocol.Broker;
import com.pinterest.memq.commons.protocol.RequestPacket;
import com.pinterest.memq.commons.protocol.RequestType;
import com.pinterest.memq.commons.protocol.ResponsePacket;
import com.pinterest.memq.commons.protocol.TopicConfig;
import com.pinterest.memq.commons.protocol.TopicMetadata;
import com.pinterest.memq.commons.protocol.Broker.BrokerType;
import com.pinterest.memq.commons.storage.DelayedDevNullStorageHandler;
import com.pinterest.memq.commons.storage.StorageHandler;
import com.pinterest.memq.commons.storage.WriteFailedException;
import com.pinterest.memq.core.MemqManager;
import com.pinterest.memq.core.clustering.MemqGovernor;
import com.pinterest.memq.core.commons.Message;
import com.pinterest.memq.core.config.EnvironmentProvider;
import com.pinterest.memq.core.config.MemqConfig;
import com.pinterest.memq.core.config.NettyServerConfig;
import com.pinterest.memq.core.rpc.MemqNettyServer;
import com.pinterest.memq.core.rpc.MemqRequestDecoder;
import com.pinterest.memq.core.rpc.MemqResponseEncoder;
import com.pinterest.memq.core.rpc.TestAuditor;
import com.pinterest.memq.core.utils.MiscUtils;
import com.salesforce.kafka.test.junit4.SharedKafkaTestResource;
import com.salesforce.kafka.test.listeners.PlainListener;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.ssl.util.SelfSignedCertificate;

public class TestMemqClientServerIntegration {

  private static final String JKS = "JKS";

  @ClassRule
  public static final SharedKafkaTestResource sharedKafkaTestResource = new SharedKafkaTestResource()
      .withBrokers(1).registerListener(new PlainListener().onPorts(9092));

  @Before
  public void before() {
    new File(new MemqConfig().getTopicCacheFile()).delete();
  }

  @After
  public void after() {
    new File(new MemqConfig().getTopicCacheFile()).delete();
    DelayedDevNullStorageHandler.reset();
    TestAuditor.reset();
    MemqNettyRequest.reset();
  }

  @Test
  public void testProducerAndServer() throws IOException {
    long currentRequestId = 1213323L;
    MemqNettyRequest request = new MemqNettyRequest("test", currentRequestId, Compression.ZSTD,
        null, false, 1024 * 1024 * 2, 100, null, null, 200, false);
    ByteBuffer buf = ByteBuffer.allocate(1024 * 100);
    byte[] payload = buf.array();
    RequestPacket requestPacket = request.getWriteRequestPacket(Unpooled.wrappedBuffer(payload));
    ByteBuf output = Unpooled.buffer(requestPacket.getSize(RequestType.PROTOCOL_VERSION));
    requestPacket.write(output, RequestType.PROTOCOL_VERSION);

    MetricRegistry registry = new MetricRegistry();
    EmbeddedChannel ech = new EmbeddedChannel(new MemqResponseEncoder(registry),
        new LengthFieldBasedFrameDecoder(ByteOrder.BIG_ENDIAN, 2 * 1024 * 1024, 0, 4, 0, 0, false),
        new MemqRequestDecoder(null, null, null, registry));
    ech.writeInbound(output);
    ech.checkException();
    assertEquals(1, ech.outboundMessages().size());
    ByteBuf val = ech.readOutbound();
    ech.close();

    String key = MemqCommonClient.makeResponseKey(requestPacket);
    ResponseHandler responseHandler = new ResponseHandler();

    final AtomicReference<ResponsePacket> p = new AtomicReference<>();
    Map<String, Consumer<ResponsePacket>> requestMap = new HashMap<>();
    requestMap.put(key, rp -> p.set(rp));
    responseHandler.setRequestMap(requestMap);
    ech = new EmbeddedChannel(new LengthFieldBasedFrameDecoder(ByteOrder.BIG_ENDIAN,
        4 * 1024 * 1024, 0, Integer.BYTES, 0, 0, false),
        new MemqNettyClientSideResponseHandler(responseHandler));
    ech.writeInbound(val);
    ech.checkException();

    assertEquals(500, p.get().getResponseCode());
  }

  @Test
  public void testRedirection() throws Exception {
    MemqConfig configuration = new MemqConfig();
    NettyServerConfig nettyServerConfig = new NettyServerConfig();
    nettyServerConfig.setPort((short) 23433);
    configuration.setNettyServerConfig(nettyServerConfig);
    TopicConfig topicConfig = new TopicConfig("test", "delayeddevnull");
    topicConfig.setBufferSize(1024 * 1024);
    topicConfig.setBatchSizeMB(2);
    topicConfig.setRingBufferSize(128);
    topicConfig.setTickFrequencyMillis(10);
    topicConfig.setBatchMilliSeconds(2000);
    Properties outputHandlerConfig = new Properties();
    outputHandlerConfig.setProperty("delay.min.millis", "1");
    outputHandlerConfig.setProperty("delay.max.millis", "2");
    topicConfig.setStorageHandlerConfig(outputHandlerConfig);
    configuration.setTopicConfig(new TopicConfig[] { topicConfig });
    MemqManager memqManager = new MemqManager(null, configuration, new HashMap<>());
    memqManager.init();

    EnvironmentProvider provider = new TestEnvironmentProvider();
    MemqGovernor governor = new MemqGovernor(memqManager, configuration, provider);
    TopicMetadata md = new TopicMetadata("test2", "delayeddevnull", new Properties());
    md.getWriteBrokers()
        .add(new Broker("127.0.0.2", (short) 9092, "2xl", "us-east-1a", BrokerType.WRITE, new HashSet<>()));
    governor.getTopicMetadataMap().put("test2", md);
    MemqNettyServer server = new MemqNettyServer(configuration, memqManager, governor,
        new HashMap<>(), null);
    server.initialize();

    Properties auditConfigs = new Properties();
    auditConfigs.setProperty("class", "com.pinterest.memq.core.rpc.TestAuditor");
    // run tests
    MemqNettyProducer<byte[], byte[]> producer = new MemqNettyProducer<>("testcluster",
        new InetSocketAddress("localhost", 23434), "test2", 10, 1000000, Compression.NONE, false,
        10, 60_000, "local", 60_000, auditConfigs, null);
    producer.setKeySerializer(new ByteArraySerializer());
    producer.setValueSerializer(new ByteArraySerializer());
    byte[] value = UUID.randomUUID().toString().getBytes();
    Set<Future<MemqWriteResult>> futures = new LinkedHashSet<>();
    for (int i = 0; i < 100; i++) {
      Future<MemqWriteResult> writeToTopic = producer.writeToTopic(null, value);
      futures.add(writeToTopic);
    }
    producer.finalizeRequest();
    System.out.println("Writes completed");
    try {
      for (Future<MemqWriteResult> future : futures) {
        future.get();
      }
      fail("Must throw execution exception since redirection is being invoked");
    } catch (ExecutionException e) {
    }
    producer.close();
    server.stop();
  }

  @Test
  public void testLocalServer() throws Exception {
    long startMs = System.currentTimeMillis();
    MemqConfig configuration = new MemqConfig();
    NettyServerConfig nettyServerConfig = new NettyServerConfig();
    nettyServerConfig.setPort((short) 23434);
    configuration.setNettyServerConfig(nettyServerConfig);
    TopicConfig topicConfig = new TopicConfig("test", "delayeddevnull");
    topicConfig.setBufferSize(1024 * 1024);
    topicConfig.setBatchSizeMB(2);
    topicConfig.setRingBufferSize(128);
    topicConfig.setTickFrequencyMillis(10);
    topicConfig.setBatchMilliSeconds(2000);
    Properties outputHandlerConfig = new Properties();
    outputHandlerConfig.setProperty("delay.min.millis", "1");
    outputHandlerConfig.setProperty("delay.max.millis", "2");
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
    Timer timer = MiscUtils.oneMinuteWindowTimer(new MetricRegistry(), "requests.write.time");
    for (int i = 0; i < 1_000_000; i++) {
      Timer.Context ctx = timer.time();
      Future<MemqWriteResult> writeToTopic = producer.writeToTopic(null, value);
      ctx.stop();
      if (i % 10000 == 0) {
        System.out.println(timer.getSnapshot().get99thPercentile() / 1_000_000);
      }
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
    assertEquals(futures.size(), DelayedDevNullStorageHandler.getCounter());
    assertEquals(futures.size(), TestAuditor.getAuditMessageList().size());

    assertEquals(MemqNettyRequest.getByteCounter(), DelayedDevNullStorageHandler.getByteCounter());
    assertEquals(MemqNettyRequest.getByteCounter(), DelayedDevNullStorageHandler.getInputStreamCounter());
    assertEquals(MemqNettyRequest.getAckedByteCounter(), MemqNettyRequest.getByteCounter());
    TestAuditor.reset();

    producer = new MemqNettyProducer<>("testcluster",
        new InetSocketAddress("localhost", 23434), "test", 10, 1000000, Compression.NONE, true, 10,
        60_000, "local", 60_000, auditConfigs, null);
    producer.setKeySerializer(new ByteArraySerializer());
    producer.setValueSerializer(new ByteArraySerializer());
    value = UUID.randomUUID().toString().getBytes();
    futures = new LinkedHashSet<>();
    timer = MiscUtils.oneMinuteWindowTimer(new MetricRegistry(), "requests.write.time");
    for (int i = 0; i < 1_000_000; i++) {
      Timer.Context ctx = timer.time();
      Future<MemqWriteResult> writeToTopic = producer.writeToTopic(null, value);
      ctx.stop();
      if (i % 10000 == 0) {
        System.out.println(timer.getSnapshot().get99thPercentile() / 1_000_000);
      }
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

    server.getChildGroup().shutdownGracefully().sync();
    server.getParentGroup().shutdownGracefully().sync();
    server.getServerChannelFuture().channel().closeFuture().sync();
  }

  @Test
  public void testLocalServerSSL() throws Exception {
    int port = 23435;
    String keystorePath = "target/testks.jks";
    new File(keystorePath).delete();
    MemqConfig configuration = new MemqConfig();
    NettyServerConfig nettyServerConfig = new NettyServerConfig();
    nettyServerConfig.setPort((short) port);

    SelfSignedCertificate cert = new SelfSignedCertificate("test-memq.pinterest.com");
    KeyStore ks = KeyStore.getInstance(KeyStore.getDefaultType());
    ks.load(null, null);
    ks.setCertificateEntry("default", cert.cert());
    ks.setEntry("default", new PrivateKeyEntry(cert.key(), new X509Certificate[] { cert.cert() }),
        new KeyStore.PasswordProtection("test".toCharArray()));
    String keystorePassword = "test";
    ks.store(new FileOutputStream(new File(keystorePath)), keystorePassword.toCharArray());

    SSLConfig sslConfig = new SSLConfig();
    sslConfig.setProtocols(Collections.singletonList("TLSv1.2"));

    sslConfig.setKeystorePassword(keystorePassword);
    sslConfig.setKeystoreType(JKS);
    sslConfig.setKeystorePath(keystorePath);

    sslConfig.setTruststorePassword(keystorePassword);
    sslConfig.setTruststorePath(keystorePath);
    sslConfig.setTruststoreType(JKS);

    nettyServerConfig.setSslConfig(sslConfig);
    configuration.setNettyServerConfig(nettyServerConfig);
    TopicConfig topicConfig = new TopicConfig(keystorePassword, "delayeddevnull");
    topicConfig.setBufferSize(1024 * 1024);
    topicConfig.setBatchSizeMB(2);
    topicConfig.setRingBufferSize(128);
    topicConfig.setTickFrequencyMillis(10);
    topicConfig.setBatchMilliSeconds(2000);
    Properties outputHandlerConfig = new Properties();
    outputHandlerConfig.setProperty("delay.min.millis", "1");
    outputHandlerConfig.setProperty("delay.max.millis", "2");
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
        new InetSocketAddress("localhost", port), keystorePassword, 10, 1000000, Compression.NONE,
        false, 10, 60_000, "local", 60_000, auditConfigs, sslConfig);
    producer.setKeySerializer(new ByteArraySerializer());
    producer.setValueSerializer(new ByteArraySerializer());
    byte[] value = UUID.randomUUID().toString().getBytes();
    Set<Future<MemqWriteResult>> futures = new LinkedHashSet<>();
    Timer timer = MiscUtils.oneMinuteWindowTimer(new MetricRegistry(), "requests.write.time");
    for (int i = 0; i < 1_000_000; i++) {
      Timer.Context ctx = timer.time();
      Future<MemqWriteResult> writeToTopic = producer.writeToTopic(null, value);
      ctx.stop();
      if (i % 10000 == 0) {
        System.out.println("[" + i + "/1000000]" +
            "\twrite time: " + timer.getSnapshot().get99thPercentile() / 1_000_000);
      }
      futures.add(writeToTopic);
    }
    producer.finalizeRequest();
    for (Future<MemqWriteResult> future : futures) {
      future.get();
    }
    System.out.println("Completed all writes");
    Thread.sleep(1000);
    producer.close();

    // server should have written 55 batches
    assertEquals(futures.size(), DelayedDevNullStorageHandler.getCounter());
    assertEquals(futures.size(), TestAuditor.getAuditMessageList().size());

    server.getChildGroup().shutdownGracefully().sync();
    server.getParentGroup().shutdownGracefully().sync();
    server.getServerChannelFuture().channel().closeFuture().sync();
  }

  @Test
  public void testMetadataRequests() throws Exception {
    short port = 22311;

    MemqConfig configuration = new MemqConfig();
    NettyServerConfig nettyServerConfig = new NettyServerConfig();
    nettyServerConfig.setPort((short) port);
    configuration.setNettyServerConfig(nettyServerConfig);
    TopicConfig topicConfig = new TopicConfig("test", "delayeddevnull");
    topicConfig.setBufferSize(1024 * 1024);
    topicConfig.setBatchSizeMB(2);
    topicConfig.setRingBufferSize(128);
    topicConfig.setTickFrequencyMillis(10);
    topicConfig.setBatchMilliSeconds(2000);
    Properties outputHandlerConfig = new Properties();
    outputHandlerConfig.setProperty("delay.min.millis", "1");
    outputHandlerConfig.setProperty("delay.max.millis", "2");
    topicConfig.setStorageHandlerConfig(outputHandlerConfig);
    configuration.setTopicConfig(new TopicConfig[] { topicConfig });
    MemqManager memqManager = new MemqManager(null, configuration, new HashMap<>());
    memqManager.init();

    EnvironmentProvider provider = new TestEnvironmentProvider();
    MemqGovernor governor = new MemqGovernor(memqManager, configuration, provider);

    TopicMetadata md = new TopicMetadata("test2", "delayeddevnull", new Properties());
    md.getWriteBrokers()
        .add(new Broker("127.0.0.1", port, "2xl", "us-east-1a", BrokerType.WRITE, new HashSet<>()));
    governor.getTopicMetadataMap().put("test2", md);

    MemqNettyServer server = new MemqNettyServer(configuration, memqManager, governor,
        new HashMap<>(), null);
    server.initialize();

    Properties properties = new Properties();
    properties.put(ConsumerConfigs.CLUSTER, "test");
    properties.put(ConsumerConfigs.CLIENT_ID, "test");
    properties.put(ConsumerConfigs.GROUP_ID, "test231231");
    properties.put(ConsumerConfigs.DRY_RUN_KEY, "true");
    properties.put(ConsumerConfigs.BOOTSTRAP_SERVERS, "localhost:" + port);
    properties.put(ConsumerConfigs.DIRECT_CONSUMER, "false");
    StorageHandler input = getEmptyTestInput();
    MemqConsumer<byte[], byte[]> consumer = new MemqConsumer<byte[], byte[]>(properties, input);
    long ts = System.currentTimeMillis();
    TopicMetadata topicMetadata = consumer.getTopicMetadata("test2", 10000);
    ts = System.currentTimeMillis() - ts;
    System.out.println("Fetched metadata in:" + ts + "ms");

    assertNotNull(topicMetadata);
    assertEquals("test2", topicMetadata.getTopicName());
    assertEquals(1, topicMetadata.getWriteBrokers().size());
    consumer.close();

    // test producer metadata requests
    Properties auditConfigs = new Properties();
    auditConfigs.setProperty("class", "com.pinterest.memq.core.rpc.TestAuditor");
    MemqProducer<byte[], byte[]> producer = new MemqNettyProducer<>("testcluster",
        new InetSocketAddress("localhost", port), "test2", 10, 1000000, Compression.NONE, false, 10,
        60_000, "local", 60_000, auditConfigs, null);
    producer.awaitConnect(100, TimeUnit.SECONDS);
    producer.close();

    properties.setProperty(ProducerConfigs.CLIENT_TYPE, "TCP");
    properties.setProperty(ProducerConfigs.KEY_SERIALIZER,
        ByteArraySerializer.class.getCanonicalName());
    properties.setProperty(ProducerConfigs.VALUE_SERIALIZER,
        ByteArraySerializer.class.getCanonicalName());
    properties.setProperty(ProducerConfigs.TOPIC_NAME, "test2");
    producer = MemqProducer.getInstance(properties);
    producer.close();

    server.getChildGroup().shutdownGracefully().sync();
    server.getParentGroup().shutdownGracefully().sync();
    server.getServerChannelFuture().channel().closeFuture().sync();
  }

  public static StorageHandler getEmptyTestInput() {
    StorageHandler input = new StorageHandler() {

      @Override
      public InputStream fetchBatchStreamForNotification(JsonObject nextNotificationToProcess) {
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
      public void writeOutput(int sizeInBytes,
                              int checksum,
                              List<Message> messages) throws WriteFailedException {
      }

      @Override
      public String getReadUrl() {
        return null;
      }

    };
    return input;
  }

  @Test
  public void testInvalidChecksumRejection() throws Exception {
    TopicConfig topicConfig = new TopicConfig("test", "delayeddevnull");
    topicConfig.setBufferSize(1024 * 1024);
    topicConfig.setBatchSizeMB(2);
    topicConfig.setRingBufferSize(128);
    topicConfig.setTickFrequencyMillis(10);
    topicConfig.setBatchMilliSeconds(2000);

    short port = 23436;
    MemqNettyServer server = createAndStartServer(topicConfig, port);

    Properties auditConfigs = new Properties();
    auditConfigs.setProperty("class", "com.pinterest.memq.core.rpc.TestAuditor");
    // run tests
    MemqNettyProducer<byte[], byte[]> producer = new MemqNettyProducer<>("testcluster",
        new InetSocketAddress("localhost", port), "test", 10, 1000000, Compression.NONE, false, 10,
        60_000, "local", 60_000, auditConfigs, null);
    producer.setKeySerializer(new ByteArraySerializer());
    producer.setValueSerializer(new ByteArraySerializer());
    producer.setDebug();
    byte[] value = UUID.randomUUID().toString().getBytes();
    Set<Future<MemqWriteResult>> futures = new LinkedHashSet<>();
    for (int i = 0; i < 50; i++) {
      if (i % 2 == 0) {
        MemqNettyRequest.setOverrideDebugChecksum(1);
      } else {
        MemqNettyRequest.setOverrideDebugChecksum(0);
      }
      Future<MemqWriteResult> writeToTopic = producer.writeToTopic(null, value);
      futures.add(writeToTopic);
      producer.finalizeRequest();
      Thread.sleep(10L);
    }

    int invalidRequests = 0;
    for (Future<MemqWriteResult> future : futures) {
      try {
        future.get();
      } catch (Exception e) {
        invalidRequests++;
      }
    }
    System.out.println("Number of invalid requests:" + invalidRequests);
    Thread.sleep(500);
    producer.close();

    assertTrue(invalidRequests > 0);
    assertTrue(MemqNettyRequest.getByteCounter() > MemqNettyRequest.getAckedByteCounter());
    assertEquals(MemqNettyRequest.getAckedByteCounter(), DelayedDevNullStorageHandler.getByteCounter());
    assertEquals(MemqNettyRequest.getAckedByteCounter(),
        DelayedDevNullStorageHandler.getInputStreamCounter());

    System.out
        .println("Number of bytes written on producer: " + MemqNettyRequest.getAckedByteCounter());

    server.getChildGroup().shutdownGracefully().sync();
    server.getParentGroup().shutdownGracefully().sync();
    server.getServerChannelFuture().channel().closeFuture().sync();
  }

  public static void createTopic(String notificationTopic, String kafkaConnectString, String auditTopic) {
    Properties adminProps = new Properties();
    adminProps.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaConnectString);
    adminProps.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
        ByteArrayDeserializer.class.getCanonicalName());
    adminProps.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
        ByteArrayDeserializer.class.getCanonicalName());
    adminProps.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "test12");
    adminProps.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    AdminClient admin = AdminClient.create(adminProps);
    admin.createTopics(Arrays.asList(new NewTopic(notificationTopic, 1, (short) 1)));
    admin.createTopics(Arrays.asList(new NewTopic(auditTopic, 1, (short) 1)));
    admin.close();
  }

  public static MemqNettyServer createAndStartServer(TopicConfig topicConfig,
                                               short port) throws UnknownHostException, Exception {
    MemqConfig configuration = new MemqConfig();
    NettyServerConfig nettyServerConfig = new NettyServerConfig();
    nettyServerConfig.setPort(port);
    configuration.setNettyServerConfig(nettyServerConfig);

    configuration.setTopicConfig(new TopicConfig[] { topicConfig });
    MemqManager memqManager = new MemqManager(null, configuration, new HashMap<>());
    memqManager.init();

    EnvironmentProvider provider = new TestEnvironmentProvider();
    MemqGovernor governor = new MemqGovernor(memqManager, configuration, provider);
    MemqNettyServer server = new MemqNettyServer(configuration, memqManager, governor,
        new HashMap<>(), null);
    server.initialize();
    return server;
  }

}

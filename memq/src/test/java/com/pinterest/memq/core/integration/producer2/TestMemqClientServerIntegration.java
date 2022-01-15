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
package com.pinterest.memq.core.integration.producer2;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileOutputStream;
import java.net.UnknownHostException;
import java.security.KeyStore;
import java.security.KeyStore.PrivateKeyEntry;
import java.security.cert.X509Certificate;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.pinterest.memq.client.commons.Compression;
import com.pinterest.memq.client.commons.serde.ByteArraySerializer;
import com.pinterest.memq.client.commons2.network.NetworkClient;
import com.pinterest.memq.client.commons2.network.netty.ClientChannelInitializer;
import com.pinterest.memq.client.producer.MemqWriteResult;
import com.pinterest.memq.client.producer2.MemqProducer;
import com.pinterest.memq.commons.config.SSLConfig;
import com.pinterest.memq.commons.protocol.RequestPacket;
import com.pinterest.memq.commons.protocol.RequestType;
import com.pinterest.memq.commons.protocol.TopicConfig;
import com.pinterest.memq.commons.protocol.WriteRequestPacket;
import com.pinterest.memq.commons.storage.DelayedDevNullStorageHandler;
import com.pinterest.memq.core.MemqManager;
import com.pinterest.memq.core.clustering.MemqGovernor;
import com.pinterest.memq.core.config.EnvironmentProvider;
import com.pinterest.memq.core.config.MemqConfig;
import com.pinterest.memq.core.config.NettyServerConfig;
import com.pinterest.memq.core.integration.TestEnvironmentProvider;
import com.pinterest.memq.core.rpc.MemqNettyServer;
import com.pinterest.memq.core.rpc.TestAuditor;
import com.salesforce.kafka.test.junit4.SharedKafkaTestResource;
import com.salesforce.kafka.test.listeners.PlainListener;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.handler.ssl.util.SelfSignedCertificate;

public class TestMemqClientServerIntegration {

  private static final String JKS = "JKS";

  @ClassRule
  public static final SharedKafkaTestResource sharedKafkaTestResource = new SharedKafkaTestResource()
      .withBrokers(1).registerListener(new PlainListener().onPorts(9092));

  @Before
  @After
  public void reset() {
    new File(new MemqConfig().getTopicCacheFile()).delete();
    DelayedDevNullStorageHandler.reset();
    TestAuditor.reset();
  }

  @Test
  public void testLocalServer() throws Exception {
    long startMs = System.currentTimeMillis();
    DelayedDevNullStorageHandler.reset();
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

    Properties networkConfigs = new Properties();
    networkConfigs.put(NetworkClient.CONFIG_CONNECT_TIMEOUT_MS, "5000");
    Properties auditConfigs = new Properties();
    auditConfigs.setProperty("class", "com.pinterest.memq.core.rpc.TestAuditor");
    // run tests
    MemqProducer.Builder<byte[], byte[]> builder = new MemqProducer.Builder<byte[], byte[]>()
        .cluster("testcluster")
        .bootstrapServers("localhost:23434")
        .topic("test")
        .maxInflightRequests(10)
        .maxPayloadBytes(1000000)
        .lingerMs(500)
        .compression(Compression.NONE)
        .disableAcks(false)
        .sendRequestTimeout(60_000)
        .networkProperties(networkConfigs)
        .locality("local")
        .auditProperties(auditConfigs)
        .keySerializer(new ByteArraySerializer())
        .valueSerializer(new ByteArraySerializer())
        .metricRegistry(new MetricRegistry());


    MemqProducer<byte[], byte[]> producer = builder.build();
    Map<String, Timer> timers;
    Set<Future<MemqWriteResult>> futures = new LinkedHashSet<>();
    byte[] value = new byte[36];
    ThreadLocalRandom.current().nextBytes(value);
    for (int i = 0; i < 1_000_000; i++) {
      if (i % 10000 == 0) {
        timers = producer.getMetricRegistry().getTimers();
        System.out.println("[" + i + "/1000000]" +
            "\tTotal write time: " + timers.get("producer.write.time").getSnapshot().get99thPercentile() / 1_000_000 +
            "\tInflight requests: " + producer.getMetricRegistry().getGauges().get("requests.inflight").getValue() +
            (timers.get("requests.write.time") != null ? "\tActual write time: " + timers.get("requests.write.time").getSnapshot().get99thPercentile() / 1_000_000 : "" ) +
            ""
        );
      }
      Future<MemqWriteResult> writeToTopic = producer.write(null, value);
      futures.add(writeToTopic);
    }
    System.out.println("Sent all writes: " + (System.currentTimeMillis() - startMs));
    producer.flush();
    for (Future<MemqWriteResult> future : futures) {
      future.get(30, TimeUnit.SECONDS);
    }
    System.out.println("Completed all writes: " + (System.currentTimeMillis() - startMs));
    Thread.sleep(500);
    producer.close();

    assertEquals(futures.size(), DelayedDevNullStorageHandler.getCounter());
    assertEquals(futures.size(), TestAuditor.getAuditMessageList().size());

    Map<String, Counter> counters = producer.getMetricRegistry().getCounters();

    assertEquals(counters.get("requests.sent.bytes").getCount(), DelayedDevNullStorageHandler.getByteCounter());
    assertEquals(counters.get("requests.sent.bytes").getCount(), DelayedDevNullStorageHandler.getInputStreamCounter());
    assertEquals(counters.get("requests.acked.bytes").getCount(), counters.get("requests.sent.bytes").getCount());
    TestAuditor.reset();

    builder.disableAcks(true);
    builder.metricRegistry(new MetricRegistry());
    producer = builder.build();
    futures = new LinkedHashSet<>();
    for (int i = 0; i < 1_000_000; i++) {
      if (i % 10000 == 0) {
        timers = producer.getMetricRegistry().getTimers();
        System.out.println("[" + i + "/1000000]" +
            "\tTotal write time: " + timers.get("producer.write.time").getSnapshot().get99thPercentile() / 1_000_000 +
            "\tInflight requests: " + producer.getMetricRegistry().getGauges().get("requests.inflight").getValue() +
            (timers.get("requests.write.time") != null ? "\tActual write time: " + timers.get("requests.write.time").getSnapshot().get99thPercentile() / 1_000_000 : "" ) +
            ""
        );
      }
      Future<MemqWriteResult> writeToTopic = producer.write(null, value);
      futures.add(writeToTopic);
    }
    System.out.println("Sent all writes: " + (System.currentTimeMillis() - startMs));
    producer.flush();
    for (Future<MemqWriteResult> future : futures) {
      future.get(30, TimeUnit.SECONDS);
    }
    System.out.println("Completed all writes: " + (System.currentTimeMillis() - startMs));
    Thread.sleep(500);
    producer.close();

    counters = producer.getMetricRegistry().getCounters();

    assertEquals(futures.size(), TestAuditor.getAuditMessageList().size());
    assertEquals(counters.get("requests.acked.bytes").getCount(), counters.get("requests.sent.bytes").getCount());

    server.getChildGroup().shutdownGracefully().sync();
    server.getParentGroup().shutdownGracefully().sync();
    server.getServerChannelFuture().channel().closeFuture().sync();
    server.stop();
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
    MemqProducer.Builder<byte[], byte[]> builder = new MemqProducer.Builder<byte[], byte[]>()
        .cluster("testcluster")
        .bootstrapServers("localhost:" + port)
        .topic("test")
        .maxInflightRequests(30)
        .maxPayloadBytes(1000000)
        .lingerMs(500)
        .compression(Compression.NONE)
        .disableAcks(false)
        .sendRequestTimeout(60_000)
        .locality("local")
        .auditProperties(auditConfigs)
        .keySerializer(new ByteArraySerializer())
        .valueSerializer(new ByteArraySerializer())
        .sslConfig(sslConfig)
        .metricRegistry(new MetricRegistry());

    MemqProducer<byte[], byte[]> producer = builder.build();
    byte[] value = UUID.randomUUID().toString().getBytes();
    Set<Future<MemqWriteResult>> futures = new LinkedHashSet<>();
    Map<String, Timer> timers;
    for (int i = 0; i < 1_000_000; i++) {
      if (i % 10000 == 0) {
        timers = producer.getMetricRegistry().getTimers();
        System.out.println("[" + i + "/1000000]" +
            "\tTotal write time: " + timers.get("producer.write.time").getSnapshot().get99thPercentile() / 1_000_000 +
            (timers.get("requests.write.time") != null ? "\tActual write time: " + timers.get("requests.write.time").getSnapshot().get99thPercentile() / 1_000_000 : "" ) +
            ""
        );
      }
      Future<MemqWriteResult> writeToTopic = producer.write(null, value);
      futures.add(writeToTopic);
    }
    producer.flush();
    for (Future<MemqWriteResult> future : futures) {
      future.get();
    }
    System.out.println("Completed all writes");
    Thread.sleep(1000);
    producer.close();

    assertEquals(futures.size(), DelayedDevNullStorageHandler.getCounter());
    assertEquals(futures.size(), TestAuditor.getAuditMessageList().size());

    server.getChildGroup().shutdownGracefully().sync();
    server.getParentGroup().shutdownGracefully().sync();
    server.getServerChannelFuture().channel().closeFuture().sync();
    server.stop();
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
    MemqProducer.Builder<byte[], byte[]> builder = new MemqProducer.Builder<byte[], byte[]>()
        .cluster("testcluster")
        .bootstrapServers("localhost:" + port)
        .topic("test")
        .maxInflightRequests(30)
        .maxPayloadBytes(1000000)
        .lingerMs(500)
        .compression(Compression.NONE)
        .disableAcks(false)
        .sendRequestTimeout(60_000)
        .locality("local")
        .auditProperties(auditConfigs)
        .keySerializer(new ByteArraySerializer())
        .valueSerializer(new ByteArraySerializer())
        .metricRegistry(new MetricRegistry());

    ChannelHandler wiretapper = new CRCTamperHandler();

    ClientChannelInitializer.setWiretapper(wiretapper);

    MemqProducer<byte[], byte[]> producer = builder.build();

    byte[] value = UUID.randomUUID().toString().getBytes();
    Set<Future<MemqWriteResult>> futures = new LinkedHashSet<>();
    for (int i = 0; i < 50; i++) {
      Future<MemqWriteResult> writeToTopic = producer.write(null, value);
      futures.add(writeToTopic);
      producer.flush();
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
    Map<String, Counter> counters = producer.getMetricRegistry().getCounters();
    long ackedBytes = counters.get("requests.acked.bytes").getCount();

    assertTrue(invalidRequests > 0);
    assertTrue(counters.get("requests.sent.bytes").getCount() > ackedBytes);
    assertEquals(ackedBytes, DelayedDevNullStorageHandler.getByteCounter());
    assertEquals(ackedBytes, DelayedDevNullStorageHandler.getInputStreamCounter());

    System.out
        .println("Number of bytes written on producer: " + ackedBytes);

    ClientChannelInitializer.setWiretapper(null);
    server.getChildGroup().shutdownGracefully().sync();
    server.getParentGroup().shutdownGracefully().sync();
    server.getServerChannelFuture().channel().closeFuture().sync();
    server.stop();
  }

  MemqNettyServer createAndStartServer(TopicConfig topicConfig,
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

  @ChannelHandler.Sharable
  static class CRCTamperHandler extends ChannelOutboundHandlerAdapter {
    private final AtomicInteger count = new AtomicInteger(0);

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise)
        throws Exception {
      RequestPacket req = new RequestPacket();
      ByteBuf b = (ByteBuf) msg;
      ByteBuf dup = b.duplicate();
      req.readFields(b, RequestType.PROTOCOL_VERSION);
      System.out.println(req.getRequestType());
      if (req.getRequestType().equals(RequestType.WRITE)) {
        WriteRequestPacket wrp = (WriteRequestPacket) req.getPayload();
        if (count.getAndIncrement() % 2 == 0) {
          System.out.println("Tampering request: " + req.getClientRequestId());
          wrp = new WriteRequestPacket(wrp.isDisableAcks(), wrp.getTopicName().getBytes(), wrp.isChecksumExists(), wrp.getChecksum() + 1, wrp.getData());
          ByteBuf buf = PooledByteBufAllocator.DEFAULT.buffer(b.capacity());
          req = new RequestPacket(req.getProtocolVersion(), req.getClientRequestId(), req.getRequestType(), wrp);
          req.write(buf, RequestType.PROTOCOL_VERSION);
          msg = buf;
          b.release();
        } else {
          msg = dup;
        }
      } else {
        msg = dup;
      }
      super.write(ctx, msg, promise);
    }
  }
}

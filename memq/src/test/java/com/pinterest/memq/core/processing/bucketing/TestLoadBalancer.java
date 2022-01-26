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

import com.pinterest.memq.client.commons.Compression;
import com.pinterest.memq.client.commons.serde.ByteArraySerializer;
import com.pinterest.memq.client.producer.MemqWriteResult;
import com.pinterest.memq.client.producer2.MemqProducer;
import com.pinterest.memq.commons.protocol.RequestPacket;
import com.pinterest.memq.commons.protocol.TopicConfig;
import com.pinterest.memq.core.MemqManager;
import com.pinterest.memq.core.clustering.MemqGovernor;
import com.pinterest.memq.core.config.EnvironmentProvider;
import com.pinterest.memq.core.config.MemqConfig;
import com.pinterest.memq.core.config.NettyServerConfig;
import com.pinterest.memq.core.integration.TestEnvironmentProvider;
import com.pinterest.memq.core.load.BalancingStrategy;
import com.pinterest.memq.core.load.LoadBalancer;
import com.pinterest.memq.core.processing.TopicProcessor;
import com.pinterest.memq.core.rpc.MemqNettyServer;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import io.netty.channel.Channel;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.util.ResourceLeakDetector;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

public class TestLoadBalancer {
  protected static final String LOCALHOST_STRING = "127.0.0.1";
  protected short port = -1;

  @BeforeClass
  public static void setup() {
    ResourceLeakDetector.setLevel(ResourceLeakDetector.Level.PARANOID);
  }

  @Before
  public void generateRandomPort() {
    short newPort = -1;
    while (port == newPort) {
      newPort = (short) ThreadLocalRandom.current().nextInt(20000, 30000);
    }
    port = newPort;
  }

  @Before
  public void reset() {
    TestBalancingStrategy.reset();
  }

  @Test
  public void testSimpleLoad() throws Exception {
    MemqConfig configuration = new MemqConfig();
    NettyServerConfig nettyServerConfig = new NettyServerConfig();
    nettyServerConfig.setPort(port);
    configuration.setNettyServerConfig(nettyServerConfig);
    TopicConfig topicConfig = new TopicConfig("test", "delayeddevnull");
    topicConfig.setEnableBucketing2Processor(true);
    topicConfig.setOutputParallelism(60);
    topicConfig.setBufferSize(1024 * 1024);
    topicConfig.setBatchSizeMB(2);
    topicConfig.setRingBufferSize(4);
    topicConfig.setTickFrequencyMillis(10);
    topicConfig.setBatchMilliSeconds(100);
    Properties loadBalancerConfig = new Properties();
    loadBalancerConfig.setProperty("balancingStrategy", TestBalancingStrategy.class.getName());
    topicConfig.setLoadBalancerConfig(loadBalancerConfig);
    Properties outputHandlerConfig = new Properties();
    outputHandlerConfig.setProperty("delay.min.millis", "500");
    outputHandlerConfig.setProperty("delay.max.millis", "1000");
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
        .bootstrapServers(LOCALHOST_STRING + ":" + port)
        .topic("test")
        .maxInflightRequests(60)
        .maxPayloadBytes(1000000)
        .lingerMs(50)
        .compression(Compression.NONE)
        .disableAcks(false)
        .sendRequestTimeout(60_000)
        .locality("local")
        .auditProperties(auditConfigs)
        .keySerializer(new ByteArraySerializer())
        .valueSerializer(new ByteArraySerializer())
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
    Thread.sleep(500);
    producer.close();

    assertEquals(TestBalancingStrategy.getDropCount(), producer.getMetricRegistry().counter("responses.code.303").getCount());

    server.getChildGroup().shutdownGracefully().sync();
    server.getParentGroup().shutdownGracefully().sync();
    server.getServerChannelFuture().channel().closeFuture().sync();
  }

  public static class TestBalancingStrategy implements BalancingStrategy {
    private static final AtomicInteger counter = new AtomicInteger(0);
    private static final AtomicInteger dropCount = new AtomicInteger(0);

    @Override
    public void configure(Properties configs) {

    }

    @Override
    public LoadBalancer.Action evaluate(RequestPacket rawPacket, ChannelHandlerContext context,
                                        TopicProcessor topicProcessor) {
      if (counter.getAndIncrement() % 10 == 1) {
        dropCount.getAndIncrement();
        return LoadBalancer.Action.DROP;
      }
      return LoadBalancer.Action.NO_OP;
    }

    public static int getDropCount() {
      return dropCount.get();
    }

    public static void reset() {
      dropCount.set(0);
      counter.set(0);
    }
  }

}

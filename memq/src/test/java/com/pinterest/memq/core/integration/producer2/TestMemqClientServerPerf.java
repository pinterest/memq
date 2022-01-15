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

import com.pinterest.memq.client.commons.Compression;
import com.pinterest.memq.client.commons.serde.ByteArraySerializer;
import com.pinterest.memq.client.producer.MemqWriteResult;
import com.pinterest.memq.client.producer.netty.MemqNettyRequest;
import com.pinterest.memq.client.producer2.MemqProducer;
import com.pinterest.memq.commons.protocol.TopicConfig;
import com.pinterest.memq.commons.storage.DelayedDevNullStorageHandler;
import com.pinterest.memq.core.MemqManager;
import com.pinterest.memq.core.clustering.MemqGovernor;
import com.pinterest.memq.core.config.EnvironmentProvider;
import com.pinterest.memq.core.config.MemqConfig;
import com.pinterest.memq.core.config.NettyServerConfig;
import com.pinterest.memq.core.integration.TestEnvironmentProvider;
import com.pinterest.memq.core.rpc.MemqNettyServer;
import com.pinterest.memq.core.rpc.TestAuditor;
import com.pinterest.memq.core.utils.DaemonThreadFactory;

import com.codahale.metrics.MetricRegistry;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.ArrayList;
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
import java.util.concurrent.atomic.AtomicBoolean;

public class TestMemqClientServerPerf {

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
  public void testServerPerf() throws Exception {
    MemqConfig configuration = new MemqConfig();
    NettyServerConfig nettyServerConfig = new NettyServerConfig();
    nettyServerConfig.setPort((short) 23437);
    configuration.setNettyServerConfig(nettyServerConfig);
    TopicConfig topicConfig = new TopicConfig("test", "delayeddevnull");
    topicConfig.setBufferSize(4 * 1024 * 1024);
    topicConfig.setBatchSizeMB(4);
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

    int numOfProducerThreads = 4;
    ExecutorService es = Executors.newFixedThreadPool(numOfProducerThreads,
        DaemonThreadFactory.INSTANCE);

    final int TOTAL = 1_000_000;

    Properties auditConfigs = new Properties();
    auditConfigs.setProperty("class", "com.pinterest.memq.core.rpc.TestAuditor");
    MemqProducer.Builder<byte[], byte[]> builder = new MemqProducer.Builder<byte[], byte[]>()
        .cluster("testcluster")
        .bootstrapServers("localhost:" + 23437)
        .topic("test")
        .maxInflightRequests(10)
        .maxPayloadBytes(3000000)
        .lingerMs(5000)
        .compression(Compression.NONE)
        .disableAcks(false)
        .sendRequestTimeout(60_000)
        .locality("local")
        .auditProperties(auditConfigs)
        ;

    List<MetricRegistry> registries = new ArrayList<>();
    long startTime = System.currentTimeMillis();
    for (int i = 0; i < numOfProducerThreads; i++) {
      MetricRegistry registry = new MetricRegistry();
      registries.add(registry);
      es.submit(() -> {
        try {
          // run tests
          MemqProducer<byte[], byte[]> producer = new MemqProducer.Builder<>(builder)
              .metricRegistry(registry)
              .keySerializer(new ByteArraySerializer())
              .valueSerializer(new ByteArraySerializer())
              .build();
          byte[] value = UUID.randomUUID().toString().getBytes();
          Set<Future<MemqWriteResult>> futures = new LinkedHashSet<>();
          for (int j = 0; j < TOTAL; j++) {
            Future<MemqWriteResult> writeToTopic = producer.write(null, value);
            futures.add(writeToTopic);
          }
          producer.flush();

          for (Future<MemqWriteResult> future : futures) {
            future.get();
          }
          Thread.sleep(500);
          producer.close();
        } catch (Exception e) {
          e.printStackTrace();
        }
      });
    }

    final AtomicBoolean flg = new AtomicBoolean(true);

    Executors.newCachedThreadPool(DaemonThreadFactory.INSTANCE).submit(() -> {
      long prevBytes = 0;
      while (flg.get()) {
        long curBytes = DelayedDevNullStorageHandler.getByteCounter();
        System.out.println("Server Throughput: " + (curBytes - prevBytes) / 1024 / 1024 + "MB/s");
        prevBytes = curBytes;
        try {
          Thread.sleep(1_000);
        } catch (InterruptedException e) {
          break;
        }
      }
    });

    es.shutdown();
    es.awaitTermination(100, TimeUnit.SECONDS);
    long endTime = System.currentTimeMillis();
    long duration = (endTime - startTime) / 1000;
    System.out.println("Took " + duration + " seconds");
    assertEquals(registries.stream().mapToLong(mr -> mr.getCounters().get("requests.sent.bytes").getCount()).sum(), DelayedDevNullStorageHandler.getByteCounter());

    flg.set(false);

    server.getChildGroup().shutdownGracefully().sync();
    server.getParentGroup().shutdownGracefully().sync();
    server.getServerChannelFuture().channel().closeFuture().sync();
    assertTrue("Performance test should take less than 15 seconds", duration < 15);
  }

}

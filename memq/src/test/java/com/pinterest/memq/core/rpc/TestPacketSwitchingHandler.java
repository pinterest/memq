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
package com.pinterest.memq.core.rpc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import java.io.File;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import org.junit.Test;

import com.codahale.metrics.MetricRegistry;
import com.pinterest.memq.commons.protocol.Broker;
import com.pinterest.memq.commons.protocol.Broker.BrokerType;
import com.pinterest.memq.commons.protocol.RequestPacket;
import com.pinterest.memq.commons.protocol.RequestType;
import com.pinterest.memq.commons.protocol.ResponsePacket;
import com.pinterest.memq.commons.protocol.TopicMetadata;
import com.pinterest.memq.commons.protocol.TopicMetadataRequestPacket;
import com.pinterest.memq.commons.protocol.TopicMetadataResponsePacket;
import com.pinterest.memq.commons.protocol.TopicConfig;
import com.pinterest.memq.commons.protocol.WriteRequestPacket;
import com.pinterest.memq.core.MemqManager;
import com.pinterest.memq.core.clustering.MemqGovernor;
import com.pinterest.memq.core.config.ClusteringConfig;
import com.pinterest.memq.core.config.LocalEnvironmentProvider;
import com.pinterest.memq.core.config.MemqConfig;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.embedded.EmbeddedChannel;

public class TestPacketSwitchingHandler {

  private static class TestGovernor extends MemqGovernor {
    private final Set<Broker> brokers;

    public TestGovernor(MemqManager mgr,
                        MemqConfig config,
                        LocalEnvironmentProvider provider,
                        Set<Broker> brokers) {
      super(mgr, config, provider);
      this.brokers = brokers;
    }

    @Override
    public Set<Broker> getAllBrokers() {
      return brokers;
    }
  }

  private static class TestGovernorWithTopicConfig extends TestGovernor {
    private final TopicConfig topicConfig;

    public TestGovernorWithTopicConfig(MemqManager mgr,
                                       MemqConfig config,
                                       LocalEnvironmentProvider provider,
                                       Set<Broker> brokers,
                                       TopicConfig topicConfig) {
      super(mgr, config, provider, brokers);
      this.topicConfig = topicConfig;
    }

    @Override
    public TopicConfig getTopicConfig(String topic) {
      if (topicConfig != null && topicConfig.getTopic().equals(topic)) {
        return topicConfig;
      }
      return null;
    }
  }

  private static class PacketSwitchingAdapter extends ChannelInboundHandlerAdapter {
    private final PacketSwitchingHandler handler;

    public PacketSwitchingAdapter(PacketSwitchingHandler handler) {
      this.handler = handler;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
      handler.handle(ctx, (RequestPacket) msg, null, null);
    }
  }

  @Test
  public void testMetadataAllBrokersWhenAssignmentsDisabled() throws Exception {
    MemqConfig config = new MemqConfig();
    ClusteringConfig clusteringConfig = new ClusteringConfig();
    clusteringConfig.setEnableAssignments(false);
    config.setClusteringConfig(clusteringConfig);
    File tmpFile = File.createTempFile("testmgrcache", "", Paths.get("/tmp").toFile());
    tmpFile.deleteOnExit();
    config.setTopicCacheFile(tmpFile.toString());
    config.setTopicConfig(new TopicConfig[] {
        new TopicConfig("test", "delayeddevnull")
    });

    MemqManager mgr = new MemqManager(null, config, new HashMap<>());
    mgr.init();

    Set<Broker> brokers = new HashSet<>(Arrays.asList(
        new Broker("1.1.1.1", (short) 9092, "test", "local", BrokerType.READ_WRITE, new HashSet<>()),
        new Broker("1.1.1.2", (short) 9092, "test", "local", BrokerType.READ_WRITE, new HashSet<>())
    ));

    MemqGovernor governor = new TestGovernor(mgr, config, new LocalEnvironmentProvider(), brokers);
    PacketSwitchingHandler handler = new PacketSwitchingHandler(mgr, governor, null, new MetricRegistry());

    EmbeddedChannel channel = new EmbeddedChannel(new PacketSwitchingAdapter(handler));
    RequestPacket request = new RequestPacket((short) 0, 1L, RequestType.TOPIC_METADATA,
        new TopicMetadataRequestPacket("test"));
    channel.writeInbound(request);

    ResponsePacket response = channel.readOutbound();
    assertNotNull(response);
    assertEquals(RequestType.TOPIC_METADATA, response.getRequestType());

    TopicMetadataResponsePacket payload = (TopicMetadataResponsePacket) response.getPacket();
    TopicMetadata metadata = payload.getMetadata();
    assertNotNull(metadata);
    assertEquals(2, metadata.getReadBrokers().size());
    assertEquals(2, metadata.getWriteBrokers().size());
  }

  @Test
  public void testUnknownTopicMetadataAssignmentsEnabled() throws Exception {
    MemqConfig config = new MemqConfig();
    ClusteringConfig clusteringConfig = new ClusteringConfig();
    clusteringConfig.setEnableAssignments(true);
    config.setClusteringConfig(clusteringConfig);
    config.setTopicConfig(new TopicConfig[] {
        new TopicConfig("known", "delayeddevnull")
    });

    MemqManager mgr = new MemqManager(null, config, new HashMap<>());
    mgr.init();

    MemqGovernor governor = new TestGovernor(mgr, config, new LocalEnvironmentProvider(),
        Collections.emptySet());
    PacketSwitchingHandler handler = new PacketSwitchingHandler(mgr, governor, null, new MetricRegistry());

    EmbeddedChannel channel = new EmbeddedChannel(new PacketSwitchingAdapter(handler));
    RequestPacket request = new RequestPacket((short) 0, 1L, RequestType.TOPIC_METADATA,
        new TopicMetadataRequestPacket("unknown"));
    try {
      channel.writeInbound(request);
      fail("Expected TOPIC_NOT_FOUND for unknown topic when assignments are enabled");
    } catch (Exception e) {
      assertEquals(PacketSwitchingHandler.TOPIC_NOT_FOUND, e);
    }
  }

  @Test
  public void testUnknownTopicMetadataAssignmentsDisabled() throws Exception {
    MemqConfig config = new MemqConfig();
    ClusteringConfig clusteringConfig = new ClusteringConfig();
    clusteringConfig.setEnableAssignments(false);
    config.setClusteringConfig(clusteringConfig);

    MemqManager mgr = new MemqManager(null, config, new HashMap<>());
    mgr.init();

    Set<Broker> brokers = new HashSet<>(Arrays.asList(
        new Broker("1.1.1.1", (short) 9092, "test", "local", BrokerType.READ_WRITE, new HashSet<>())
    ));

    MemqGovernor governor = new TestGovernor(mgr, config, new LocalEnvironmentProvider(), brokers);
    PacketSwitchingHandler handler = new PacketSwitchingHandler(mgr, governor, null, new MetricRegistry());

    EmbeddedChannel channel = new EmbeddedChannel(new PacketSwitchingAdapter(handler));
    RequestPacket request = new RequestPacket((short) 0, 1L, RequestType.TOPIC_METADATA,
        new TopicMetadataRequestPacket("unknown"));
    channel.writeInbound(request);

    ResponsePacket response = channel.readOutbound();
    assertNotNull(response);
    TopicMetadataResponsePacket payload = (TopicMetadataResponsePacket) response.getPacket();
    TopicMetadata metadata = payload.getMetadata();
    assertEquals("unknown", metadata.getTopicName());
    assertEquals(1, metadata.getWriteBrokers().size());
    assertEquals(1, metadata.getReadBrokers().size());
  }

  @Test
  public void testUnknownTopicWriteAssignmentsDisabled() throws Exception {
    MemqConfig config = new MemqConfig();
    ClusteringConfig clusteringConfig = new ClusteringConfig();
    clusteringConfig.setEnableAssignments(false);
    config.setClusteringConfig(clusteringConfig);

    MemqManager mgr = new MemqManager(null, config, new HashMap<>());
    mgr.init();

    MemqGovernor governor = new TestGovernor(mgr, config, new LocalEnvironmentProvider(),
        Collections.emptySet());
    PacketSwitchingHandler handler = new PacketSwitchingHandler(mgr, governor, null, new MetricRegistry());

    WriteRequestPacket writePacket = new WriteRequestPacket();
    writePacket.setTopicName("unknown");
    writePacket.setData(new byte[1]);
    RequestPacket request = new RequestPacket((short) 0, 1L, RequestType.WRITE, writePacket);
    try {
      handler.handle(null, request, null, null);
      fail("Expected TOPIC_NOT_FOUND for unknown topic write when assignments are disabled");
    } catch (Exception e) {
      assertEquals(PacketSwitchingHandler.TOPIC_NOT_FOUND, e);
    }
  }

  @Test
  public void testMetadataUsesZkTopicConfigWhenAssignmentsDisabled() throws Exception {
    MemqConfig config = new MemqConfig();
    ClusteringConfig clusteringConfig = new ClusteringConfig();
    clusteringConfig.setEnableAssignments(false);
    config.setClusteringConfig(clusteringConfig);

    MemqManager mgr = new MemqManager(null, config, new HashMap<>());
    mgr.init();

    TopicConfig topicConfig = new TopicConfig("loggen_loadtest", "delayeddevnull");
    Set<Broker> brokers = new HashSet<>(Arrays.asList(
        new Broker("1.1.1.1", (short) 9092, "test", "local", BrokerType.READ_WRITE, new HashSet<>())
    ));

    MemqGovernor governor = new TestGovernorWithTopicConfig(mgr, config,
        new LocalEnvironmentProvider(), brokers, topicConfig);
    PacketSwitchingHandler handler = new PacketSwitchingHandler(mgr, governor, null, new MetricRegistry());

    EmbeddedChannel channel = new EmbeddedChannel(new PacketSwitchingAdapter(handler));
    RequestPacket request = new RequestPacket((short) 0, 1L, RequestType.TOPIC_METADATA,
        new TopicMetadataRequestPacket("loggen_loadtest"));
    channel.writeInbound(request);

    ResponsePacket response = channel.readOutbound();
    assertNotNull(response);
    TopicMetadataResponsePacket payload = (TopicMetadataResponsePacket) response.getPacket();
    TopicMetadata metadata = payload.getMetadata();
    assertEquals("loggen_loadtest", metadata.getTopicName());
    assertEquals("delayeddevnull", metadata.getStorageHandlerName());
    assertEquals(1, metadata.getWriteBrokers().size());
  }
}

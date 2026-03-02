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
package com.pinterest.memq.core.gossip;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.junit.Test;

import com.pinterest.memq.commons.protocol.Broker;
import com.pinterest.memq.commons.protocol.Broker.BrokerType;
import com.pinterest.memq.core.clustering.MemqGovernor;
import com.pinterest.memq.core.config.GossipConfig;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

public class TestGossipServer {

  @Test
  public void testEncodeDecodeRoundTrip() {
    long now = System.currentTimeMillis();
    GossipMessage original = new GossipMessage("192.168.1.10", 42, true, now);

    ByteBuf buf = Unpooled.buffer();
    original.encode(buf);

    GossipMessage decoded = GossipMessage.decode(buf);
    assertEquals("192.168.1.10", decoded.getBrokerId());
    assertEquals(42, decoded.getFreeSlots());
    assertTrue(decoded.isFreeze());
    assertEquals(now, decoded.getSendTimestampMs());

    buf.release();
  }

  @Test
  public void testEncodeDecodeWithPlaceholderValues() {
    long now = System.currentTimeMillis();
    GossipMessage original = new GossipMessage("10.0.0.1", 0, false, now);

    ByteBuf buf = Unpooled.buffer();
    original.encode(buf);

    GossipMessage decoded = GossipMessage.decode(buf);
    assertEquals("10.0.0.1", decoded.getBrokerId());
    assertEquals(0, decoded.getFreeSlots());
    assertFalse(decoded.isFreeze());
    assertEquals(now, decoded.getSendTimestampMs());

    buf.release();
  }

  @Test
  public void testGossipState() {
    long sendTs = System.currentTimeMillis() - 100;
    long recvTs = System.currentTimeMillis();
    GossipMessage msg = new GossipMessage("broker-1", 5, false, sendTs);
    GossipState state = new GossipState(msg, recvTs);

    assertEquals(msg, state.getMessage());
    assertEquals(recvTs, state.getReceiveTimestampMs());
    assertEquals(sendTs, state.getMessage().getSendTimestampMs());
  }

  @Test
  public void testSendAndReceiveOnLocalhost() throws Exception {
    int gossipPort = 19094;

    GossipConfig config = new GossipConfig();
    config.setEnabled(true);
    config.setPort(gossipPort);
    config.setIntervalMs(100);

    // Receiver server listens on gossipPort but does not broadcast (no peers).
    GossipServer receiver = new GossipServer("receiver", "us-east-1a", config,
        new StubGovernor(Collections.emptySet()));

    // Sender server binds on a different port, broadcasts to the receiver.
    GossipConfig senderConfig = new GossipConfig();
    senderConfig.setEnabled(true);
    senderConfig.setPort(gossipPort + 1);
    senderConfig.setIntervalMs(100);

    Broker receiverBroker = new Broker("127.0.0.1", (short) 9090, "test", "us-east-1a",
        BrokerType.WRITE, Collections.emptySet());
    // The sender discovers the receiver as a peer.
    // In production all brokers share the same gossip port;
    // here we override broadcastGossip target port via a subclass.
    GossipServer sender = new GossipServer("sender", "us-east-1a", senderConfig,
        new StubGovernor(Collections.singleton(receiverBroker))) {
      @Override
      protected int getTargetPort() {
        return gossipPort;
      }
    };

    try {
      receiver.start();
      sender.start();

      Thread.sleep(500);

      Map<String, GossipState> states = receiver.getPeerStates();
      assertTrue("Receiver should have gotten gossip from sender",
          states.containsKey("sender"));

      GossipState fromSender = states.get("sender");
      assertEquals("sender", fromSender.getMessage().getBrokerId());
      assertEquals(0, fromSender.getMessage().getFreeSlots());
      assertFalse(fromSender.getMessage().isFreeze());
      assertTrue(fromSender.getMessage().getSendTimestampMs() > 0);
      assertTrue(fromSender.getReceiveTimestampMs() >= fromSender.getMessage().getSendTimestampMs());

    } finally {
      sender.stop();
      receiver.stop();
    }
  }

  @Test
  public void testPeerStatesPopulatedAfterReceive() throws Exception {
    GossipConfig config = new GossipConfig();
    config.setEnabled(true);
    config.setPort(19096);
    config.setIntervalMs(50);

    GossipServer server = new GossipServer("receiver", "us-east-1a", config,
        new StubGovernor(Collections.emptySet()));

    try {
      server.start();

      // Manually send a datagram to the server
      io.netty.channel.nio.NioEventLoopGroup senderGroup = new io.netty.channel.nio.NioEventLoopGroup(1);
      io.netty.bootstrap.Bootstrap senderBootstrap = new io.netty.bootstrap.Bootstrap();
      senderBootstrap.group(senderGroup)
          .channel(io.netty.channel.socket.nio.NioDatagramChannel.class)
          .handler(new io.netty.channel.ChannelInitializer<io.netty.channel.socket.nio.NioDatagramChannel>() {
            @Override
            protected void initChannel(io.netty.channel.socket.nio.NioDatagramChannel ch) {
            }
          });
      io.netty.channel.Channel senderChannel = senderBootstrap.bind(0).sync().channel();

      long sendTs = System.currentTimeMillis();
      GossipMessage msg = new GossipMessage("external-broker", 10, true, sendTs);
      ByteBuf buf = Unpooled.buffer();
      msg.encode(buf);
      senderChannel.writeAndFlush(
          new io.netty.channel.socket.DatagramPacket(buf,
              new java.net.InetSocketAddress("127.0.0.1", 19096))).sync();

      Thread.sleep(200);

      Map<String, GossipState> states = server.getPeerStates();
      assertNotNull(states.get("external-broker"));
      GossipState state = states.get("external-broker");
      assertEquals("external-broker", state.getMessage().getBrokerId());
      assertEquals(10, state.getMessage().getFreeSlots());
      assertTrue(state.getMessage().isFreeze());
      assertEquals(sendTs, state.getMessage().getSendTimestampMs());
      assertTrue(state.getReceiveTimestampMs() >= sendTs);

      senderChannel.close();
      senderGroup.shutdownGracefully();
    } finally {
      server.stop();
    }
  }

  @Test
  public void testGetAllBrokersInRackFiltersOutOtherRacks() {
    Set<Broker> allBrokers = new HashSet<>();
    allBrokers.add(new Broker("10.0.0.1", (short) 9090, "test", "us-east-1a",
        BrokerType.WRITE, Collections.emptySet()));
    allBrokers.add(new Broker("10.0.0.2", (short) 9090, "test", "us-east-1a",
        BrokerType.WRITE, Collections.emptySet()));
    allBrokers.add(new Broker("10.0.1.1", (short) 9090, "test", "us-east-1b",
        BrokerType.WRITE, Collections.emptySet()));
    allBrokers.add(new Broker("10.0.2.1", (short) 9090, "test", "us-west-2a",
        BrokerType.WRITE, Collections.emptySet()));

    RackFilteringStubGovernor governor = new RackFilteringStubGovernor(allBrokers);

    // Requesting us-east-1a should return only the two 1a brokers.
    Set<Broker> rack1a = governor.getAllBrokersInRack("us-east-1a");
    assertEquals(2, rack1a.size());
    Set<String> ips1a = rack1a.stream().map(Broker::getBrokerIP).collect(Collectors.toSet());
    assertTrue(ips1a.contains("10.0.0.1"));
    assertTrue(ips1a.contains("10.0.0.2"));

    // Requesting us-east-1b should return only the one 1b broker.
    Set<Broker> rack1b = governor.getAllBrokersInRack("us-east-1b");
    assertEquals(1, rack1b.size());
    assertEquals("10.0.1.1", rack1b.iterator().next().getBrokerIP());

    // Requesting a rack with no brokers should return empty.
    Set<Broker> rackNone = governor.getAllBrokersInRack("eu-west-1a");
    assertTrue(rackNone.isEmpty());

    // Null rack returns all brokers.
    Set<Broker> all = governor.getAllBrokersInRack(null);
    assertEquals(4, all.size());
  }

  /**
   * Stub governor that returns a fixed set of brokers (ignores rack filtering).
   */
  private static class StubGovernor extends MemqGovernor {

    private final Set<Broker> brokers;

    StubGovernor(Set<Broker> brokers) {
      super(null, stubConfig(), stubProvider());
      this.brokers = brokers;
    }

    @Override
    public Set<Broker> getAllBrokersInRack(String rack) {
      return brokers;
    }

    private static com.pinterest.memq.core.config.MemqConfig stubConfig() {
      com.pinterest.memq.core.config.MemqConfig config = new com.pinterest.memq.core.config.MemqConfig();
      return config;
    }

    private static com.pinterest.memq.core.config.EnvironmentProvider stubProvider() {
      return new com.pinterest.memq.core.config.EnvironmentProvider() {
        public String getIP() {
          return "127.0.0.1";
        }

        public String getRack() {
          return "us-east-1a";
        }

        public String getInstanceType() {
          return "test";
        }
      };
    }
  }

  /**
   * Stub governor that filters brokers by rack, matching the real
   * MemqGovernor.getAllBrokersInRack behavior.
   */
  private static class RackFilteringStubGovernor extends MemqGovernor {

    private final Set<Broker> allBrokers;

    RackFilteringStubGovernor(Set<Broker> allBrokers) {
      super(null, stubConfig(), stubProvider());
      this.allBrokers = allBrokers;
    }

    @Override
    public Set<Broker> getAllBrokersInRack(String rack) {
      if (rack == null) {
        return allBrokers;
      }
      return allBrokers.stream()
          .filter(b -> rack.equals(b.getLocality()))
          .collect(Collectors.toSet());
    }

    private static com.pinterest.memq.core.config.MemqConfig stubConfig() {
      return new com.pinterest.memq.core.config.MemqConfig();
    }

    private static com.pinterest.memq.core.config.EnvironmentProvider stubProvider() {
      return new com.pinterest.memq.core.config.EnvironmentProvider() {
        public String getIP() {
          return "127.0.0.1";
        }

        public String getRack() {
          return "us-east-1a";
        }

        public String getInstanceType() {
          return "test";
        }
      };
    }
  }
}

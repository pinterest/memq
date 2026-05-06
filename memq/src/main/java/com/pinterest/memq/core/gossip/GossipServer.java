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

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import com.pinterest.memq.commons.protocol.Broker;
import com.pinterest.memq.core.clustering.MemqGovernor;
import com.pinterest.memq.core.config.GossipConfig;
import com.pinterest.memq.core.slot.SlotManager;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.DatagramPacket;
import io.netty.channel.socket.nio.NioDatagramChannel;

import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

public class GossipServer {

  private static final Logger logger = Logger.getLogger(GossipServer.class.getName());

  private final String brokerId;
  private final String rack;
  private final GossipConfig config;
  private final MemqGovernor governor;
  private final MetricRegistry registry;
  private final Counter sentCounter;
  private final Counter expiredCounter;
  private final SlotManager slotManager;
  private final ConcurrentHashMap<String, GossipState> peerStates = new ConcurrentHashMap<>();

  private NioEventLoopGroup group;
  private Channel channel;
  private ScheduledExecutorService senderExecutor;

  public GossipServer(String brokerId, String rack, GossipConfig config, MemqGovernor governor) {
    this(brokerId, rack, config, governor, new MetricRegistry(), null);
  }

  public GossipServer(String brokerId, String rack, GossipConfig config, MemqGovernor governor,
                      MetricRegistry registry) {
    this(brokerId, rack, config, governor, registry, null);
  }

  public GossipServer(String brokerId, String rack, GossipConfig config, MemqGovernor governor,
                      MetricRegistry registry, SlotManager slotManager) {
    this.brokerId = brokerId;
    this.rack = rack;
    this.config = config;
    this.governor = governor;
    this.registry = registry;
    this.sentCounter = registry.counter("message.sent");
    this.expiredCounter = registry.counter("message.expired");
    this.slotManager = slotManager;
  }

  public void start() throws InterruptedException {
    group = new NioEventLoopGroup(1, r -> {
      Thread t = new Thread(r, "gossip-io");
      t.setDaemon(true);
      return t;
    });

    Bootstrap bootstrap = new Bootstrap();
    bootstrap.group(group)
        .channel(NioDatagramChannel.class)
        .handler(new ChannelInitializer<NioDatagramChannel>() {
          @Override
          protected void initChannel(NioDatagramChannel ch) {
            ch.pipeline().addLast(new GossipMessageDecoder(peerStates, registry));
          }
        });

    channel = bootstrap.bind(config.getPort()).sync().channel();
    logger.info("Gossip UDP listener started on port " + config.getPort());

    senderExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
      Thread t = new Thread(r, "gossip-sender");
      t.setDaemon(true);
      return t;
    });
    senderExecutor.scheduleAtFixedRate(this::broadcastGossip, config.getIntervalMs(),
        config.getIntervalMs(), TimeUnit.MILLISECONDS);
  }

  private void broadcastGossip() {
    try {
      int freeSlots = slotManager != null ? slotManager.getFreeSlots() : 0;
      boolean freeze = slotManager != null && slotManager.isFrozen();
      GossipMessage msg = new GossipMessage(brokerId, freeSlots, freeze, System.currentTimeMillis());
      Set<Broker> brokers = governor.getAllBrokersInRack(rack);
      for (Broker broker : brokers) {
        if (brokerId.equals(broker.getBrokerIP())) {
          continue;
        }
        InetSocketAddress target = new InetSocketAddress(broker.getBrokerIP(), getTargetPort());
        ByteBuf buf = channel.alloc().buffer();
        msg.encode(buf);
        channel.writeAndFlush(new DatagramPacket(buf, target));
        sentCounter.inc();
      }
    } catch (Exception e) {
      logger.log(Level.WARNING, "Failed to broadcast gossip", e);
    }
  }

  public void stop() {
    if (senderExecutor != null) {
      senderExecutor.shutdownNow();
    }
    if (channel != null) {
      channel.close();
    }
    if (group != null) {
      group.shutdownGracefully();
    }
    logger.info("Gossip server stopped");
  }

  protected int getTargetPort() {
    return config.getPort();
  }

  /**
   * Snapshot of currently-fresh peer states. Entries whose last receive
   * time is older than {@code config.getPeerTtlMs()} are removed from the
   * backing map (CAS-style via {@code ConcurrentHashMap.remove(k, v)} so
   * a concurrent write from the decoder is not clobbered) and omitted
   * from the returned snapshot.
   * <p>
   * Called by {@code EvictionManager} on its tick, not on any hot path.
   */
  public Map<String, GossipState> getPeerStates() {
    long now = System.currentTimeMillis();
    long ttlMs = config.getPeerTtlMs();
    Map<String, GossipState> snapshot = new HashMap<>(peerStates.size());
    Iterator<Map.Entry<String, GossipState>> it = peerStates.entrySet().iterator();
    while (it.hasNext()) {
      Map.Entry<String, GossipState> e = it.next();
      GossipState state = e.getValue();
      if (now - state.getReceiveTimestampMs() > ttlMs) {
        if (peerStates.remove(e.getKey(), state)) {
          expiredCounter.inc();
        }
      } else {
        snapshot.put(e.getKey(), state);
      }
    }
    return Collections.unmodifiableMap(snapshot);
  }

  ConcurrentHashMap<String, GossipState> getPeerStatesInternal() {
    return peerStates;
  }
}

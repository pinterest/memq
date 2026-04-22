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
import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.socket.DatagramPacket;

import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

public class GossipMessageDecoder extends SimpleChannelInboundHandler<DatagramPacket> {

  private static final Logger logger = Logger.getLogger(GossipMessageDecoder.class.getName());

  private final ConcurrentHashMap<String, GossipState> peerStates;
  private final Counter receivedCounter;
  private final Histogram latencyHistogram;

  public GossipMessageDecoder(ConcurrentHashMap<String, GossipState> peerStates,
                              MetricRegistry registry) {
    this.peerStates = peerStates;
    this.receivedCounter = registry.counter("message.received");
    this.latencyHistogram = registry.histogram("message.latency");
  }

  @Override
  protected void channelRead0(ChannelHandlerContext ctx, DatagramPacket packet) {
    try {
      GossipMessage msg = GossipMessage.decode(packet.content());
      long now = System.currentTimeMillis();
      GossipState state = new GossipState(msg, now);
      peerStates.put(msg.getBrokerId(), state);
      receivedCounter.inc();
      latencyHistogram.update(now - msg.getSendTimestampMs());
    } catch (Exception e) {
      logger.log(Level.WARNING, "Failed to decode gossip datagram", e);
    }
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
    logger.log(Level.WARNING, "Exception in gossip channel", cause);
  }
}

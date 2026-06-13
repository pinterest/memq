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

import java.security.Principal;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

import javax.ws.rs.InternalServerErrorException;
import javax.ws.rs.NotAuthorizedException;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.RedirectionException;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import com.pinterest.memq.commons.protocol.ReadRequestPacket;
import com.pinterest.memq.commons.protocol.RequestPacket;
import com.pinterest.memq.commons.protocol.ResponseCodes;
import com.pinterest.memq.commons.protocol.ResponsePacket;
import com.pinterest.memq.commons.protocol.TopicMetadata;
import com.pinterest.memq.commons.protocol.TopicMetadataRequestPacket;
import com.pinterest.memq.commons.protocol.TopicMetadataResponsePacket;
import com.pinterest.memq.commons.protocol.WriteRequestPacket;
import com.pinterest.memq.commons.protocol.WriteResponsePacket;
import com.pinterest.memq.commons.protocol.Broker.BrokerType;
import com.pinterest.memq.core.MemqManager;
import com.pinterest.memq.core.clustering.MemqGovernor;
import com.pinterest.memq.core.processing.TopicProcessor;
import com.pinterest.memq.core.security.Authorizer;
import com.pinterest.memq.core.slot.SlotManager;

import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import io.netty.channel.ChannelHandlerContext;
import io.netty.util.Attribute;
import io.netty.util.AttributeKey;

public class PacketSwitchingHandler {

  protected static final NotFoundException TOPIC_NOT_FOUND = new NotFoundException(
      "Topic not found");
  protected static final InternalServerErrorException SERVER_NOT_INITIALIZED = new InternalServerErrorException(
      "Server not initialized correctly");
  protected static final InternalServerErrorException INVALID_PACKET_TYPE = new InternalServerErrorException(
      "Invalid packet type");
  private static final Logger logger = Logger
      .getLogger(PacketSwitchingHandler.class.getCanonicalName());

  /**
   * Per-channel set of v4+ producer ids ({@code pid}) seen on the connection.
   * Populated on the write path and drained by {@link #releaseChannelProducers}
   * from the decoder's {@code channelInactive} so the {@link SlotManager}
   * accounting for those producers is reclaimed immediately on disconnect
   * rather than after {@code idleProducerTimeoutMs}. Only v4+ ids (unique
   * client UUIDs) are tracked; the v3 {@code pid == remoteIp} fallback can be
   * shared by multiple connections from one host, so those rely on the
   * idle-timeout backstop to avoid evicting a still-live sibling connection.
   */
  public static final AttributeKey<Set<String>> PRODUCER_IDS_ATTR =
      AttributeKey.valueOf("memq.producerIds");
  protected MemqManager mgr;
  protected Counter writeRquestCounter;
  protected Counter readRequestCounter;
  protected Counter invalidPacketCounter;
  protected Authorizer authorizer;
  protected MemqGovernor governor;

  public PacketSwitchingHandler(MemqManager mgr,
                                MemqGovernor governor,
                                Authorizer authorizer,
                                MetricRegistry registry) {
    this.mgr = mgr;
    this.governor = governor;
    this.authorizer = authorizer;
    initializeMetrics(registry);
  }

  private void initializeMetrics(MetricRegistry registry) {
    writeRquestCounter = registry.counter("request.write");
    readRequestCounter = registry.counter("request.read");
    invalidPacketCounter = registry.counter("request.invalidcode");
  }

  public void handle(ChannelHandlerContext ctx,
                     RequestPacket requestPacket,
                     Principal principal,
                     String clientAddress) throws Exception {
    authorize(requestPacket, principal, clientAddress);
    switch (requestPacket.getRequestType()) {
    case WRITE:
      WriteRequestPacket writePacket = (WriteRequestPacket) requestPacket.getPayload();
      executeWriteRequest(ctx, requestPacket, writePacket);
      break;
    case TOPIC_METADATA:
      TopicMetadataRequestPacket mdRequest = (TopicMetadataRequestPacket) requestPacket
          .getPayload();
      List<String> requestedTopics = mdRequest.getTopics();
      List<TopicMetadata> results;
      if (requestedTopics.isEmpty()) {
        results = new ArrayList<>(governor.getTopicMetadataMap().values());
      } else {
        results = new ArrayList<>(requestedTopics.size());
        for (String t : requestedTopics) {
          TopicMetadata md = governor.getTopicMetadataMap().get(t);
          if (md == null) {
            throw new NotFoundException("Topic not found:" + t);
          }
          results.add(md);
        }
      }
      ctx.writeAndFlush(new ResponsePacket(requestPacket.getProtocolVersion(),
          requestPacket.getClientRequestId(), requestPacket.getRequestType(), ResponseCodes.OK,
          new TopicMetadataResponsePacket(results)));
      break;
    case READ:
      ReadRequestPacket readPacket = (ReadRequestPacket) requestPacket.getPayload();
      executeReadRequest(ctx, requestPacket, readPacket);
      break;
    default:
      throw new Exception("Unsupported request type");
    }
  }

  private void authorize(RequestPacket requestPacket, Principal principal, String clientAddress) throws Exception {
    String resource = null;
    switch (requestPacket.getRequestType()) {
    case WRITE:
      if (mgr.getConfiguration().getBrokerType() == BrokerType.READ) {
        throw new Exception("Read only broker cannot accept write requests");
      }
      resource = ((WriteRequestPacket) requestPacket.getPayload()).getTopicName();
      break;
    case READ:
      if (mgr.getConfiguration().getBrokerType() == BrokerType.WRITE) {
        throw new Exception("Write only broker cannot accept read requests");
      }
      resource = ((ReadRequestPacket) requestPacket.getPayload()).getTopicName();
    default:
      break;
    }
    if (authorizer != null) {
      if (resource == null) {
        return;
      }
      boolean authorized = authorizer.authorize(principal, clientAddress, resource,
          requestPacket.getRequestType());
      if (!authorized) {
        throw new NotAuthorizedException(requestPacket.getRequestType().name()
            + " access to resource:" + resource + " not allowed for principal:" + principal);
      }
    }
  }

  protected void executeWriteRequest(ChannelHandlerContext ctx,
                                     RequestPacket requestPacket,
                                     WriteRequestPacket writePacket) {
    if (mgr == null) {
      throw SERVER_NOT_INITIALIZED;
    }
    TopicProcessor topicProcessor = mgr.getProcessorMap().get(writePacket.getTopicName());
    if (topicProcessor != null) {
      writeRquestCounter.inc();
      // Resolve the canonical producer ID. v4 clients send a client-generated
      // UUID so multiple producers on the same host are distinguished. v3
      // clients don't send one, so we fall back to the remote IP.
      InetSocketAddress remote = (InetSocketAddress) ctx.channel().remoteAddress();
      String remoteIp = remote.getAddress().getHostAddress();
      // The presence of a v4+ producerId is itself the signal that this is a
      // v4+ producer (a unique client UUID). v3 clients send none, so we fall
      // back to the remote IP.
      boolean hasExplicitProducerId = requestPacket.getProtocolVersion() >= 4
          && writePacket.getProducerId() != null
          && !writePacket.getProducerId().isEmpty();
      String producerId = hasExplicitProducerId ? writePacket.getProducerId() : remoteIp;
      SlotManager sm = mgr.getSlotManager();
      if (sm != null) {
        sm.recordWrite(producerId, writePacket.getTopicName(), writePacket.getDataLength());
        // Map the opaque v4 UUID back to its source host so slot/eviction logs
        // can be traced to an IP.
        sm.recordProducerIp(producerId, remoteIp);
        if (hasExplicitProducerId) {
          // Always register so EvictionManager can target it, even if the slot
          // map is empty (bootstrap: producer hasn't received any slot
          // ownership info from any broker yet).
          //
          // v5 producers report per-target slot counts; v4 producers carry
          // only the connection set and the packet codec already populated
          // the map with equal-weight 1s on decode. The eviction strategy
          // treats equal-weight maps as ranking targets purely by free
          // slots, identical to legacy behavior.
          Map<String, Integer> connectionSlots = writePacket.getCurrentConnectionSlots();
          sm.recordProducerConnections(producerId,
              connectionSlots == null ? Collections.<String, Integer>emptyMap() : connectionSlots);
          // Remember the id on the channel so a disconnect reclaims its slot
          // accounting immediately instead of waiting out the idle timeout.
          trackProducerId(ctx, producerId);
        }
      }
      topicProcessor.registerChannel(ctx.channel());
      topicProcessor.write(requestPacket, writePacket, ctx, producerId);
    } else if (governor.getTopicMetadataMap().containsKey(writePacket.getTopicName())) {
      throw new RedirectionException(301, null);
    } else {
      logger.severe("Topic not found:" + writePacket.getTopicName());
      throw TOPIC_NOT_FOUND;
    }
  }

  protected void executeReadRequest(ChannelHandlerContext ctx,
                                    RequestPacket requestPacket,
                                    ReadRequestPacket readPacket) {
    if (mgr == null) {
      throw SERVER_NOT_INITIALIZED;
    }
    TopicProcessor topicProcessor = mgr.getProcessorMap().get(readPacket.getTopicName());
    if (topicProcessor != null) {
      readRequestCounter.inc();
      topicProcessor.read(requestPacket, readPacket, ctx);
    }
  }

  /**
   * Record a v4+ producer id against the channel. The channel's inbound events
   * (writes here and {@code channelInactive}) all run on the same event loop
   * thread, so the per-channel {@link Set} needs no synchronization.
   */
  private static void trackProducerId(ChannelHandlerContext ctx, String producerId) {
    Attribute<Set<String>> attr = ctx.channel().attr(PRODUCER_IDS_ATTR);
    Set<String> ids = attr.get();
    if (ids == null) {
      ids = new HashSet<>(2);
      attr.set(ids);
    }
    ids.add(producerId);
  }

  /**
   * Release {@link SlotManager} accounting for every v4+ producer id seen on
   * this channel. Invoked from the decoder's {@code channelInactive} so a
   * producer's slot, connection, and (when enabled) metric state is reclaimed
   * the moment its connection closes, rather than after
   * {@code idleProducerTimeoutMs}. This is what prevents abandoned UUIDs from a
   * producer-fleet restart from piling up for the full idle window.
   */
  public void releaseChannelProducers(ChannelHandlerContext ctx) {
    Set<String> producerIds = ctx.channel().attr(PRODUCER_IDS_ATTR).getAndSet(null);
    if (producerIds == null || producerIds.isEmpty() || mgr == null) {
      return;
    }
    SlotManager sm = mgr.getSlotManager();
    if (sm == null) {
      return;
    }
    for (String pid : producerIds) {
      sm.removeProducer(pid);
    }
  }
}

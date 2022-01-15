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

import io.netty.channel.ChannelHandlerContext;

public class PacketSwitchingHandler {

  protected static final NotFoundException TOPIC_NOT_FOUND = new NotFoundException(
      "Topic not found");
  protected static final InternalServerErrorException SERVER_NOT_INITIALIZED = new InternalServerErrorException(
      "Server not initialized correctly");
  protected static final InternalServerErrorException INVALID_PACKET_TYPE = new InternalServerErrorException(
      "Invalid packet type");
  private static final Logger logger = Logger
      .getLogger(PacketSwitchingHandler.class.getCanonicalName());
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
      TopicMetadata md = governor.getTopicMetadataMap().get(mdRequest.getTopic());
      if (md == null) {
        throw TOPIC_NOT_FOUND;
      } else {
        ResponsePacket msg = new ResponsePacket(requestPacket.getProtocolVersion(),
            requestPacket.getClientRequestId(), requestPacket.getRequestType(), ResponseCodes.OK,
            new TopicMetadataResponsePacket(md));
        ctx.writeAndFlush(msg);
      }
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
      topicProcessor.registerChannel(ctx.channel());
      topicProcessor.write(requestPacket, writePacket, ctx);
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
}

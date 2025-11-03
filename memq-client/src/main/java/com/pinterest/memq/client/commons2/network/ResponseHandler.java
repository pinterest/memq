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
package com.pinterest.memq.client.commons2.network;

import com.pinterest.memq.client.commons2.TransportPacketIdentifier;
import com.pinterest.memq.commons.protocol.ResponsePacket;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

import io.netty.channel.Channel;
import io.netty.channel.ChannelId;

public class ResponseHandler implements Closeable {

  private static final Logger logger = LoggerFactory.getLogger(ResponseHandler.class);
  private final AtomicReference<Map<TransportPacketIdentifier, CompletableFuture<ResponsePacket>>>
      inflightRequestMapRef = new AtomicReference<>(new ConcurrentHashMap<>());

  // Track which requests belong to which channel so we can reject only that channel's inflight on close
  private final Map<ChannelId, Set<TransportPacketIdentifier>> channelToRequests = new ConcurrentHashMap<>();
  private final Map<TransportPacketIdentifier, ChannelId> requestToChannel = new ConcurrentHashMap<>();

  public void handle(ResponsePacket responsePacket) throws Exception {
    CompletableFuture<ResponsePacket> future = removeRequest(new TransportPacketIdentifier(responsePacket));
    if (future != null) {
      future.complete(responsePacket);
    } else {
      // no handler for response skipping
      logger.error("No handler for request:" + responsePacket.getRequestType());
    }
  }

  public void setInflightRequestMap(
      Map<TransportPacketIdentifier, CompletableFuture<ResponsePacket>> inflightRequestMap) {
    inflightRequestMapRef.set(inflightRequestMap);
  }

  public void addRequest(TransportPacketIdentifier identifier,
                         CompletableFuture<ResponsePacket> future) {
    inflightRequestMapRef.get().put(identifier, future);
  }

  public void registerRequest(Channel channel,
                              TransportPacketIdentifier identifier,
                              CompletableFuture<ResponsePacket> future) {
    inflightRequestMapRef.get().put(identifier, future);
    ChannelId channelId = channel.id();
    channelToRequests.computeIfAbsent(channelId, k -> ConcurrentHashMap.newKeySet()).add(identifier);
    requestToChannel.put(identifier, channelId);
  }

  public CompletableFuture<ResponsePacket> removeRequest(TransportPacketIdentifier identifier) {
    CompletableFuture<ResponsePacket> future = inflightRequestMapRef.get().remove(identifier);
    ChannelId channelId = requestToChannel.remove(identifier);
    if (channelId != null) {
      Set<TransportPacketIdentifier> set = channelToRequests.get(channelId);
      if (set != null) {
        set.remove(identifier);
        if (set.isEmpty()) {
          channelToRequests.remove(channelId);
        }
      }
    }
    return future;
  }

  public boolean cancelRequest(TransportPacketIdentifier identifier, Throwable reason) {
    CompletableFuture<ResponsePacket> future = removeRequest(identifier);
    if (future == null) {
      return false;
    }
    rejectRequestFuture(future, reason);
    return true;
  }

  private void rejectRequestFuture(CompletableFuture<ResponsePacket> future, Throwable reason) {
    future.completeExceptionally(reason);
  }

  public int getInflightRequestCount() {
    return inflightRequestMapRef.get().size();
  }

  public void cleanAndRejectInflightRequests(Throwable reason) {
    logger.debug("Replacing and cleaning inflight requests due to " + reason);
    Map<TransportPacketIdentifier, CompletableFuture<ResponsePacket>> oldMap = inflightRequestMapRef.getAndSet(new ConcurrentHashMap<>());

    // reject futures with the provided reason and remove them from the map
    for (Map.Entry<TransportPacketIdentifier, CompletableFuture<ResponsePacket>> entry : oldMap
        .entrySet()) {
      rejectRequestFuture(entry.getValue(), reason);
    }
    channelToRequests.clear();
    requestToChannel.clear();
  }

  public void cleanAndRejectInflightRequestsForChannel(Channel channel, Throwable reason) {
    ChannelId channelId = channel.id();
    Set<TransportPacketIdentifier> identifiers = channelToRequests.remove(channelId);
    if (identifiers == null || identifiers.isEmpty()) {
      return;
    }
    for (TransportPacketIdentifier identifier : identifiers) {
      CompletableFuture<ResponsePacket> future = inflightRequestMapRef.get().remove(identifier);
      requestToChannel.remove(identifier);
      if (future != null) {
        rejectRequestFuture(future, reason);
      }
    }
  }

  public void close() {
    cleanAndRejectInflightRequests(new ClientClosedException());
  }

}

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


import io.netty.channel.ChannelId;
import io.netty.util.AttributeKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class ResponseHandler implements Closeable {

  private static final Logger logger = LoggerFactory.getLogger(ResponseHandler.class);
  private final ConcurrentMap<ChannelId, Map<TransportPacketIdentifier, CompletableFuture<ResponsePacket>>> connectionMap = new ConcurrentHashMap<>();
  private final AtomicInteger inflightRequestCount = new AtomicInteger();

  public void handle(ChannelId channelId, ResponsePacket responsePacket) throws Exception {
    CompletableFuture<ResponsePacket> future = removeRequest(channelId, new TransportPacketIdentifier(responsePacket));
    responsePacket.attr(NetworkClient.PACKET_CHANNEL_ID_ATTR_KEY).set(channelId);
    if (future != null) {
      future.complete(responsePacket);
    } else {
      // no handler for response skipping
      logger.error("No handler for request:" + responsePacket.getRequestType());
    }
  }

  public void addRequest(
      ChannelId channelId,
      TransportPacketIdentifier identifier,
      CompletableFuture<ResponsePacket> future
  ) {
    inflightRequestCount.incrementAndGet();
    connectionMap.compute(channelId, (k, v) -> v == null ? new ConcurrentHashMap<>() : v).put(identifier, future);
  }

  public CompletableFuture<ResponsePacket> removeRequest(ChannelId channelId, TransportPacketIdentifier identifier) {
    inflightRequestCount.decrementAndGet();
    return connectionMap.get(channelId).remove(identifier);
  }

  public boolean cancelRequest(ChannelId channelId, TransportPacketIdentifier identifier, Throwable reason) {
    CompletableFuture<ResponsePacket> future = removeRequest(channelId, identifier);
    if (future == null) {
      return false;
    }
    rejectRequestFuture(future, reason);
    return true;
  }

  private void rejectRequestFuture(CompletableFuture<ResponsePacket> future, Throwable reason) {
    future.completeExceptionally(reason);
  }

  public void cleanAndRejectInflightRequests(ChannelId channelId, Throwable reason) {
    logger.debug("Replacing and cleaning inflight requests due to " + reason);
    Map<TransportPacketIdentifier, CompletableFuture<ResponsePacket>> oldMap = connectionMap.remove(channelId);
    if (oldMap == null || oldMap.isEmpty()) {
      return;
    }

    // reject futures with the provided reason and remove them from the map
    for (Map.Entry<TransportPacketIdentifier, CompletableFuture<ResponsePacket>> entry : oldMap
        .entrySet()) {
      rejectRequestFuture(entry.getValue(), reason);
    }
    inflightRequestCount.addAndGet(-oldMap.size());
  }

  public int getInflightRequestCount() {
    return inflightRequestCount.get();
  }

  public void close() {
    for (ChannelId channelId : connectionMap.keySet()) {
      cleanAndRejectInflightRequests(channelId, new ClientClosedException());
    }
  }

}

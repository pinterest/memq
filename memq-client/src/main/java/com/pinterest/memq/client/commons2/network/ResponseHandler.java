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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

public class ResponseHandler implements Closeable {

  private static final Logger logger = LoggerFactory.getLogger(ResponseHandler.class);
  private final AtomicReference<Map<TransportPacketIdentifier, CompletableFuture<ResponsePacket>>>
      inflightRequestMapRef = new AtomicReference<>(new ConcurrentHashMap<>());

  public void handle(ResponsePacket responsePacket) throws Exception {
    CompletableFuture<ResponsePacket> future = inflightRequestMapRef.get()
        .remove(new TransportPacketIdentifier(responsePacket));
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

  public CompletableFuture<ResponsePacket> removeRequest(TransportPacketIdentifier identifier) {
    return inflightRequestMapRef.get().remove(identifier);
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
  }

  public void close() {
    cleanAndRejectInflightRequests(new ClientClosedException());
  }

}

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
package com.pinterest.memq.core.rpc.queue;

import com.pinterest.memq.commons.protocol.RequestPacket;
import com.pinterest.memq.commons.protocol.WriteRequestPacket;

import io.netty.channel.ChannelHandlerContext;

/**
 * Wrapper class that holds all the context needed to process a queued request.
 */
public class QueuedRequest {
  
  private final ChannelHandlerContext ctx;
  private final RequestPacket requestPacket;
  private final WriteRequestPacket writePacket;
  private final String topicName;
  private final int size;
  private final long enqueueTimeNanos;

  public QueuedRequest(ChannelHandlerContext ctx,
                       RequestPacket requestPacket,
                       WriteRequestPacket writePacket) {
    this.ctx = ctx;
    this.requestPacket = requestPacket;
    this.writePacket = writePacket;
    this.topicName = writePacket.getTopicName();
    this.size = writePacket.getDataLength();
    this.enqueueTimeNanos = System.nanoTime();
  }

  public ChannelHandlerContext getCtx() {
    return ctx;
  }

  public RequestPacket getRequestPacket() {
    return requestPacket;
  }

  public WriteRequestPacket getWritePacket() {
    return writePacket;
  }

  public String getTopicName() {
    return topicName;
  }

  /**
   * Returns the size of the request in bytes.
   * Used by strategies like DRR that need to track byte counts.
   */
  public int getSize() {
    return size;
  }

  public long getEnqueueTimeNanos() {
    return enqueueTimeNanos;
  }
}

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
package com.pinterest.memq.core.commons;

import com.google.common.annotations.VisibleForTesting;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.ChannelHandlerContext;
import io.netty.util.Recycler;

public class Message {

  private ByteBuf buf;
  private volatile long clientRequestId = -1L;
  private volatile long serverRequestId = -1L;
  private volatile ChannelHandlerContext pipelineReference;
  private volatile short clientProtocolVersion;
  private final Recycler.Handle<Message> handle;

  private static final Recycler<Message> RECYCLER = new Recycler<Message>() {
    @Override
    protected Message newObject(Handle<Message> handle) {
      return new Message(handle);
    }
  };

  private Message(Recycler.Handle<Message> handle) {
    this.handle = handle;
  }

  public static Message newInstance(ByteBuf buf,
                                    long clientRequestId,
                                    long serverRequestId,
                                    ChannelHandlerContext pipelineReference,
                                    short clientProtocolVersion){
    Message message = RECYCLER.get();
    message.buf = buf;
    message.clientRequestId = clientRequestId;
    message.serverRequestId = serverRequestId;
    message.pipelineReference = pipelineReference;
    message.clientProtocolVersion = clientProtocolVersion;
    return message;
  }

  public void recycle() {
    buf = null;
    clientRequestId = 0;
    serverRequestId = 0;
    pipelineReference = null;
    clientProtocolVersion = 0;
    handle.recycle(this);
  }

//  public Message(ByteBuf buf,
//                 long clientRequestId,
//                 long serverRequestId,
//                 ChannelHandlerContext pipelineReference,
//                 short clientProtocolVersion) {
//    this.buf = buf;
//    this.clientRequestId = clientRequestId;
//    this.serverRequestId = serverRequestId;
//    this.pipelineReference = pipelineReference;
//    this.clientProtocolVersion = clientProtocolVersion;
//  }
//
  @VisibleForTesting
  public Message(int capacity, boolean isDirect) {
    handle = null;
    if (!isDirect) {
      buf = PooledByteBufAllocator.DEFAULT.buffer(capacity, capacity);
    } else {
      buf = PooledByteBufAllocator.DEFAULT.directBuffer(capacity, capacity);
    }
  }

  /**
   * @return the buf
   */
  public ByteBuf getBuf() {
    return buf;
  }

  public int readIndex() {
    return buf.readerIndex();
  }

  public int writeIndex() {
    return buf.writerIndex();
  }

  /**
   * Write the byte array to ByteBuffer and flip it for reading
   * 
   * To be called only once
   * 
   * @param buf
   */
  public void put(byte[] buf) {
    this.buf.writeBytes(buf);
  }

  /**
   * @return the clientRequestId
   */
  public long getClientRequestId() {
    return clientRequestId;
  }

  /**
   * @param clientRequestId the clientRequestId to set
   */
  public void setClientRequestId(long clientRequestId) {
    this.clientRequestId = clientRequestId;
  }

  /**
   * @return the serverRequestId
   */
  public long getServerRequestId() {
    return serverRequestId;
  }

  /**
   * @param serverRequestId the serverRequestId to set
   */
  public void setServerRequestId(long serverRequestId) {
    this.serverRequestId = serverRequestId;
  }

  @Override
  public String toString() {
    return serverRequestId + "_" + clientRequestId;
  }

  public void reset() {
    buf.clear();
    pipelineReference = null;
    clientProtocolVersion = 0;
  }

  public ChannelHandlerContext getPipelineReference() {
    return pipelineReference;
  }

  public void setPipelineReference(ChannelHandlerContext pipelineReference) {
    this.pipelineReference = pipelineReference;
  }

  public short getClientProtocolVersion() {
    return clientProtocolVersion;
  }

  public void setClientProtocolVersion(short clientProtocolVersion) {
    this.clientProtocolVersion = clientProtocolVersion;
  }
}

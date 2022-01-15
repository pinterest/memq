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
package com.pinterest.memq.core.processing.bucketing;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import com.pinterest.memq.commons.protocol.RequestPacket;
import com.pinterest.memq.commons.protocol.WriteRequestPacket;
import com.pinterest.memq.core.commons.Message;
import com.pinterest.memq.core.utils.CrcProcessor;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;

public class Batch {

  private List<Message> messages;
  private volatile int writeIndex;
  private AtomicBoolean available = new AtomicBoolean(true);
  private volatile long originationTimestamp;
  private volatile long dispatchTimestamp;
  private AtomicLong remainingSlots;
  private int size;

  public Batch(int size, AtomicLong remainingSlots) {
    this.size = size;
    this.remainingSlots = remainingSlots;
    messages = new ArrayList<>();
  }

  public boolean hasSpace() {
    return writeIndex < size;
  }

  public boolean hasData() {
    return writeIndex > 0;
  }

  public void write(long serverRequestId,
                    RequestPacket packetHeader,
                    WriteRequestPacket writePacket,
                    ChannelHandlerContext ctx) throws Exception {
    prepareWrite(serverRequestId, packetHeader.getClientRequestId());
    ByteBuf buf = writePacket.getData().retainedSlice();
    if (writePacket.isChecksumExists()) {
      ByteBuf checksumBuffer = buf.duplicate();
      validateChecksumAndRejectMessage(checksumBuffer, writePacket.getChecksum());
    }
    Message message = Message.newInstance(buf, packetHeader.getClientRequestId(), serverRequestId, ctx,
        packetHeader.getProtocolVersion());
    if (ctx != null) {
      message.setPipelineReference(ctx);
    }
    messages.add(message);
  }

  private void validateChecksumAndRejectMessage(ByteBuf checksumBuffer,
                                                int payloadChecksum) throws Exception {

    CrcProcessor crc = new CrcProcessor();
    checksumBuffer.forEachByte(crc);
    int localChecksum = crc.getChecksum();
    if (localChecksum != payloadChecksum) {
      writeIndex--;
      throw new Exception(
          "Invalid checksum - header: " + payloadChecksum + " payload: " + localChecksum);
    }
  }

  private void prepareWrite(long serverRequestId, long clientRequestId) {
    if (writeIndex == 0) {
      originationTimestamp = System.currentTimeMillis();
    }
    if (!isAvailable()) {
      throw new IllegalArgumentException(
          "Batch is not available for new writes, batch contract was breached");
    }
    writeIndex++;
  }

  public List<Message> getMessageBatchAsList() {
    List<Message> messageList = new ArrayList<>(messages);
    return messageList;
  }

  public boolean isAvailable() {
    return available.get();
  }

  public void setAvailable(boolean availableStatus) {
    available.set(availableStatus);
    if (!availableStatus) {
      remainingSlots.addAndGet(-size);
      dispatchTimestamp = System.currentTimeMillis();
    } else {
      remainingSlots.addAndGet(size);
    }
  }

  public void reset() {
    for (Message message : messages) {
      message.recycle();
      if (message.getBuf().refCnt() > 0) {
        message.getBuf().release();
      }
    }
    messages.clear();
    writeIndex = 0;
    originationTimestamp = 0;
    dispatchTimestamp = 0;
    setAvailable(true);
  }

  public long getOriginationTimestamp() {
    return originationTimestamp;
  }

  public long getDispatchTimestamp() {
    return dispatchTimestamp;
  }

  public int getWriteIndex() {
    return writeIndex;
  }

  @Override
  public String toString() {
    return "Batch [messages=" + messages + " writeIndex=" + writeIndex + ", available=" + available
        + ", originationTimestamp=" + originationTimestamp + ", dispatchTimestamp="
        + dispatchTimestamp + ", remainingSlots=" + remainingSlots + "]";
  }

  public static String toString(Object[] ary, int tillIndex) {
    StringBuilder builder = new StringBuilder();
    for (int i = 0; i < tillIndex; i++) {
      Object object = ary[i];
      builder.append(object.toString() + ",");
    }
    return builder.toString();
  }

}

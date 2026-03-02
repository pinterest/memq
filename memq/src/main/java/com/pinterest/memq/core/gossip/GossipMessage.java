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

import io.netty.buffer.ByteBuf;

import java.nio.charset.StandardCharsets;

public class GossipMessage {

  private final String brokerId;
  private final int freeSlots;
  private final boolean freeze;
  private final long sendTimestampMs;

  public GossipMessage(String brokerId, int freeSlots, boolean freeze, long sendTimestampMs) {
    this.brokerId = brokerId;
    this.freeSlots = freeSlots;
    this.freeze = freeze;
    this.sendTimestampMs = sendTimestampMs;
  }

  public String getBrokerId() {
    return brokerId;
  }

  public int getFreeSlots() {
    return freeSlots;
  }

  public boolean isFreeze() {
    return freeze;
  }

  public long getSendTimestampMs() {
    return sendTimestampMs;
  }

  public void encode(ByteBuf buf) {
    byte[] idBytes = brokerId.getBytes(StandardCharsets.UTF_8);
    buf.writeShort(idBytes.length);
    buf.writeBytes(idBytes);
    buf.writeInt(freeSlots);
    buf.writeBoolean(freeze);
    buf.writeLong(sendTimestampMs);
  }

  public static GossipMessage decode(ByteBuf buf) {
    int idLen = buf.readUnsignedShort();
    byte[] idBytes = new byte[idLen];
    buf.readBytes(idBytes);
    String brokerId = new String(idBytes, StandardCharsets.UTF_8);
    int freeSlots = buf.readInt();
    boolean freeze = buf.readBoolean();
    long sendTimestampMs = buf.readLong();
    return new GossipMessage(brokerId, freeSlots, freeze, sendTimestampMs);
  }

  @Override
  public String toString() {
    return "GossipMessage{brokerId='" + brokerId + "', freeSlots=" + freeSlots
        + ", freeze=" + freeze + ", sendTimestampMs=" + sendTimestampMs + "}";
  }
}

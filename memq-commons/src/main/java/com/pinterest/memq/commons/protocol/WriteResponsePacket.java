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
package com.pinterest.memq.commons.protocol;

import io.netty.buffer.ByteBuf;

public class WriteResponsePacket implements Packet {

  private String targetBrokerIp;
  private int numSlotsToEvict;
  private int numSlotsOwned;
  /**
   * Explicit broker capability declaration. Set by the broker on every
   * write response it produces to advertise the highest protocol version
   * it understands. Producers use this — not heuristics on slot counts —
   * to decide whether to enable v4 weighted endpoint selection.
   * <p>
   * Default 0 means "broker did not declare a version" (a pre-feature
   * broker, or a default-constructed packet that nobody stamped). New
   * brokers MUST stamp this on every WriteResponsePacket they send.
   */
  private short serverProtocolVersion;

  public WriteResponsePacket() {
  }

  public WriteResponsePacket(String targetBrokerIp, int numSlotsToEvict, int numSlotsOwned) {
    this.targetBrokerIp = targetBrokerIp;
    this.numSlotsToEvict = numSlotsToEvict;
    this.numSlotsOwned = numSlotsOwned;
  }

  @Override
  public void readFields(ByteBuf buf, short protocolVersion) {
    if (protocolVersion >= RequestType.V4) {
      targetBrokerIp = ProtocolUtils.readStringWithTwoByteEncoding(buf);
      if (targetBrokerIp != null && targetBrokerIp.isEmpty()) {
        targetBrokerIp = null;
      }
      numSlotsToEvict = buf.readInt();
      numSlotsOwned = buf.readInt();
      // Backward-compat: serverProtocolVersion was added after the initial
      // v4 release. Older v4 brokers won't write this trailing short, in
      // which case we leave it at 0 ("broker didn't declare").
      if (buf.readableBytes() >= Short.BYTES) {
        serverProtocolVersion = buf.readShort();
      }
    }
  }

  @Override
  public void write(ByteBuf buf, short protocolVersion) {
    if (protocolVersion >= RequestType.V4) {
      ProtocolUtils.writeStringWithTwoByteEncoding(buf, targetBrokerIp);
      buf.writeInt(numSlotsToEvict);
      buf.writeInt(numSlotsOwned);
      buf.writeShort(serverProtocolVersion);
    }
  }

  @Override
  public int getSize(short protocolVersion) {
    if (protocolVersion >= RequestType.V4) {
      return ProtocolUtils.getStringSerializedSizeWithTwoByteEncoding(targetBrokerIp)
          + Integer.BYTES + Integer.BYTES + Short.BYTES;
    }
    return 0;
  }

  public String getTargetBrokerIp() {
    return targetBrokerIp;
  }

  public void setTargetBrokerIp(String targetBrokerIp) {
    this.targetBrokerIp = targetBrokerIp;
  }

  public int getNumSlotsToEvict() {
    return numSlotsToEvict;
  }

  public void setNumSlotsToEvict(int numSlotsToEvict) {
    this.numSlotsToEvict = numSlotsToEvict;
  }

  public int getNumSlotsOwned() {
    return numSlotsOwned;
  }

  public void setNumSlotsOwned(int numSlotsOwned) {
    this.numSlotsOwned = numSlotsOwned;
  }

  public boolean hasEviction() {
    return targetBrokerIp != null && !targetBrokerIp.isEmpty();
  }

  public short getServerProtocolVersion() {
    return serverProtocolVersion;
  }

  public void setServerProtocolVersion(short serverProtocolVersion) {
    this.serverProtocolVersion = serverProtocolVersion;
  }
}

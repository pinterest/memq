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
  private int targetBrokerPort;
  private int numSlotsToEvict;
  private int numSlotsOwned;

  public WriteResponsePacket() {
  }

  public WriteResponsePacket(String targetBrokerIp, int targetBrokerPort,
                             int numSlotsToEvict, int numSlotsOwned) {
    this.targetBrokerIp = targetBrokerIp;
    this.targetBrokerPort = targetBrokerPort;
    this.numSlotsToEvict = numSlotsToEvict;
    this.numSlotsOwned = numSlotsOwned;
  }

  @Override
  public void readFields(ByteBuf buf, short protocolVersion) {
    if (protocolVersion >= 4) {
      targetBrokerIp = ProtocolUtils.readStringWithTwoByteEncoding(buf);
      if (targetBrokerIp != null && targetBrokerIp.isEmpty()) {
        targetBrokerIp = null;
      }
      targetBrokerPort = buf.readInt();
      numSlotsToEvict = buf.readInt();
      numSlotsOwned = buf.readInt();
    }
  }

  @Override
  public void write(ByteBuf buf, short protocolVersion) {
    if (protocolVersion >= 4) {
      ProtocolUtils.writeStringWithTwoByteEncoding(buf, targetBrokerIp);
      buf.writeInt(targetBrokerPort);
      buf.writeInt(numSlotsToEvict);
      buf.writeInt(numSlotsOwned);
    }
  }

  @Override
  public int getSize(short protocolVersion) {
    if (protocolVersion >= 4) {
      return ProtocolUtils.getStringSerializedSizeWithTwoByteEncoding(targetBrokerIp)
          + Integer.BYTES + Integer.BYTES + Integer.BYTES;
    }
    return 0;
  }

  public String getTargetBrokerIp() {
    return targetBrokerIp;
  }

  public void setTargetBrokerIp(String targetBrokerIp) {
    this.targetBrokerIp = targetBrokerIp;
  }

  public int getTargetBrokerPort() {
    return targetBrokerPort;
  }

  public void setTargetBrokerPort(int targetBrokerPort) {
    this.targetBrokerPort = targetBrokerPort;
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
}

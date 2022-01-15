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

import java.io.IOException;

import io.netty.buffer.ByteBuf;

public class RequestPacket extends TransportPacket {
  private Packet payload;

  public RequestPacket() {
  }

  public RequestPacket(short protocolVersion,
                       long clientRequestId,
                       RequestType requestType,
                       Packet payload) {
    super(protocolVersion, clientRequestId, requestType);
    this.payload = payload;
  }

  @Override
  public void readFields(ByteBuf inBuffer, short pv) throws IOException {
    inBuffer.readInt();
    this.protocolVersion = inBuffer.readShort();
    clientRequestId = inBuffer.readLong();
    requestType = RequestType.extractPacketType(inBuffer);
    payload = requestType.requestImplementationSupplier.get();
    payload.readFields(inBuffer, this.protocolVersion);
  }

  @Override
  public void write(ByteBuf outBuf, short protocolVersion) {
    outBuf.writeInt(getSize(protocolVersion));
    outBuf.writeShort(protocolVersion);
    outBuf.writeLong(clientRequestId);
    outBuf.writeByte(requestType.ordinal());
    payload.write(outBuf, protocolVersion);
  }

  @Override
  public int getSize(short protocolVersion) {
    return Short.BYTES + Long.BYTES + Byte.BYTES + payload.getSize(protocolVersion);
  }

  public Packet getPayload() {
    return payload;
  }

  public void setPayload(Packet payload) {
    this.payload = payload;
  }

  @Override
  public void release() throws IOException {
    payload.release();
    super.release();
  }
}

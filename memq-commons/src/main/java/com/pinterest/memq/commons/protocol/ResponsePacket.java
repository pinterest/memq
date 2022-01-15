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

public class ResponsePacket extends TransportPacket {

  protected short responseCode;
  protected String errorMessage;
  protected Packet packet;

  public ResponsePacket() {
  }

  public ResponsePacket(short protocolVersion,
                        long clientRequestId,
                        RequestType requestType,
                        short responseCode,
                        String errorMessage) {
    super(protocolVersion, clientRequestId, requestType);
    this.responseCode = responseCode;
    this.errorMessage = errorMessage;
  }

  public ResponsePacket(short protocolVersion,
                        long clientRequestId,
                        RequestType requestType,
                        short responseCode,
                        Packet payload) {
    super(protocolVersion, clientRequestId, requestType);
    this.responseCode = responseCode;
    this.packet = payload;
  }

  public ResponsePacket(short protocolVersion,
                        long clientRequestId,
                        RequestType requestType,
                        short responseCode,
                        String errorMessage,
                        Packet payload) {
    super(protocolVersion, clientRequestId, requestType);
    this.responseCode = responseCode;
    this.errorMessage = errorMessage;
    this.packet = payload;
  }

  @Override
  public void readFields(ByteBuf buf, short protocolVersion) throws IOException {
    buf.readInt();
    this.protocolVersion = buf.readShort();
    clientRequestId = buf.readLong();
    requestType = RequestType.extractPacketType(buf);
    responseCode = buf.readShort();
    errorMessage = ProtocolUtils.readStringWithTwoByteEncoding(buf);
    if (responseCode == ResponseCodes.OK) {
      packet = requestType.responseImplementationSupplier.get();
      packet.readFields(buf, protocolVersion);
    }
  }

  @Override
  public void write(ByteBuf buf, short protocolVersion) {
    buf.writeInt(getSize(protocolVersion) - Integer.BYTES);
    buf.writeShort(protocolVersion);
    buf.writeLong(clientRequestId);
    buf.writeByte(requestType.ordinal());
    buf.writeShort(responseCode);
    ProtocolUtils.writeStringWithTwoByteEncoding(buf, errorMessage);
    if (packet != null) {
      packet.write(buf, protocolVersion);
    }
  }

  @Override
  public int getSize(short protocolVersion) {
    return Integer.BYTES + Short.BYTES + Long.BYTES + Byte.BYTES + Short.BYTES
        + ProtocolUtils.getStringSerializedSizeWithTwoByteEncoding(errorMessage)
        + (packet != null ? packet.getSize(protocolVersion) : 0);
  }

  @Override
  public void release() throws IOException {
    if (packet != null) {
      packet.release();
    }
    super.release();
  }

  public Packet getPacket() {
    return packet;
  }

  public short getResponseCode() {
    return responseCode;
  }

  public void setResponseCode(short responseCode) {
    this.responseCode = responseCode;
  }

  public String getErrorMessage() {
    return errorMessage;
  }

  public void setErrorMessage(String errorMessage) {
    this.errorMessage = errorMessage;
  }

  @Override
  public String toString() {
    return "ResponsePacket{" +
        "responseCode=" + responseCode +
        ", errorMessage='" + errorMessage + '\'' +
        ", packet=" + packet +
        ", protocolVersion=" + protocolVersion +
        ", clientRequestId=" + clientRequestId +
        ", requestType=" + requestType +
        '}';
  }
}

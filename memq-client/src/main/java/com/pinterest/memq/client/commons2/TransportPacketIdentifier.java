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
package com.pinterest.memq.client.commons2;

import com.pinterest.memq.commons.protocol.TransportPacket;

import io.netty.buffer.ByteBuf;

import java.io.IOException;

public class TransportPacketIdentifier extends TransportPacket {

  public TransportPacketIdentifier(TransportPacket packet) {
    protocolVersion = packet.getProtocolVersion();
    clientRequestId = packet.getClientRequestId();
    requestType = packet.getRequestType();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    TransportPacketIdentifier that = (TransportPacketIdentifier) o;

    if (protocolVersion != that.protocolVersion) {
      return false;
    }
    if (clientRequestId != that.clientRequestId) {
      return false;
    }
    return requestType == that.requestType;
  }

  @Override
  public int hashCode() {
    int result = protocolVersion;
    result = 31 * result + (int) (clientRequestId ^ (clientRequestId >>> 32));
    result = 31 * result + requestType.hashCode();
    return result;
  }

  @Override
  public String toString() {
    return "PacketIdentifier{" +
        "protocolVersion=" + protocolVersion +
        ", clientId=" + clientRequestId +
        ", requestType=" + requestType +
        '}';
  }

  @Override
  public void readFields(ByteBuf buf, short protocolVersion) throws IOException {

  }

  @Override
  public void write(ByteBuf buf, short protocolVersion) {

  }

  @Override
  public int getSize(short protocolVersion) {
    return 0;
  }
}

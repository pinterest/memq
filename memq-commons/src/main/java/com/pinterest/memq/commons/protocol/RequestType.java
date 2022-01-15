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
import java.util.function.Supplier;

import io.netty.buffer.ByteBuf;

/**
 * MemQ request type information. <br>
 * <br>
 * 
 * NOTE: ENUM ORIDINAL VALUEs used for protocol encoding in protocol. DO NOT
 * CHANGE THE ORDERING OF THE ENUM ENTRIES AS THAT WILL CAUSE PROTOCOL
 * CORRUPTION.
 */
public enum RequestType {

                         // WARNING: reordering of request type will break protocol, ordinal values
                         // are used for request type
                         WRITE(() -> new WriteRequestPacket(), () -> new WriteResponsePacket()),
                         TOPIC_METADATA(() -> new TopicMetadataRequestPacket(),
                             () -> new TopicMetadataResponsePacket()),
                         READ(() -> new ReadRequestPacket(), () -> new ReadResponsePacket());

  public Supplier<Packet> requestImplementationSupplier;
  public Supplier<Packet> responseImplementationSupplier;

  private RequestType(Supplier<Packet> requestImplementationSupplier,
                      Supplier<Packet> responseImplementationSupplier) {
    this.requestImplementationSupplier = requestImplementationSupplier;
    this.responseImplementationSupplier = responseImplementationSupplier;
  }

  public static final short PROTOCOL_VERSION = 3;

  public static RequestType extractPacketType(ByteBuf inBuffer) throws IOException {
    int requestTypeCode = (int) inBuffer.readByte();
    RequestType[] values = RequestType.values();
    if (requestTypeCode > values.length - 1) {
      throw new IOException("Invalid request type:" + requestTypeCode);
    }
    RequestType requestType = values[requestTypeCode];// request type
    return requestType;
  }

}

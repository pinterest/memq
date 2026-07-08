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

  /**
   * Wire protocol version.
   * <p>
   * <b>v4+ (current):</b> write requests carry a producer-id and a connection
   * list in which every entry carries the producer's per-target slot count;
   * write responses carry the eviction directive fields. The broker uses the
   * slot counts for connection-count consolidation decisions in
   * {@code CurrConnectionsEvictionStrategy}, so brokers must be upgraded before
   * producers. The connection-list format is identical for versions 4 and 5 --
   * the earlier split (where v4 omitted the per-entry slot counts) has been
   * consolidated, so a single {@code >= V4} gate governs the whole format.
   * <p>
   * <b>v3 (legacy):</b> no producer-id or connection list; routing falls back
   * to client-side round-robin.
   */
  public static final short PROTOCOL_VERSION = 5;

  /**
   * First protocol version that carries a producer-id and a connection list
   * (with per-target slot counts) on write requests, and the eviction
   * directive fields on write responses. This is the single modern wire
   * format; anything below it is legacy v3.
   */
  public static final short V4 = 4;

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

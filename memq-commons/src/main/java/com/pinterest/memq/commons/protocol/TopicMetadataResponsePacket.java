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

public class TopicMetadataResponsePacket implements Packet {

  private TopicMetadata metadata;

  public TopicMetadataResponsePacket() {
  }

  public TopicMetadataResponsePacket(TopicMetadata metadata) {
    this.metadata = metadata;
  }

  @Override
  public void readFields(ByteBuf buf, short protocolVersion) throws IOException {
    metadata = new TopicMetadata();
    metadata.readFields(buf, protocolVersion);
  }

  @Override
  public void write(ByteBuf buf, short protocolVersion) {
    metadata.write(buf, protocolVersion);
  }

  @Override
  public int getSize(short protocolVersion) {
    return metadata.getSize(protocolVersion);
  }

  public TopicMetadata getMetadata() {
    return metadata;
  }

  @Override
  public String toString() {
    return "TopicMetadataResponsePacket [metadata=" + metadata + "]";
  }

}

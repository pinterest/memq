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

public class TopicMetadataRequestPacket implements Packet {

  private String topic;

  public TopicMetadataRequestPacket() {
  }

  public TopicMetadataRequestPacket(String topic) {
    this.topic = topic;
  }

  @Override
  public void readFields(ByteBuf buf, short protocolVersion) throws IOException {
    topic = ProtocolUtils.readStringWithTwoByteEncoding(buf);
  }

  @Override
  public void write(ByteBuf buf, short protocolVersion) {
    ProtocolUtils.writeStringWithTwoByteEncoding(buf, topic);
  }

  @Override
  public int getSize(short protocolVersion) {
    return ProtocolUtils.getStringSerializedSizeWithTwoByteEncoding(topic);
  }
  
  public String getTopic() {
    return topic;
  }

}

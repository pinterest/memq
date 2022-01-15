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
import java.util.HashMap;
import java.util.Map;

import io.netty.buffer.ByteBuf;

public class ClusterMetadataResponse implements Packet {

  private Map<String, TopicMetadata> topicMetadataMap = new HashMap<>();

  public ClusterMetadataResponse() {
  }

  public ClusterMetadataResponse(Map<String, TopicMetadata> topicMetadataMap) {
    this.topicMetadataMap = topicMetadataMap;
  }

  @Override
  public void readFields(ByteBuf buf, short protocolVersion) throws IOException {
    short topicCount = buf.readShort();
    for (int i = 0; i < topicCount; i++) {
      String topicName = ProtocolUtils.readStringWithTwoByteEncoding(buf);
      TopicMetadata md = new TopicMetadata();
      md.readFields(buf, protocolVersion);
      topicMetadataMap.put(topicName, md);
    }
  }

  @Override
  public void write(ByteBuf buf, short protocolVersion) {

  }

  @Override
  public int getSize(short protocolVersion) {
    return topicMetadataMap.entrySet().stream()
        .mapToInt(e -> ProtocolUtils.getStringSerializedSizeWithTwoByteEncoding(e.getKey())
            + e.getValue().getSize(protocolVersion))
        .sum();
  }

}

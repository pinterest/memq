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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import io.netty.buffer.ByteBuf;

/**
 * Wire format: topic strings written consecutively with 2-byte length encoding.
 * An empty list signals "list all topics".
 * <p>
 * Backward compatible: old clients write 1 string, new brokers read all
 * available; new clients writing N strings to old brokers is safe because
 * old brokers read only the first string.
 */
public class TopicMetadataRequestPacket implements Packet {

  private List<String> topics = Collections.emptyList();

  public TopicMetadataRequestPacket() {
  }

  public TopicMetadataRequestPacket(String topic) {
    this.topics = Collections.singletonList(topic);
  }

  public TopicMetadataRequestPacket(Collection<String> topics) {
    this.topics = new ArrayList<>(topics);
  }

  @Override
  public void readFields(ByteBuf buf, short protocolVersion) throws IOException {
    topics = new ArrayList<>();
    while (buf.isReadable()) {
      topics.add(ProtocolUtils.readStringWithTwoByteEncoding(buf));
    }
  }

  @Override
  public void write(ByteBuf buf, short protocolVersion) {
    for (String topic : topics) {
      ProtocolUtils.writeStringWithTwoByteEncoding(buf, topic);
    }
  }

  @Override
  public int getSize(short protocolVersion) {
    int size = 0;
    for (String topic : topics) {
      size += ProtocolUtils.getStringSerializedSizeWithTwoByteEncoding(topic);
    }
    return size;
  }

  public String getTopic() {
    return topics.isEmpty() ? null : topics.get(0);
  }

  public List<String> getTopics() {
    return topics;
  }
}

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
 * Wire format: TopicMetadata objects written consecutively.
 * <p>
 * Backward compatible: old brokers write 1 metadata, new clients read all
 * available (get 1). New brokers writing N metadata to old clients is safe
 * because old clients read only the first.
 */
public class TopicMetadataResponsePacket implements Packet {

  private List<TopicMetadata> metadataList = new ArrayList<>();

  public TopicMetadataResponsePacket() {
  }

  public TopicMetadataResponsePacket(TopicMetadata metadata) {
    this.metadataList = Collections.singletonList(metadata);
  }

  public TopicMetadataResponsePacket(Collection<TopicMetadata> metadataList) {
    this.metadataList = new ArrayList<>(metadataList);
  }

  @Override
  public void readFields(ByteBuf buf, short protocolVersion) throws IOException {
    metadataList = new ArrayList<>();
    while (buf.isReadable()) {
      TopicMetadata md = new TopicMetadata();
      md.readFields(buf, protocolVersion);
      metadataList.add(md);
    }
  }

  @Override
  public void write(ByteBuf buf, short protocolVersion) {
    for (TopicMetadata md : metadataList) {
      md.write(buf, protocolVersion);
    }
  }

  @Override
  public int getSize(short protocolVersion) {
    int size = 0;
    for (TopicMetadata md : metadataList) {
      size += md.getSize(protocolVersion);
    }
    return size;
  }

  public TopicMetadata getMetadata() {
    return metadataList.isEmpty() ? null : metadataList.get(0);
  }

  public List<TopicMetadata> getMetadataList() {
    return metadataList;
  }

  @Override
  public String toString() {
    return "TopicMetadataResponsePacket [metadataList=" + metadataList + "]";
  }
}

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
import java.util.HashSet;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;

import io.netty.buffer.ByteBuf;

public class TopicMetadata implements Packet {

  private String topicName;
  private Set<Broker> writeBrokers = new HashSet<>();
  private Set<Broker> readBrokers = new HashSet<>();
  private String storageHandlerName;
  private Properties storageHandlerConfig = new Properties();

  public TopicMetadata() {
  }

  public TopicMetadata(String topicName,
                       String storageHandlerName,
                       Properties storageHandlerConfig) {
    this.topicName = topicName;
    this.storageHandlerName = storageHandlerName;
    this.storageHandlerConfig = storageHandlerConfig;
  }

  public TopicMetadata(TopicConfig config) {
    this.topicName = config.getTopic();
    this.storageHandlerName = config.getStorageHandlerName();
    this.storageHandlerConfig = config.getStorageHandlerConfig();
  }

  public TopicMetadata(String topicName,
                       Set<Broker> writeBrokers,
                       Set<Broker> readBrokers,
                       String storageHandlerName,
                       Properties storageHandlerConfig) {
    this.topicName = topicName;
    this.writeBrokers = writeBrokers;
    this.readBrokers = readBrokers;
    this.storageHandlerName = storageHandlerName;
    this.storageHandlerConfig = storageHandlerConfig;
  }

  public String getTopicName() {
    return topicName;
  }

  public void setTopicName(String topicName) {
    this.topicName = topicName;
  }

  @Override
  public void readFields(ByteBuf buf, short protocolVersion) throws IOException {
    topicName = ProtocolUtils.readStringWithTwoByteEncoding(buf);
    short brokerCount = buf.readShort();
    for (int i = 0; i < brokerCount; i++) {
      Broker broker = new Broker();
      broker.readFields(buf, protocolVersion);
      writeBrokers.add(broker);
    }
    storageHandlerName = ProtocolUtils.readStringWithTwoByteEncoding(buf);
    short numProperties = buf.readShort();
    for (int i = 0; i < numProperties; i++) {
      storageHandlerConfig.setProperty(ProtocolUtils.readStringWithTwoByteEncoding(buf),
          ProtocolUtils.readStringWithTwoByteEncoding(buf));
    }
    if (protocolVersion >= 3) {
      // get read brokers
      brokerCount = buf.readShort();
      for (int i = 0; i < brokerCount; i++) {
        Broker broker = new Broker();
        broker.readFields(buf, protocolVersion);
        readBrokers.add(broker);
      }
    }
  }

  @Override
  public void write(ByteBuf buf, short protocolVersion) {
    ProtocolUtils.writeStringWithTwoByteEncoding(buf, topicName);
    buf.writeShort((short) writeBrokers.size());
    for (Broker broker : writeBrokers) {
      broker.write(buf, protocolVersion);
    }
    ProtocolUtils.writeStringWithTwoByteEncoding(buf, storageHandlerName);
    buf.writeShort((short) storageHandlerConfig.size());
    for (Entry<Object, Object> entry : storageHandlerConfig.entrySet()) {
      ProtocolUtils.writeStringWithTwoByteEncoding(buf, entry.getKey().toString());
      ProtocolUtils.writeStringWithTwoByteEncoding(buf, entry.getValue().toString());
    }
    if (protocolVersion >= 3) {
      // send read brokers
      buf.writeShort((short) readBrokers.size());
      for (Broker broker : readBrokers) {
        broker.write(buf, protocolVersion);
      }
    }
  }

  public Set<Broker> getWriteBrokers() {
    return writeBrokers;
  }

  public Set<Broker> getReadBrokers() {
    return readBrokers;
  }

  public String getStorageHandlerName() {
    return storageHandlerName;
  }

  public void setStorageHandlerName(String storageHandlerName) {
    this.storageHandlerName = storageHandlerName;
  }

  public Properties getStorageHandlerConfig() {
    return storageHandlerConfig;
  }

  @Override
  public int getSize(short protocolVersion) {
    int s = ProtocolUtils.getStringSerializedSizeWithTwoByteEncoding(topicName) + Short.BYTES
        + writeBrokers.stream().mapToInt(b -> b.getSize(protocolVersion)).sum()
        + ProtocolUtils.getStringSerializedSizeWithTwoByteEncoding(storageHandlerName) + Short.BYTES
        + storageHandlerConfig.entrySet().stream().mapToInt(
            e -> ProtocolUtils.getStringSerializedSizeWithTwoByteEncoding(e.getKey().toString())
                + ProtocolUtils.getStringSerializedSizeWithTwoByteEncoding(e.getValue().toString()))
            .sum();
    if (protocolVersion >= 3) {
      s += Short.BYTES + readBrokers.stream().mapToInt(b -> b.getSize(protocolVersion)).sum();
    }
    return s;
  }

  @Override
  public String toString() {
    return "TopicMetadata [topicName=" + topicName + ", writeBrokers=" + writeBrokers
        + ", readBrokers=" + readBrokers + ", storageType=" + storageHandlerName
        + ", storageProperties=" + storageHandlerConfig + "]";
  }

}

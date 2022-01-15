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
import java.util.Set;

import io.netty.buffer.ByteBuf;

public class Broker implements Packet, Comparable<Broker> {

  public static enum BrokerType {
                                 WRITE,
                                 READ,
                                 READ_WRITE
  }

  private String brokerIP;
  private short brokerPort;
  private String instanceType;
  private String locality;
  private Set<TopicAssignment> assignedTopics = new HashSet<>();
  private int totalNetworkCapacity;
  private BrokerType brokerType = BrokerType.WRITE;

  public Broker() {
  }

  public Broker(Broker broker) {
    this.brokerIP = broker.brokerIP;
    this.brokerPort = broker.brokerPort;
    this.instanceType = broker.instanceType;
    this.locality = broker.locality;
    this.assignedTopics = broker.assignedTopics;
    this.totalNetworkCapacity = broker.totalNetworkCapacity;
    this.brokerType = broker.brokerType;
  }

  public Broker(String brokerIP,
                short brokerPort,
                String instanceType,
                String locality,
                BrokerType brokerType,
                Set<TopicAssignment> assignedTopics) {
    this.brokerIP = brokerIP;
    this.brokerPort = brokerPort;
    this.instanceType = instanceType;
    this.locality = locality;
    this.assignedTopics = assignedTopics;
    this.brokerType = brokerType;
  }

  private int getUsedNetworkCapacity() {
    return assignedTopics.stream().mapToInt(t -> (int) t.getInputTrafficMB()).sum();
  }

  public int getAvailableCapacity() {
    return totalNetworkCapacity - getUsedNetworkCapacity();
  }

  public int getTotalNetworkCapacity() {
    return totalNetworkCapacity;
  }

  public void setTotalNetworkCapacity(int totalNetworkCapacity) {
    this.totalNetworkCapacity = totalNetworkCapacity;
  }

  public String getBrokerIP() {
    return brokerIP;
  }

  public void setBrokerIP(String brokerIP) {
    this.brokerIP = brokerIP;
  }

  public short getBrokerPort() {
    return brokerPort;
  }

  public void setBrokerPort(short brokerPort) {
    this.brokerPort = brokerPort;
  }

  public String getInstanceType() {
    return instanceType;
  }

  public void setInstanceType(String instanceType) {
    this.instanceType = instanceType;
  }

  public String getLocality() {
    return locality;
  }

  public void setLocality(String locality) {
    this.locality = locality;
  }

  public Set<TopicAssignment> getAssignedTopics() {
    return assignedTopics;
  }

  public void setAssignedTopics(Set<TopicAssignment> assignedTopics) {
    this.assignedTopics = assignedTopics;
  }

  @Override
  public int hashCode() {
    return brokerIP.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof Broker) {
      return ((Broker) obj).getBrokerIP().equals(brokerIP);
    }
    return false;
  }

  @Override
  public int compareTo(Broker o) {
    return brokerIP.compareTo(o.getBrokerIP());
  }

  public BrokerType getBrokerType() {
    return brokerType;
  }

  public void setBrokerType(BrokerType brokerType) {
    this.brokerType = brokerType;
  }

  @Override
  public String toString() {
    return "Broker [brokerIP=" + brokerIP + ", brokerPort=" + brokerPort + ", instanceType="
        + instanceType + ", locality=" + locality + ", totalNetworkCapacity=" + totalNetworkCapacity
        + ", brokerType=" + brokerType + "]";
  }

  @Override
  public void readFields(ByteBuf buf, short protocolVersion) throws IOException {
    locality = ProtocolUtils.readStringWithTwoByteEncoding(buf);
    brokerIP = ProtocolUtils.readStringWithTwoByteEncoding(buf);
    brokerPort = buf.readShort();
  }

  @Override
  public void write(ByteBuf buf, short protocolVersion) {
    ProtocolUtils.writeStringWithTwoByteEncoding(buf, locality);
    ProtocolUtils.writeStringWithTwoByteEncoding(buf, brokerIP);
    buf.writeShort(brokerPort);
  }

  @Override
  public int getSize(short protocolVersion) {
    return ProtocolUtils.getStringSerializedSizeWithTwoByteEncoding(locality)
        + ProtocolUtils.getStringSerializedSizeWithTwoByteEncoding(brokerIP) + Short.BYTES;
  }

}

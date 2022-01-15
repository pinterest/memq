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
package com.pinterest.memq.core.rpc;

public class TestAuditMessage {

  byte[] cluster, hash, topic, hostAddress;
  long epoch, id;
  int messageCount;
  boolean isProducer;

  public TestAuditMessage(byte[] cluster,
                          byte[] hash,
                          byte[] topic,
                          byte[] hostAddress,
                          long epoch,
                          long id,
                          int messageCount,
                          boolean isProducer) {
    super();
    this.cluster = cluster;
    this.hash = hash;
    this.topic = topic;
    this.hostAddress = hostAddress;
    this.epoch = epoch;
    this.id = id;
    this.messageCount = messageCount;
    this.isProducer = isProducer;
  }

  public byte[] getCluster() {
    return cluster;
  }

  public void setCluster(byte[] cluster) {
    this.cluster = cluster;
  }

  public byte[] getHash() {
    return hash;
  }

  public void setHash(byte[] hash) {
    this.hash = hash;
  }

  public byte[] getTopic() {
    return topic;
  }

  public void setTopic(byte[] topic) {
    this.topic = topic;
  }

  public byte[] getHostAddress() {
    return hostAddress;
  }

  public void setHostAddress(byte[] hostAddress) {
    this.hostAddress = hostAddress;
  }

  public long getEpoch() {
    return epoch;
  }

  public void setEpoch(long epoch) {
    this.epoch = epoch;
  }

  public long getId() {
    return id;
  }

  public void setId(long id) {
    this.id = id;
  }

  public int getMessageCount() {
    return messageCount;
  }

}
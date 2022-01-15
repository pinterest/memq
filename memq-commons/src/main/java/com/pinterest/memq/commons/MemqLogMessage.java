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
package com.pinterest.memq.commons;

import java.util.Map;

public class MemqLogMessage<K, V> {

  public static final String INTERNAL_FIELD_TOPIC = "topic";
  public static final String INTERNAL_FIELD_WRITE_TIMESTAMP = "wts";
  public static final String INTERNAL_FIELD_NOTIFICATION_PARTITION_ID = "npi";
  public static final String INTERNAL_FIELD_NOTIFICATION_READ_TIMESTAMP = "nrts";
  public static final String INTERNAL_FIELD_NOTIFICATION_PARTITION_OFFSET = "npo";
  public static final String INTERNAL_FIELD_OBJECT_SIZE = "objectSize";
  public static final String INTERNAL_FIELD_MESSAGE_OFFSET = "mo";
  
  private final K key;
  private final V value;
  private final MessageId messageId;
  private final Map<String, byte[]> headers;
  private long writeTimestamp;
  private long messageOffsetInBatch;
  private int notificationPartitionId;
  private long notificationPartitionOffset;
  private long notificationReadTimestamp;
  private final boolean endOfBatch;

  public MemqLogMessage(MessageId messageId, Map<String, byte[]> headers, K key, V value, boolean endOfBatch) {
    this.messageId = messageId;
    this.headers = headers;
    this.value = value;
    this.key = key;
    this.endOfBatch = endOfBatch;
  }

  public MessageId getMessageId() {
    return messageId;
  }

  public Map<String, byte[]> getHeaders() {
    return headers;
  }

  public K getKey() {
    return key;
  }

  public V getValue() {
    return value;
  }

  public long getWriteTimestamp() {
    return writeTimestamp;
  }

  public void setWriteTimestamp(long writeTimestamp) {
    this.writeTimestamp = writeTimestamp;
  }

  public long getMessageOffsetInBatch() {
    return messageOffsetInBatch;
  }

  public void setMessageOffsetInBatch(long messageOffsetInBatch) {
    this.messageOffsetInBatch = messageOffsetInBatch;
  }

  public int getNotificationPartitionId() {
    return notificationPartitionId;
  }

  public void setNotificationPartitionId(int notificationPartitionId) {
    this.notificationPartitionId = notificationPartitionId;
  }

  public long getNotificationPartitionOffset() {
    return notificationPartitionOffset;
  }

  public void setNotificationPartitionOffset(long notificationPartitionOffset) {
    this.notificationPartitionOffset = notificationPartitionOffset;
  }

  public long getNotificationReadTimestamp() {
    return notificationReadTimestamp;
  }

  public void setNotificationReadTimestamp(long notificationReadTimestamp) {
    this.notificationReadTimestamp = notificationReadTimestamp;
  }

  public boolean isEndOfBatch() {
    return endOfBatch;
  }

  @Override
  public String toString() {
    return "[key=" + key + ", value=" + value + ", messageId=" + messageId + ", headers=" + headers
        + ", writeTimestamp=" + writeTimestamp + ", messageOffsetInBatch=" + messageOffsetInBatch
        + ", notificationPartitionId=" + notificationPartitionId + ", notificationPartitionOffset="
        + notificationPartitionOffset + ", notificationReadTimestamp=" + notificationReadTimestamp
        + "]";
  }

}

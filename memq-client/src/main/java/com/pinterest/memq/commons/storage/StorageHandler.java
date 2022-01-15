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
package com.pinterest.memq.commons.storage;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Properties;

import com.codahale.metrics.MetricRegistry;
import com.google.gson.JsonObject;
import com.pinterest.memq.client.commons2.DataNotFoundException;
import com.pinterest.memq.commons.BatchHeader;
import com.pinterest.memq.commons.BatchHeader.IndexEntry;
import com.pinterest.memq.commons.protocol.BatchData;
import com.pinterest.memq.core.commons.Message;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;

public interface StorageHandler {

  String SIZE = "objectSize";

  default void initWriter(Properties outputHandlerConfig,
                          String topic,
                          MetricRegistry registry) throws Exception {
  }

  /**
   * Reconfigure the storage handler with new configs
   * 
   * @param outputHandlerConfig the new configs
   * @return true if configuration is accepted, otherwise false
   */
  default boolean reconfigure(Properties outputHandlerConfig) {
    return true;
  };

  void writeOutput(int sizeInBytes,
                   int checksum,
                   List<Message> messages) throws WriteFailedException;

  String getReadUrl();

  static ByteBuf getBatchHeadersAsByteArray(final List<Message> messages) {
    // index bytes 12 bytes per
    // index entry
    int length = Integer.BYTES * 2 + // header length
        messages.size() * Integer.BYTES * 3;
    ByteBuf header = PooledByteBufAllocator.DEFAULT.directBuffer(length, length);
    header.writeInt(header.capacity() - Integer.BYTES);// header length
    writeMessageIndex(messages, header, length);
    return header;
  }

  static void writeMessageIndex(final List<Message> messages, ByteBuf header, int capacity) {
    // build message index
    header.writeInt(messages.size());
    int offset = capacity;
    for (int i = 0; i < messages.size(); i++) {
      Message message = messages.get(i);
      int size = message.getBuf().readableBytes();
      header.writeInt(i);//
      header.writeInt(offset);
      header.writeInt(size);
      offset += size;
    }
  }

  default void closeWriter() {
  }

  default void initReader(Properties properties, MetricRegistry registry) throws Exception {
  }

  default InputStream fetchBatchStreamForNotification(JsonObject nextNotificationToProcess) throws IOException,
                                                                                            DataNotFoundException {
    throw new UnsupportedOperationException();
  }

  default BatchData fetchBatchStreamForNotificationBuf(JsonObject nextNotificationToProcess) throws IOException,
                                                                                             DataNotFoundException {
    throw new UnsupportedOperationException();
  }

  default BatchHeader fetchHeaderForBatch(JsonObject nextNotificationToProcess) throws IOException,
                                                                                DataNotFoundException {
    throw new UnsupportedOperationException();
  }

  default BatchData fetchHeaderForBatchBuf(JsonObject nextNotificationToProcess) throws IOException,
                                                                                 DataNotFoundException {
    throw new UnsupportedOperationException();
  }

  default DataInputStream fetchMessageAtIndex(JsonObject objectNotification,
                                              IndexEntry index) throws IOException,
                                                                DataNotFoundException {
    throw new UnsupportedOperationException();
  }

  default int getBatchSizeFromNotification(JsonObject notification) {
    return notification.get(SIZE).getAsInt();
  }

  default void closeReader() {
  }

  default BatchData fetchMessageAtIndexBuf(JsonObject objectNotification,
                                           IndexEntry index) throws IOException, DataNotFoundException {
    throw new UnsupportedOperationException();
  }

}

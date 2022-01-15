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
package com.pinterest.memq.client.commons;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Semaphore;
import java.util.function.BiFunction;

import org.apache.commons.compress.utils.IOUtils;

import com.codahale.metrics.MetricRegistry;
import com.google.gson.JsonObject;
import com.pinterest.memq.client.commons.serde.ByteArrayDeserializer;
import com.pinterest.memq.client.producer.MemqProducer;
import com.pinterest.memq.client.producer.netty.MemqNettyRequest;
import com.pinterest.memq.commons.MemqLogMessage;
import com.pinterest.memq.commons.MessageId;

public class TestUtils {

  public static InputStream getBatchHeadersAsInputStream(final List<ByteBuffer> messages) throws IOException {
    ByteBuffer header = ByteBuffer.allocate(Integer.BYTES * 2 + // header length
        messages.size() * Integer.BYTES * 3 // index bytes 12 bytes per index entry
    );
    header.putInt(header.capacity() - Integer.BYTES);// header length

    writeMessageIndex(messages, header);
    return new ByteArrayInputStream(header.array());
  }

  public static void writeMessageIndex(final List<ByteBuffer> messages, ByteBuffer header) {
    // build message index
    header.putInt(messages.size());
    int offset = header.limit();
    for (int i = 0; i < messages.size(); i++) {
      ByteBuffer message = messages.get(i);
      int size = message.limit();
      header.putInt(i);//
      header.putInt(offset);
      header.putInt(size);
      offset += size;
    }
  }

  public static byte[] getMemqBatchData(String baseLogMessage,
                                        BiFunction<String, Integer, byte[]> getLogMessageBytes,
                                        int logMessageCount,
                                        int msgs,
                                        boolean enableMessageId,
                                        Compression compression,
                                        List<byte[]> messageIdHashes,
                                        boolean enableTestHeaders) throws Exception {
    List<ByteBuffer> bufList = new ArrayList<>();
    for (int l = 0; l < msgs; l++) {
      byte[] rawData = createMessage(baseLogMessage, getLogMessageBytes, logMessageCount,
          enableMessageId, compression, messageIdHashes, enableTestHeaders);
      bufList.add(ByteBuffer.wrap(rawData));
    }
    ByteArrayOutputStream os = new ByteArrayOutputStream();
    InputStream buf = getBatchHeadersAsInputStream(bufList);
    IOUtils.copy(buf, os);
    for (ByteBuffer byteBuffer : bufList) {
      os.write(byteBuffer.array());
    }
    System.out.println("Created:" + bufList.size() + " buffers");
    return os.toByteArray();
  }

  public static byte[] createMessage(String baseLogMessage,
                                     BiFunction<String, Integer, byte[]> getLogMessageBytes,
                                     int logMessageCount,
                                     boolean enableMessageId,
                                     Compression compression,
                                     List<byte[]> messageIdHashes,
                                     boolean enableTestHeaders) throws IOException {
    Semaphore maxRequestLock = new Semaphore(1);
    MemqNettyRequest task = new MemqNettyRequest("xyz", 1L, Compression.GZIP,
        maxRequestLock, true, 1024 * 1024, 100, null, null, 10_000, false);
    for (int k = 0; k < logMessageCount; k++) {
      byte[] bytes = getLogMessageBytes.apply(baseLogMessage, k);
      MessageId id = null;
      if (enableMessageId) {
        id = new SimpleMessageId(k);
      }
      byte[] headerBytes = null;
      if (enableTestHeaders) {
        Map<String, byte[]> headers = new HashMap<>();
        headers.put("test", "value".getBytes());
        headerBytes = MemqProducer.serializeHeadersToByteArray(headers);
      }
      MemqProducer.writeMemqLogMessage(id, headerBytes, null, bytes, task,
          System.currentTimeMillis());
    }
    task.markReady();
    task.getOutputStream().close();
    if (messageIdHashes != null) {
      messageIdHashes.add(task.getMessageIdHash());
    }
    byte[] rawData = task.getPayloadAsByteArrays();
    task.getBuffer().release();
    return rawData;
  }

  public static MemqLogMessageIterator<byte[], byte[]> getTestDataIterator(String baseLogMessage,
                                                                           BiFunction<String, Integer, byte[]> getLogMessageBytes,
                                                                           int logMessageCount,
                                                                           int msgs,
                                                                           Compression compression,
                                                                           boolean enableHeaders) throws Exception {
    byte[] rawData = getMemqBatchData(baseLogMessage, getLogMessageBytes, logMessageCount, msgs,
        false, compression, null, enableHeaders);

    ByteArrayInputStream in = new ByteArrayInputStream(rawData);
    DataInputStream stream = new DataInputStream(in);
    JsonObject obj = new JsonObject();
    obj.addProperty(MemqLogMessage.INTERNAL_FIELD_TOPIC, "test");
    obj.addProperty(MemqLogMessage.INTERNAL_FIELD_OBJECT_SIZE, rawData.length);
    obj.addProperty(MemqLogMessage.INTERNAL_FIELD_NOTIFICATION_PARTITION_ID, 1);
    obj.addProperty(MemqLogMessage.INTERNAL_FIELD_NOTIFICATION_PARTITION_OFFSET, 1);
    obj.addProperty(MemqLogMessage.INTERNAL_FIELD_NOTIFICATION_READ_TIMESTAMP,
        System.currentTimeMillis());
    return new MemqLogMessageIterator<>("test", "test", stream, obj, new ByteArrayDeserializer(),
        new ByteArrayDeserializer(), new MetricRegistry(), false, null);
  }

  public static MemqLogMessageIterator<byte[], byte[]> getTestDataIteratorWithAllFields(String baseLogMessage,
                                                                                        BiFunction<String, Integer, byte[]> getLogMessageBytes,
                                                                                        int logMessageCount,
                                                                                        int msgs,
                                                                                        Compression compression,
                                                                                        boolean enableHeaders) throws Exception {
    byte[] rawData = getMemqBatchData(baseLogMessage, getLogMessageBytes, logMessageCount, msgs,
        true, compression, null, enableHeaders);
    ByteArrayInputStream in = new ByteArrayInputStream(rawData);
    DataInputStream stream = new DataInputStream(in);
    JsonObject obj = new JsonObject();
    obj.addProperty(MemqLogMessage.INTERNAL_FIELD_TOPIC, "test");
    obj.addProperty(MemqLogMessage.INTERNAL_FIELD_OBJECT_SIZE, rawData.length);
    obj.addProperty(MemqLogMessage.INTERNAL_FIELD_NOTIFICATION_PARTITION_ID, 1);
    obj.addProperty(MemqLogMessage.INTERNAL_FIELD_NOTIFICATION_PARTITION_OFFSET, 1);
    obj.addProperty(MemqLogMessage.INTERNAL_FIELD_NOTIFICATION_READ_TIMESTAMP,
        System.currentTimeMillis());
    return new MemqLogMessageIterator<>("test", "test", stream, obj, new ByteArrayDeserializer(),
        new ByteArrayDeserializer(), new MetricRegistry(), false, null);
  }
}
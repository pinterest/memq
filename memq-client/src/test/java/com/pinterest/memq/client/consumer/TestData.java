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
package com.pinterest.memq.client.consumer;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.util.Base64;
import java.util.function.BiFunction;

import org.junit.Test;

import com.codahale.metrics.MetricRegistry;
import com.google.gson.JsonObject;
import com.pinterest.memq.client.commons.Compression;
import com.pinterest.memq.client.commons.MemqLogMessageIterator;
import com.pinterest.memq.client.commons.TestUtils;
import com.pinterest.memq.client.commons.serde.ByteArrayDeserializer;
import com.pinterest.memq.commons.MemqLogMessage;

public class TestData {
  
  @Test
  public void testCompressionNone() throws Exception {
    BiFunction<String, Integer, byte[]> getLogMessageBytes = (base, k) -> base.getBytes();
    byte[] memqBatchData = TestUtils.getMemqBatchData("test1231231", getLogMessageBytes, 2, 5, true,
        Compression.NONE, null, true);

    byte[] decode = Base64.getDecoder().decode(Base64.getEncoder().encodeToString(memqBatchData));
    DataInputStream dataInputStream = new DataInputStream(new ByteArrayInputStream(decode));
    JsonObject currNotificationObj = new JsonObject();
    currNotificationObj.addProperty(MemqLogMessage.INTERNAL_FIELD_TOPIC, "test");
    currNotificationObj.addProperty(MemqLogMessage.INTERNAL_FIELD_OBJECT_SIZE, decode.length);
    MemqLogMessageIterator<byte[], byte[]> memqLogMessageIterator = new MemqLogMessageIterator<byte[], byte[]>(
        "test", "client0", dataInputStream, currNotificationObj, new ByteArrayDeserializer(),
        new ByteArrayDeserializer(), new MetricRegistry(), false, null);
    while (memqLogMessageIterator.hasNext()) {
      memqLogMessageIterator.next();
    }
    memqLogMessageIterator.close();
  }

  @Test
  public void testCompressionZstd() throws Exception {
    BiFunction<String, Integer, byte[]> getLogMessageBytes = (base, k) -> base.getBytes();
    byte[] memqBatchData = TestUtils.getMemqBatchData("test1231231", getLogMessageBytes, 2, 5, true,
        Compression.ZSTD, null, true);

    byte[] decode = Base64.getDecoder().decode(Base64.getEncoder().encodeToString(memqBatchData));
    DataInputStream dataInputStream = new DataInputStream(new ByteArrayInputStream(decode));
    JsonObject currNotificationObj = new JsonObject();
    currNotificationObj.addProperty(MemqLogMessage.INTERNAL_FIELD_TOPIC, "test");
    currNotificationObj.addProperty(MemqLogMessage.INTERNAL_FIELD_OBJECT_SIZE, decode.length);
    MemqLogMessageIterator<byte[], byte[]> memqLogMessageIterator = new MemqLogMessageIterator<byte[], byte[]>(
        "test", "client0", dataInputStream, currNotificationObj, new ByteArrayDeserializer(),
        new ByteArrayDeserializer(), new MetricRegistry(), false, null);
    while (memqLogMessageIterator.hasNext()) {
      memqLogMessageIterator.next();
    }
    memqLogMessageIterator.close();
  }

  
  @Test
  public void testCompressionGzip() throws Exception {
    BiFunction<String, Integer, byte[]> getLogMessageBytes = (base, k) -> base.getBytes();
    byte[] memqBatchData = TestUtils.getMemqBatchData("test1231231", getLogMessageBytes, 2, 5, true,
        Compression.GZIP, null, true);

    byte[] decode = Base64.getDecoder().decode(Base64.getEncoder().encodeToString(memqBatchData));
    DataInputStream dataInputStream = new DataInputStream(new ByteArrayInputStream(decode));
    JsonObject currNotificationObj = new JsonObject();
    currNotificationObj.addProperty(MemqLogMessage.INTERNAL_FIELD_TOPIC, "test");
    currNotificationObj.addProperty(MemqLogMessage.INTERNAL_FIELD_OBJECT_SIZE, decode.length);
    MemqLogMessageIterator<byte[], byte[]> memqLogMessageIterator = new MemqLogMessageIterator<byte[], byte[]>(
        "test", "client0", dataInputStream, currNotificationObj, new ByteArrayDeserializer(),
        new ByteArrayDeserializer(), new MetricRegistry(), false, null);
    while (memqLogMessageIterator.hasNext()) {
      memqLogMessageIterator.next();
    }
    memqLogMessageIterator.close();
  }
  
//  @Test
//  public void testCompressionLz4() throws Exception {
//    BiFunction<String, Integer, byte[]> getLogMessageBytes = (base, k) -> base.getBytes();
//    byte[] memqBatchData = TestUtils.getMemqBatchData("test1231231", getLogMessageBytes, 2, 5, true,
//        Compression.LZ4, null, true);
//
//    byte[] decode = Base64.getDecoder().decode(Base64.getEncoder().encodeToString(memqBatchData));
//    DataInputStream dataInputStream = new DataInputStream(new ByteArrayInputStream(decode));
//    JsonObject currNotificationObj = new JsonObject();
//    currNotificationObj.addProperty(MemqLogMessage.INTERNAL_FIELD_TOPIC, "test");
//    currNotificationObj.addProperty(MemqLogMessage.INTERNAL_FIELD_OBJECT_SIZE, decode.length);
//    MemqLogMessageIterator<byte[], byte[]> memqLogMessageIterator = new MemqLogMessageIterator<byte[], byte[]>(
//        "test", "client0", dataInputStream, currNotificationObj, new ByteArrayDeserializer(),
//        new ByteArrayDeserializer(), new MetricRegistry(), false, null);
//    while (memqLogMessageIterator.hasNext()) {
//      memqLogMessageIterator.next();
//    }
//    memqLogMessageIterator.close();
//  }
}

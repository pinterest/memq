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
  public void testData() throws Exception {
    BiFunction<String, Integer, byte[]> getLogMessageBytes = (base, k) -> base.getBytes();
    byte[] memqBatchData = TestUtils.getMemqBatchData("test1231231", getLogMessageBytes, 2,
        5, true, Compression.ZSTD, null, true);
    
    String encodeToString = Base64.getEncoder().encodeToString(memqBatchData);
    System.out.println(encodeToString.length());
    System.out.print(encodeToString);

//    byte[] decode = Base64.getDecoder().decode(
//        "AAAAHAAAAAIAAAAAAAAAIAAAAJIAAAABAAAAsgAAAJIAKABkABUErBAAJQAAAXdqDH1EAAAAAAAAAAGRLVIQAAAAAAIAAABqACAAAAF3agx9QwgAAAAAAAAAAAANAAR0ZXN0AAV2YWx1ZQAAAAAAAAALdGVzdDEyMzEyMzEAIAAAAXdqDH1DCAAAAAAAAAABAA0ABHRlc3QABXZhbHVlAAAAAAAAAAt0ZXN0MTIzMTIzMQAoAGQAFQSsEAAlAAABd2oMfUUAAAAAAAAAAf6kvmoAAAAAAgAAAGoAIAAAAXdqDH1ECAAAAAAAAAAAAA0ABHRlc3QABXZhbHVlAAAAAAAAAAt0ZXN0MTIzMTIzMQAgAAABd2oMfUUIAAAAAAAAAAEADQAEdGVzdAAFdmFsdWUAAAAAAAAAC3Rlc3QxMjMxMjMx");
//    DataInputStream dataInputStream = new DataInputStream(new ByteArrayInputStream(decode));
//
//    JsonObject currNotificationObj = new JsonObject();
//    currNotificationObj.addProperty(MemqLogMessage.INTERNAL_FIELD_TOPIC, "test");
//    currNotificationObj.addProperty(MemqLogMessage.INTERNAL_FIELD_OBJECT_SIZE, decode.length);
//    MemqLogMessageIterator<byte[], byte[]> memqLogMessageIterator = new MemqLogMessageIterator<>(
//        "test", dataInputStream, currNotificationObj, new ByteArrayDeserializer(),
//        new ByteArrayDeserializer(), new MetricRegistry(), false, null);
//    while (memqLogMessageIterator.hasNext()) {
//      memqLogMessageIterator.next();
//    }
  }

}

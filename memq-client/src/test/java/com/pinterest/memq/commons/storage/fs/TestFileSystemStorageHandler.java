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
package com.pinterest.memq.commons.storage.fs;

import static org.junit.Assert.assertEquals;

import java.io.DataInputStream;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import io.netty.util.ResourceLeakDetector;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.ImmutableList;
import com.google.gson.JsonObject;
import com.pinterest.memq.client.commons.Compression;
import com.pinterest.memq.client.commons.MemqLogMessageIterator;
import com.pinterest.memq.client.commons.TestUtils;
import com.pinterest.memq.client.commons.serde.ByteArrayDeserializer;
import com.pinterest.memq.commons.MemqLogMessage;
import com.pinterest.memq.commons.protocol.BatchData;
import com.pinterest.memq.commons.storage.StorageHandler;
import com.pinterest.memq.core.commons.Message;
import com.pinterest.memq.core.utils.MiscUtils;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.PooledByteBufAllocator;

public class TestFileSystemStorageHandler {

  @BeforeClass
  public static void setup() {
    ResourceLeakDetector.setLevel(ResourceLeakDetector.Level.PARANOID);
  }


  private MetricRegistry registry = new MetricRegistry();

  @Rule
  public TemporaryFolder folder = new TemporaryFolder();

  @Test
  public void testInit() throws Exception {
    FileSystemStorageHandler handler = new FileSystemStorageHandler();
    Properties outputHandlerConfig = new Properties();
    outputHandlerConfig.setProperty(FileSystemStorageHandler.STORAGE_DIRS,
        folder.newFolder().getAbsolutePath());
    String topic = "testTopic";
    handler.initWriter(outputHandlerConfig, topic, registry);
    handler.closeWriter();
  }

  @Test
  public void testSpeculativeWrite() throws Exception {
    StorageHandler handler = new FileSystemStorageHandler();
    Properties outputHandlerConfig = new Properties();
    outputHandlerConfig.setProperty(FileSystemStorageHandler.STORAGE_DIRS,
        folder.newFolder().getAbsolutePath());
    String topic = "testTopic";
    handler.initWriter(outputHandlerConfig, topic, registry);

    List<Message> messageList = generateSampleMessages();
    int sizeInBytes = batchSizeInBytes(messageList);
    handler.writeOutput(sizeInBytes, 0, messageList);
    handler.closeWriter();
  }

  @Test
  public void testfetchBatchStreamForNotificationBuf() throws Exception {
    StorageHandler handler = new FileSystemStorageHandler();
    Properties outputHandlerConfig = new Properties();
    String absolutePath = folder.newFolder().getAbsolutePath();
    outputHandlerConfig.setProperty(FileSystemStorageHandler.STORAGE_DIRS, absolutePath);
    String topic = "testTopic1";
    handler.initWriter(outputHandlerConfig, topic, registry);

    byte[] msg = TestUtils.createMessage("hello world", (base, k) -> base.getBytes(), 100, true,
        Compression.GZIP, null, false);
    ByteBuf buffer = PooledByteBufAllocator.DEFAULT.buffer();
    buffer.writeBytes(msg);
    List<Message> messages = ImmutableList.of(Message.newInstance(buffer, 1, 1, null, (short) 3));

    handler.writeOutput(msg.length, 0, messages);
    Properties properties = new Properties();
    handler.initReader(properties, registry);
    JsonObject notification = new JsonObject();
    notification.addProperty(FileSystemStorageHandler.TOPIC, topic);
    String path = absolutePath + "/" + topic + "/" + MiscUtils.getHostname()  + "/1_1_0";
    notification.addProperty(FileSystemStorageHandler.PATH, path);
    BatchData buf = handler.fetchBatchStreamForNotificationBuf(notification);
    DataInputStream stream = new DataInputStream(new ByteBufInputStream(buf.getDataAsBuf(), true));
    JsonObject obj = new JsonObject();
    obj.addProperty(MemqLogMessage.INTERNAL_FIELD_TOPIC, topic);
    obj.addProperty(MemqLogMessage.INTERNAL_FIELD_OBJECT_SIZE, new File(path).length());
    obj.addProperty(MemqLogMessage.INTERNAL_FIELD_NOTIFICATION_PARTITION_ID, 1);
    obj.addProperty(MemqLogMessage.INTERNAL_FIELD_NOTIFICATION_PARTITION_OFFSET, 1);
    obj.addProperty(MemqLogMessage.INTERNAL_FIELD_NOTIFICATION_READ_TIMESTAMP,
        System.currentTimeMillis());
    MemqLogMessageIterator<byte[], byte[]> iterator = new MemqLogMessageIterator<>("test", "test",
        stream, obj, new ByteArrayDeserializer(), new ByteArrayDeserializer(), registry, false,
        null);
    int c = 0;
    while (iterator.hasNext()) {
      iterator.next();
      c++;
    }
    assertEquals(100, c);
    iterator.close();
  }

  public static int batchSizeInBytes(List<Message> batch) {
    return batch.stream().mapToInt(b -> b.getBuf().writerIndex()).sum();
  }

  private List<Message> generateSampleMessages() {
    List<Message> messages = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      Message m = new Message(1024 * 1024, true);
      m.put(new byte[1024 * 100]);
      messages.add(m);
    }
    return messages;
  }

}

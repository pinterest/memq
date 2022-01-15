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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.AbstractMap;
import java.util.Arrays;
import java.util.List;
import java.util.Map.Entry;
import java.util.UUID;

import org.apache.commons.compress.utils.IOUtils;
import org.junit.Test;

import com.pinterest.memq.core.commons.Message;
import com.pinterest.memq.core.commons.MessageBufferInputStream;

import io.netty.buffer.ByteBufInputStream;

public class TestStorageHandler {

  @Test
  public void testBatchHeadersAsInputStream() throws IOException {
    String uuid = UUID.randomUUID().toString();
    Message m1 = new Message(1024, false);
    m1.put(uuid.getBytes("utf-8"));
    Message m2 = new Message(512, false);
    m2.put("abcdefgh".getBytes("utf-8"));
    Message m3 = new Message(128, false);
    m3.put("123456789".getBytes("utf-8"));
    List<Message> messages = Arrays.asList(m1, m2, m3);
    InputStream stream = new ByteBufInputStream(StorageHandler.getBatchHeadersAsByteArray(messages));
    @SuppressWarnings("unused")
    byte b;
    int byteCount = 0;
    while ((b = (byte) stream.read()) != -1) {
      byteCount++;
    }
    assertEquals(4 + 4 + 12 * 3, byteCount);
  }

  @Test
  public void testBatchWritesWithStreamConcat() throws Exception {
    String uuid = UUID.randomUUID().toString();
    Message m1 = new Message(1024, false);
    m1.put(uuid.getBytes("utf-8"));
    Message m2 = new Message(512, false);
    m2.put("abcdefgh".getBytes("utf-8"));
    Message m3 = new Message(128, false);
    m3.put("123456789".getBytes("utf-8"));
    List<Message> messages = Arrays.asList(m1, m2, m3);
    InputStream stream = new ByteBufInputStream(StorageHandler.getBatchHeadersAsByteArray(messages));

    ByteArrayOutputStream os = new ByteArrayOutputStream();
    long length = IOUtils.copy(stream, os);
    assertEquals(4 + 4 + 12 * 3, length);
    IOUtils.copy(new MessageBufferInputStream(messages, null), os);
    os.close();

    byte[] output = os.toByteArray();
    assertEquals((4 + 4 + 12 * 3) + (uuid.length() + 8 + 9), output.length);

    Entry<Integer, Integer> entryForSecondMessage = getIndexEntriesFor(2, output);
    ByteBuffer buf = ByteBuffer.wrap(output);
    buf.position(entryForSecondMessage.getKey());
    byte[] ary = new byte[9];
    buf.get(ary);
    assertArrayEquals("123456789".getBytes("utf-8"), ary);
  }

  private Entry<Integer, Integer> getIndexEntriesFor(int n, byte[] output) throws IOException {
    // try to reverse engineer the offsets from header
    DataInputStream dis = new DataInputStream(new ByteArrayInputStream(output));
    dis.readInt();// length
    dis.readInt();// count
    for (int idx = 0; idx <= n; idx++) {
      // first message
      dis.readInt();
      int offset = dis.readInt();
      int size = dis.readInt();
      if (idx == n) {
        return new AbstractMap.SimpleEntry<>(offset, size);
      }
    }
    return null;
  }

}

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
package com.pinterest.memq.client.producer;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.InvocationTargetException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.BiFunction;
import java.util.zip.CRC32;

import org.junit.Test;

import com.pinterest.memq.client.commons.Compression;
import com.pinterest.memq.client.commons.MemqMessageHeader;
import com.pinterest.memq.client.commons.TestUtils;
import com.pinterest.memq.client.producer.netty.MemqNettyRequest;
import com.pinterest.memq.commons.MemqLogMessage;
import com.pinterest.memq.commons.MessageId;

public class TestMemqProducer {

  @Test
  public void testProducerMessageEncoding() throws IOException, InstantiationException,
                                            IllegalAccessException, IllegalArgumentException,
                                            InvocationTargetException, NoSuchMethodException,
                                            SecurityException {
    Semaphore maxRequestLock = new Semaphore(1);
    MemqNettyRequest task = new MemqNettyRequest("xyz", 1L, Compression.GZIP,
        maxRequestLock, true, 1024 * 1024, 100, null, null, 10_000, false);
    int count = 100;
    OutputStream os = task.getOutputStream();
    String data = "xyzabcs";
    String ary = data;
    for (int i = 0; i < 2; i++) {
      ary += ary;
    }
    for (int k = 0; k < count; k++) {
      byte[] bytes = (ary + "i:" + k).getBytes();
      MemqProducer.writeMemqLogMessage(null, null, null, bytes, task, System.currentTimeMillis());
    }
    task.markReady();
    os.close();
    byte[] buf = task.getPayloadAsByteArrays();
    ByteBuffer wrap = ByteBuffer.wrap(buf);

    // task.getHeader().getHeaderLength(), buf.length);
    System.out.println("Header length:" + MemqMessageHeader.getHeaderLength());
    assertEquals("Header length didn't match in payload", MemqMessageHeader.getHeaderLength(),
        wrap.getShort());
    assertEquals(task.getVersion(), wrap.getShort());
    short extraHeaderLength = wrap.getShort();
    assertEquals(21, extraHeaderLength);
    // skip testing extra content
    wrap.position(wrap.position() + extraHeaderLength);

    int crc = wrap.getInt();
    byte compression = wrap.get();
    assertEquals(1, compression);
    int messageCount = wrap.getInt();
    assertEquals(count, messageCount);
    int lengthOfPayload = wrap.getInt();
    ByteBuffer payload = wrap.slice();
    CRC32 crcCalc = new CRC32();
    crcCalc.update(payload);
    payload.rewind();

    byte[] outputPayload = new byte[lengthOfPayload];
    for (int i = 0; i < lengthOfPayload; i++) {
      outputPayload[i] = payload.get();
    }

    InputStream stream = new ByteArrayInputStream(outputPayload);
    for (Compression comp : Compression.values()) {
      if (comp.id == compression) {
        stream = comp.getCompressStream(stream);
        break;
      }
    }

    DataInputStream dis = new DataInputStream(stream);
    for (int k = 0; k < count; k++) {
      short internalFieldsLength = dis.readShort();
      assertEquals(11, internalFieldsLength);
      dis.read(new byte[internalFieldsLength]);
      int headerLength = dis.readInt();
      assertEquals(0, headerLength);
      int length = dis.readInt();
      byte[] b = new byte[length];
      dis.readFully(b);
      assertEquals(crc, (int) crcCalc.getValue());
      assertArrayEquals((ary + "i:" + k).getBytes(), b);
    }
  }

  @Test
  public void testProduceConsumeProtocolCompatibility() throws Exception {
    // write data using producer
    int count = 200000;
    for (Compression c : Compression.values()) {
      String data = "xyza33245245234534bcs";
      String ary = data;
      for (int i = 0; i < 2; i++) {
        ary += ary;
      }
      BiFunction<String, Integer, byte[]> getLogMessageBytes = (base, k) -> (base + k).getBytes();
      String baseLogMessage = ary + "i:";
      long ts = System.currentTimeMillis();
      Iterator<MemqLogMessage<byte[], byte[]>> iterator = TestUtils
          .getTestDataIteratorWithAllFields(baseLogMessage, getLogMessageBytes, count, 1, c, false);
      for (int i = 0; i < 5001; i++) {
        assertTrue(iterator.hasNext()); // assert idempotence
      }
      long ts1 = System.currentTimeMillis();
      int z = 0;
      while (iterator.hasNext()) {
        MemqLogMessage<byte[], byte[]> next = iterator.next();
        assertNull(next.getKey());
        assertEquals(z, ByteBuffer.wrap(next.getMessageId().toByteArray()).getLong());
        assertNull(next.getHeaders());
        assertTrue(ts <= (Long) next.getWriteTimestamp());
        assertEquals("z:" + z, ary + "i:" + z, new String(next.getValue())); // assert value equals
        z++;
      }
      ts1 = System.currentTimeMillis() - ts1;
      System.out.println("Time to process:" + ts1 + "ms for:" + z);
      assertEquals(count, z);
      assertFalse(iterator.hasNext());
    }
  }

  @Test
  public void testMessageIdHashPerf() throws IOException {
    Semaphore maxRequestLock = new Semaphore(1);
    MemqNettyRequest req = new MemqNettyRequest("xyz", 1L, Compression.GZIP,
        maxRequestLock, true, 1024 * 1024, 100, null, null, 10_000, false);
    long nextLong = ThreadLocalRandom.current().nextLong();
    // Takes 1.68s for 100M 16byte messageIds
    for (int i = 0; i < 100_000_000; i++) {
      req.addMessageId(new MessageId(ByteBuffer.allocate(16).putLong(nextLong).putLong(i).array()));
    }

    req.getBuffer().release();
  }

}

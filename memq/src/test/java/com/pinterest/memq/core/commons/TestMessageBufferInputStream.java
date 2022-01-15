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
package com.pinterest.memq.core.commons;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import org.junit.Before;
import org.junit.Test;

import com.codahale.metrics.Counter;
import com.pinterest.memq.core.commons.Message;
import com.pinterest.memq.core.commons.MessageBufferInputStream;
import com.pinterest.memq.core.utils.CoreUtils;

@SuppressWarnings("unused")
public class TestMessageBufferInputStream {

  private List<Message> list;
  private AtomicInteger slotCounter;

  @Before
  public void before() {
    slotCounter = new AtomicInteger();
    String uuid = UUID.randomUUID().toString();
    try {
      Message m1 = new Message(1024, false);
      m1.put(uuid.getBytes("utf-8"));
      Message m2 = new Message(512, false);
      m2.put("abcdefgh".getBytes("utf-8"));
      Message m3 = new Message(128, false);
      m3.put("123456789".getBytes("utf-8"));
      list = Arrays.asList(m1, m2, m3);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  public void testBasicReads() throws IOException {
    MessageBufferInputStream is = new MessageBufferInputStream(list, new Counter());

    int count = 0;
    byte b;
    while ((b = (byte) is.read()) != -1) {
      count++;
    }
    assertEquals(53, count);
    is.close();
  }

  @Test
  public void testMultiRead() throws Exception {
    int size = CoreUtils.batchSizeInBytes(list);
    for (int i = 0; i < 10; i++) {
      MessageBufferInputStream is = new MessageBufferInputStream(list, new Counter());
      int c = 0;
      byte b;
      while ((b = (byte) is.read()) != -1) {
        c++;
      }
      assertEquals("Failed for:" + i, size, c);
      is.close();
    }

    size = CoreUtils.batchSizeInBytes(list);
    MessageBufferInputStream is = new MessageBufferInputStream(list, new Counter());
    for (int i = 0; i < 10; i++) {
      is.resetToBeginnging();
      int c = 0;
      byte b;
      while ((b = (byte) is.read()) != -1) {
        c++;
      }
      assertEquals("Failed for:" + i, size, c);
      is.close();
    }
  }

//  @Test
  public void testMarkSupport() throws IOException {
    MessageBufferInputStream is = new MessageBufferInputStream(list, new Counter());
    assertEquals(true, is.markSupported());

    assertEquals(53, CoreUtils.batchSizeInBytes(list));

    int count = 0;
    for (int i = 0; i < 10; i++) {
      is.read();
      count++;
    }
    is.mark(1024 * 1024);
    for (int i = 0; i < 10; i++) {
      is.read();
      count++;
    }
    assertEquals(20, count);

    is.reset();
    byte b;
    while ((b = (byte) is.read()) != -1) {
      count++;
    }
    assertEquals(63, count);

    is.reset();
    count = 0;
    while ((b = (byte) is.read()) != -1) {
      count++;
    }
    assertEquals(43, count);

    is.reset();
    count = 0;
    while ((b = (byte) is.read()) != -1) {
      count++;
    }
    assertEquals(43, count);

    is.close();
  }

  public void testMarkCombinations() throws IOException {
    Message m1 = new Message(1024, false);
    m1.put("asdaaq3erqddasdfqw3rqGdsasdfsadfgasdvadvasFDFDFsdsefasfdfsafSADfasdfasdfasdf"
        .getBytes("utf-8"));
    Message m2 = new Message(512, false);
    m2.put("asdferq3rfdsasdvavasdgwr34t45#@$@$!#@%#$%!@#$~@$!##E@#!$3".getBytes("utf-8"));
    Message m3 = new Message(128, false);
    m3.put("123456789".getBytes("utf-8"));
    list = Arrays.asList(m1, m2, m3);
    MessageBufferInputStream is = new MessageBufferInputStream(list, new Counter());
    assertEquals(142, CoreUtils.batchSizeInBytes(list));

    int count = 0;
    is.mark(0);
    byte b;
    while ((b = (byte) is.read()) != -1) {
      count++;
    }
    assertEquals(142, is.getBytesRead());
    assertEquals(142, count);

    is.reset();
    count = 0;
    while ((b = (byte) is.read()) != -1) {
      count++;
    }
    assertEquals(142, count);
    is.close();
  }

  public void testHeavyLoad() throws IOException {
    List<Message> list = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      Message m = new Message(1024 * 1024, true);
      m.put(new byte[1024 * 100]);
      list.add(m);
    }
    int BYTES = 1024 * 100 * 10;
    assertEquals(BYTES, CoreUtils.batchSizeInBytes(list));

    Random rand = new Random();
    for (int i = 0; i < 100; i++) {
      int nextInt = rand.nextInt(BYTES);
      rewindMessages(list);
      MessageBufferInputStream mis = new MessageBufferInputStream(list, new Counter());
      mis.mark(1024 * 1024 * 100);
      byte b;
      int k = 0;
      while ((b = (byte) mis.read()) != -1) {
        k++;
        if (k == nextInt) {
          mis.reset();
        }
      }
      assertEquals(nextInt + BYTES, mis.getBytesRead());
      mis.close();
    }
  }

  public void testEventResetRead() throws IOException {
    int CNT = 100;
    List<Message> list = new ArrayList<>();
    for (int i = 0; i < CNT; i++) {
      Message m = new Message(1024 * 1024, true);
      list.add(m);
    }

    for (int p = 0; p < 10; p++) {
      for (int k = 0; k < CNT; k++) {
        Message m = list.get(k);
        m.reset();
        String data = UUID.randomUUID().toString();
        for (int j = 0; j < ThreadLocalRandom.current().nextInt(1000); j++) {
          data += UUID.randomUUID().toString();
        }
        m.put(data.getBytes());
      }
      int batchSizeInBytes = CoreUtils.batchSizeInBytes(list);
      MessageBufferInputStream mis = new MessageBufferInputStream(list, new Counter());
      mis.mark(10);
      byte b;
      int k = 0;
      while ((b = (byte) mis.read()) != -1) {
        k++;
      }
      System.out.println("Size in bytes:" + batchSizeInBytes);
      assertEquals(batchSizeInBytes, k);
      mis.close();
    }
  }

  @Test
  public void testReRead() throws IOException {
    List<Message> list = new ArrayList<>();
    int BYTES = 0;
    for (int i = 0; i < 10; i++) {
      Message m = new Message(1024 * 1024, true);
      int byteCount = 1024 * ThreadLocalRandom.current().nextInt(10);
      m.put(new byte[byteCount]);
      BYTES += byteCount;
      list.add(m);
    }
    assertEquals(BYTES, CoreUtils.batchSizeInBytes(list));

    int k = 0;
    for (int i = 0; i < 100; i++) {
      MessageBufferInputStream mis = new MessageBufferInputStream(list, new Counter());
      byte b;
      while ((b = (byte) mis.read()) != -1) {
        k++;
      }
      mis.close();
    }

    assertEquals(BYTES * 100, k);
  }

  @Test
  public void testMarkBatchReads() throws IOException {
    List<Message> list = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      Message m = new Message(1024 * 1024, true);
      m.put(new byte[1024 * 100]);
      list.add(m);
    }
    int BYTES = 1024 * 100 * 10;
    assertEquals(BYTES, CoreUtils.batchSizeInBytes(list));

    MessageBufferInputStream mis = new MessageBufferInputStream(list, new Counter());
    byte[] b = new byte[127];
    int k = 0;

    int t = 0;
    while ((t = mis.read(b)) != -1) {
      k += t;
    }

    assertEquals(BYTES, k);
    mis.close();
  }

  @Test
  public void testMd5SumCalculationCheck() throws IOException {
    int BYTES = 0;
    List<Message> list = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      Message m = new Message(1024 * 1024, true);
      byte[] buf = new byte[1024 * ThreadLocalRandom.current().nextInt(10, 100)];
      BYTES += buf.length;
      m.put(buf);
      list.add(m);
    }
    assertEquals(BYTES, CoreUtils.batchSizeInBytes(list));
    MessageBufferInputStream mis = new MessageBufferInputStream(list, new Counter());
//    MD5DigestCalculatingInputStream stream = new MD5DigestCalculatingInputStream(
//        new LengthCheckInputStream(mis, BYTES, true));
    byte[] b = new byte[127];
    int k = 0;

    int t = 0;
    while ((t = mis.read(b)) != -1) {
      k += t;
    }
    assertEquals(BYTES, k);
//    stream.close();

    for (int p = 0; p < 100; p++) {
      BYTES = 0;
      for (int i = 0; i < 10; i++) {
        Message m = list.get(i);
        m.reset();
        byte[] buf = new byte[1024 * ThreadLocalRandom.current().nextInt(10, 100)];
        BYTES += buf.length;
        m.put(buf);
      }

      assertEquals(BYTES, CoreUtils.batchSizeInBytes(list));
      mis = new MessageBufferInputStream(list, new Counter());
//      stream = new MD5DigestCalculatingInputStream(new LengthCheckInputStream(mis, BYTES, true));
      b = new byte[127];
      k = 0;

      t = 0;
      while ((t = mis.read(b)) != -1) {
        k += t;
      }
      assertEquals(BYTES, k);
//      stream.close();
    }
    mis.close();
  }

  public void rewindMessages(List<Message> list) {
    for (Message m : list) {
      m.getBuf().resetReaderIndex();
      m.getBuf().resetWriterIndex();
    }
  }
}
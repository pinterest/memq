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
package com.pinterest.memq.client.producer.netty;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.net.InetSocketAddress;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeoutException;
import java.util.zip.CRC32;

import org.junit.Before;
import org.junit.Test;

import com.codahale.metrics.MetricRegistry;
import com.google.gson.JsonObject;
import com.pinterest.memq.client.commons.Compression;
import com.pinterest.memq.client.commons.MemqLogMessageIterator;
import com.pinterest.memq.client.commons.MemqMessageHeader;
import com.pinterest.memq.client.commons.serde.ByteArrayDeserializer;
import com.pinterest.memq.client.commons.serde.ByteArraySerializer;
import com.pinterest.memq.client.producer.MemqWriteResult;
import com.pinterest.memq.client.producer.TaskRequest;
import com.pinterest.memq.commons.MemqLogMessage;
import com.pinterest.memq.commons.protocol.RequestType;

import io.netty.buffer.ByteBuf;
import reactor.netty.DisposableServer;
import reactor.netty.tcp.TcpServer;

public class TestMemqNettyProducer {
  private static final String LOCALHOST_STRING = "127.0.0.1";
  private int port = -1;

  private InetSocketAddress commonAddress;

  @Before
  public void generateRandomPort() {
    int newPort = -1;
    while (port == newPort) {
      newPort = ThreadLocalRandom.current().nextInt(20000, 30000);
    }
    port = newPort;
    commonAddress = InetSocketAddress.createUnresolved(LOCALHOST_STRING, port);
  }

  @Test
  public void testSimpleWrite() throws Exception {
    DisposableServer mockServer = TcpServer.create()
        .bindAddress(() -> commonAddress)
        .handle(((in, out) -> {
          in.receive().subscribe();
          return out.neverComplete();
        }))
        .bindNow();
    MemqNettyProducer<byte[], byte[]> producer = new MemqNettyProducer<>("testcluster", commonAddress,
        "testTopic", 2, 1024 * 1024, Compression.NONE, false, 100, 30000, "local", 1000, null,
        null);
    assertNotNull(producer);
    assertTrue(!producer.getEs().isShutdown());
    producer.setKeySerializer(new ByteArraySerializer());
    producer.setValueSerializer(new ByteArraySerializer());
    Set<Future<MemqWriteResult>> futures = new HashSet<>();
    for (int i = 0; i < 100; i++) {
      Future<MemqWriteResult> future = producer.writeToTopic(null,
          UUID.randomUUID().toString().getBytes());
      futures.add(future);
    }
    assertEquals("There should be 1 request in the request map", 1,
        producer.getRequestMap().size());
    assertEquals("Since there is 1 pending request there should only be 1 future", 1,
        futures.size());
    TaskRequest request = producer.getRequestMap().entrySet().iterator().next().getValue();
    assertTrue(request instanceof MemqNettyRequest);
    MemqNettyRequest nettyRequest = (MemqNettyRequest) request;
    producer.finalizeRequest();
    assertTrue("Request should be ready", nettyRequest.isReady());
    Thread.sleep(100);
    try {
      futures.iterator().next().get();
      fail("Must throw ack timeout");
    } catch (ExecutionException ee) {
      System.out.println(ee.getCause());
      assertTrue(ee.getCause() instanceof TimeoutException);
    } catch (Exception e) {
      fail();
    }
    assertEquals(0, producer.getRequestMap().size());
    producer.close();
    mockServer.dispose();
  }

  @Test
  public void testWriteProtocolCompatibility() throws Exception {
    CompletableFuture<ByteBuf> bufFuture = new CompletableFuture<>();
    DisposableServer mockServer = TcpServer.create()
        .bindAddress(() -> commonAddress)
        .handle((in, out) -> {
          in.receive().aggregate().retain().subscribe(bufFuture::complete);
          return out.neverComplete();
        })
        .bindNow();
    String topicName = "testTopic";
    MemqNettyProducer<byte[], byte[]> producer = new MemqNettyProducer<>("testcluster", commonAddress,
        topicName, 2, 1024 * 1024, Compression.NONE, true, 100, 30000, "local", 10000, null, null);
    producer.setKeySerializer(new ByteArraySerializer());
    producer.setValueSerializer(new ByteArraySerializer());
    Set<Future<MemqWriteResult>> futures = new HashSet<>();
    int totalBytesWritten = 0;
    for (int i = 0; i < 50; i++) {
      byte[] bytes = UUID.randomUUID().toString().getBytes();
      totalBytesWritten += bytes.length + 13 + 4 + 4;
      Future<MemqWriteResult> future = producer.writeToTopic(null, bytes);
      futures.add(future);
    }
    Future<MemqWriteResult> response = producer.writeToTopic(null, new byte[1024 * 1024]);
    assertNull(response);
    TaskRequest request = producer.getRequestMap().entrySet().iterator().next().getValue();
    assertTrue(request instanceof MemqNettyRequest);
    MemqNettyRequest nettyRequest = (MemqNettyRequest) request;
    nettyRequest.setDebugEnabled();
    producer.finalizeRequest();
    assertTrue("Request should be ready", nettyRequest.isReady());
    Thread.sleep(300);
    producer.close();

    // verify data written
    ByteBuf buf = bufFuture.get();
    assertNotNull(buf);

    // attempt to read data
    buf.readInt();
    assertEquals(RequestType.PROTOCOL_VERSION, buf.readShort());
    assertEquals(request.getId(), buf.readLong());
    assertEquals(0, buf.readByte());
    assertEquals(true, buf.readBoolean());
    assertEquals(topicName.length(), buf.readShort());
    byte[] tpn = new byte[topicName.length()];
    buf.readBytes(tpn);
    assertEquals(topicName, new String(tpn));
    int checksum = buf.readInt();
    int payloadLength = buf.readInt();
    assertEquals(totalBytesWritten + MemqMessageHeader.getHeaderLength(), payloadLength);

    byte[] payload = new byte[payloadLength];
    buf.readBytes(payload);
    buf.release();

    CRC32 crc = new CRC32();
    crc.update(payload);
    assertEquals(checksum, (int) crc.getValue());

    JsonObject obj = new JsonObject();
    obj.addProperty(MemqLogMessage.INTERNAL_FIELD_TOPIC, "test");
    obj.addProperty(MemqLogMessage.INTERNAL_FIELD_OBJECT_SIZE, payload.length);
    obj.addProperty(MemqLogMessage.INTERNAL_FIELD_NOTIFICATION_PARTITION_ID, 1);
    obj.addProperty(MemqLogMessage.INTERNAL_FIELD_NOTIFICATION_PARTITION_OFFSET, 1);
    obj.addProperty(MemqLogMessage.INTERNAL_FIELD_NOTIFICATION_READ_TIMESTAMP,
        System.currentTimeMillis());
    MemqLogMessageIterator<byte[], byte[]> itr = new MemqLogMessageIterator<>("test", "test",
        new DataInputStream(new ByteArrayInputStream(payload)), obj, new ByteArrayDeserializer(),
        new ByteArrayDeserializer(), new MetricRegistry(), true, null);
    int c = 0;
    while (itr.hasNext()) {
      itr.next();
      c++;
    }
    assertEquals(50, c);
    mockServer.dispose();
  }

  @Test
  public void testMemoryUsage() throws Exception {
    DisposableServer mockServer = TcpServer.create()
        .bindAddress(() -> commonAddress)
        .handle((in, out) -> {
          in.receive().subscribe();
          return out.neverComplete();
        })
        .bindNow();
    String topicName = "testTopic";
    MemqNettyProducer<byte[], byte[]> producer = new MemqNettyProducer<>("testcluster", commonAddress,
        topicName, 10, 1024 * 1024 * 4, Compression.NONE, true, 100, 30000, "local", 1000, null,
        null);
    producer.setDebug();
    producer.setKeySerializer(new ByteArraySerializer());
    producer.setValueSerializer(new ByteArraySerializer());
    int batchBytes = 0;
    long ts = System.currentTimeMillis();
    byte[] bytes = UUID.randomUUID().toString().getBytes();
    for (int i = 0; i < 10_000_000; i++) {
      producer.writeToTopic(null, bytes);
      batchBytes += bytes.length;
      if ((System.currentTimeMillis() - ts) >= 1000) {
        System.out.println(batchBytes / 1024 / 1024 + "MB/s\t" + i);
        batchBytes = 0;
        ts = System.currentTimeMillis();
      }
    }
    producer.close();
    mockServer.dispose();
  }
}

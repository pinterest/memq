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
package com.pinterest.memq.commons.protocol;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Collections;

import org.junit.Test;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.Unpooled;

public class TestWriteRequestPacketV4 {

  @Test
  public void testV4RoundTrip() {
    ByteBuf payload = Unpooled.wrappedBuffer("hello world".getBytes());
    WriteRequestPacket original = new WriteRequestPacket(false, "testTopic".getBytes(),
        true, 12345, payload.duplicate());
    original.setProducerId("abc-123-uuid");
    original.setCurrentConnections(Arrays.asList("10.0.0.1", "10.0.0.2", "10.0.0.3"));

    ByteBuf buf = PooledByteBufAllocator.DEFAULT.buffer();
    original.write(buf, (short) 4);

    WriteRequestPacket decoded = new WriteRequestPacket();
    decoded.readFields(buf, (short) 4);

    assertEquals(false, decoded.isDisableAcks());
    assertEquals("testTopic", decoded.getTopicName());
    assertEquals(12345, decoded.getChecksum());
    assertTrue(decoded.isChecksumExists());

    assertEquals("abc-123-uuid", decoded.getProducerId());

    assertNotNull(decoded.getCurrentConnections());
    assertEquals(3, decoded.getCurrentConnections().size());
    assertEquals("10.0.0.1", decoded.getCurrentConnections().get(0));
    assertEquals("10.0.0.2", decoded.getCurrentConnections().get(1));
    assertEquals("10.0.0.3", decoded.getCurrentConnections().get(2));

    assertEquals(payload.readableBytes(), decoded.getDataLength());

    payload.release();
    buf.release();
  }

  @Test
  public void testV4EmptyConnections() {
    ByteBuf payload = Unpooled.wrappedBuffer("data".getBytes());
    WriteRequestPacket original = new WriteRequestPacket(true, "topic2".getBytes(),
        true, 99, payload.duplicate());
    original.setProducerId("producer-xyz");
    original.setCurrentConnections(Collections.emptyList());

    ByteBuf buf = PooledByteBufAllocator.DEFAULT.buffer();
    original.write(buf, (short) 4);

    WriteRequestPacket decoded = new WriteRequestPacket();
    decoded.readFields(buf, (short) 4);

    assertTrue(decoded.isDisableAcks());
    assertEquals("topic2", decoded.getTopicName());
    assertEquals("producer-xyz", decoded.getProducerId());
    assertNotNull(decoded.getCurrentConnections());
    assertEquals(0, decoded.getCurrentConnections().size());

    payload.release();
    buf.release();
  }

  @Test
  public void testV4NullProducerIdAndConnections() {
    ByteBuf payload = Unpooled.wrappedBuffer("data".getBytes());
    WriteRequestPacket original = new WriteRequestPacket(false, "topic3".getBytes(),
        true, 42, payload.duplicate());
    // producerId and currentConnections left null

    ByteBuf buf = PooledByteBufAllocator.DEFAULT.buffer();
    original.write(buf, (short) 4);

    WriteRequestPacket decoded = new WriteRequestPacket();
    decoded.readFields(buf, (short) 4);

    assertEquals("topic3", decoded.getTopicName());
    assertEquals("", decoded.getProducerId());
    assertNotNull(decoded.getCurrentConnections());
    assertEquals(0, decoded.getCurrentConnections().size());

    payload.release();
    buf.release();
  }

  @Test
  public void testV3RequestOnV4Broker() {
    ByteBuf payload = Unpooled.wrappedBuffer("v3payload".getBytes());
    WriteRequestPacket original = new WriteRequestPacket(false, "topicV3".getBytes(),
        true, 555, payload.duplicate());

    ByteBuf buf = PooledByteBufAllocator.DEFAULT.buffer();
    original.write(buf, (short) 3);

    WriteRequestPacket decoded = new WriteRequestPacket();
    // v4 broker receives v3 request (protocolVersion=3 from envelope)
    decoded.readFields(buf, (short) 3);

    assertEquals("topicV3", decoded.getTopicName());
    assertEquals(555, decoded.getChecksum());
    assertNull("v3 packets must not carry producerId", decoded.getProducerId());
    assertNull("v3 packets must not carry currentConnections", decoded.getCurrentConnections());
    assertEquals(payload.readableBytes(), decoded.getDataLength());

    payload.release();
    buf.release();
  }

  @Test
  public void testV4SizeCalculation() {
    ByteBuf payload = Unpooled.wrappedBuffer("test".getBytes());
    WriteRequestPacket pkt = new WriteRequestPacket(false, "t".getBytes(),
        true, 0, payload.duplicate());
    pkt.setProducerId("pid-1");
    pkt.setCurrentConnections(Arrays.asList("1.2.3.4"));

    int expectedSize = pkt.getSize((short) 4);

    ByteBuf buf = PooledByteBufAllocator.DEFAULT.buffer();
    pkt.write(buf, (short) 4);

    assertEquals(expectedSize, buf.readableBytes());

    payload.release();
    buf.release();
  }
}

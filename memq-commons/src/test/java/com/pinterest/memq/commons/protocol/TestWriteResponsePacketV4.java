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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;

public class TestWriteResponsePacketV4 {

  @Test
  public void testV4RoundTripWithEviction() {
    WriteResponsePacket original = new WriteResponsePacket("10.0.0.5", 9092, 2, 5);
    assertTrue(original.hasEviction());

    ByteBuf buf = PooledByteBufAllocator.DEFAULT.buffer();
    original.write(buf, (short) 4);

    WriteResponsePacket decoded = new WriteResponsePacket();
    decoded.readFields(buf, (short) 4);

    assertEquals("10.0.0.5", decoded.getTargetBrokerIp());
    assertEquals(9092, decoded.getTargetBrokerPort());
    assertEquals(2, decoded.getNumSlotsToEvict());
    assertEquals(5, decoded.getNumSlotsOwned());
    assertTrue(decoded.hasEviction());

    buf.release();
  }

  @Test
  public void testV4RoundTripNoEviction() {
    WriteResponsePacket original = new WriteResponsePacket(null, 0, 0, 10);
    assertFalse(original.hasEviction());

    ByteBuf buf = PooledByteBufAllocator.DEFAULT.buffer();
    original.write(buf, (short) 4);

    WriteResponsePacket decoded = new WriteResponsePacket();
    decoded.readFields(buf, (short) 4);

    assertNull(decoded.getTargetBrokerIp());
    assertEquals(0, decoded.getTargetBrokerPort());
    assertEquals(0, decoded.getNumSlotsToEvict());
    assertEquals(10, decoded.getNumSlotsOwned());
    assertFalse(decoded.hasEviction());

    buf.release();
  }

  @Test
  public void testV3ResponseEmpty() {
    WriteResponsePacket pkt = new WriteResponsePacket();

    ByteBuf buf = PooledByteBufAllocator.DEFAULT.buffer();
    pkt.write(buf, (short) 3);

    assertEquals(0, buf.readableBytes());
    assertEquals(0, pkt.getSize((short) 3));

    buf.release();
  }

  @Test
  public void testV3ReadFieldsNoOp() {
    WriteResponsePacket original = new WriteResponsePacket("10.0.0.1", 9092, 1, 3);

    ByteBuf buf = PooledByteBufAllocator.DEFAULT.buffer();
    original.write(buf, (short) 4);

    // v3 client reads v4 response - should not read eviction fields
    WriteResponsePacket v3Decoded = new WriteResponsePacket();
    v3Decoded.readFields(buf, (short) 3);

    assertNull(v3Decoded.getTargetBrokerIp());
    assertEquals(0, v3Decoded.getNumSlotsOwned());
    assertFalse(v3Decoded.hasEviction());

    buf.release();
  }

  @Test
  public void testV4SizeCalculation() {
    WriteResponsePacket pkt = new WriteResponsePacket("192.168.1.100", 9092, 1, 7);

    int size = pkt.getSize((short) 4);
    assertTrue(size > 0);

    ByteBuf buf = PooledByteBufAllocator.DEFAULT.buffer();
    pkt.write(buf, (short) 4);

    assertEquals(size, buf.readableBytes());

    buf.release();
  }

  @Test
  public void testV4BrokerWritesV3Response() {
    WriteResponsePacket pkt = new WriteResponsePacket("10.0.0.1", 9092, 1, 3);

    ByteBuf buf = PooledByteBufAllocator.DEFAULT.buffer();
    // broker writes v3 response (for v3 client)
    pkt.write(buf, (short) 3);

    assertEquals(0, buf.readableBytes());

    buf.release();
  }
}

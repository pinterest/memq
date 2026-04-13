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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.zip.CRC32;

import org.junit.Test;

import com.pinterest.memq.client.producer.MemqWriteResult;
import com.pinterest.memq.client.producer.TaskRequest;
import com.pinterest.memq.client.producer2.Request;
import com.pinterest.memq.core.utils.MemqUtils;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.Unpooled;

public class TestMemqMessageHeader {

  /**
   * Verify that writeHeader via TaskRequest writes each field at the correct
   * absolute offset and that the final idx equals getHeaderLength().
   */
  @Test
  public void testWriteHeaderIdxPositionsViaTaskRequest() throws Exception {
    int logMessageCount = 7;
    Compression compression = Compression.GZIP;
    StubTaskRequest tr = new StubTaskRequest("testtopic", 42, 20000);
    tr.setCompressionForTest(compression);
    for (int i = 0; i < logMessageCount; i++) {
      tr.incrementLogMessageCountForTest();
    }

    byte[] payload = "hello world payload".getBytes();
    short headerLength = MemqMessageHeader.getHeaderLength();
    ByteBuf buffer = Unpooled.buffer(headerLength + payload.length);
    buffer.writerIndex(headerLength);
    buffer.writeBytes(payload);

    MemqMessageHeader header = new MemqMessageHeader(tr);
    header.writeHeader(buffer);

    int expectedExtraHeaderLen = MemqUtils.HOST_IPV4_ADDRESS.length + 1 + 8 + 8;

    CRC32 expectedCrc = new CRC32();
    ByteBuf bodySlice = buffer.slice(headerLength, payload.length);
    expectedCrc.update(bodySlice.nioBuffer());
    int expectedChecksum = (int) expectedCrc.getValue();

    int idx = 0;
    assertEquals("headerLength at offset 0", headerLength, buffer.getShort(idx));
    idx += 2;
    assertEquals("version at offset 2", tr.getVersion(), buffer.getShort(idx));
    idx += 2;
    assertEquals("extraHeaderContentLength at offset 4", expectedExtraHeaderLen, buffer.getShort(idx));
    idx += 2;

    byte hostAddrLen = buffer.getByte(idx);
    assertEquals("host address length", MemqUtils.HOST_IPV4_ADDRESS.length, hostAddrLen);
    idx += 1;
    byte[] hostAddr = new byte[hostAddrLen];
    buffer.getBytes(idx, hostAddr);
    assertArrayEquals("host address bytes", MemqUtils.HOST_IPV4_ADDRESS, hostAddr);
    idx += hostAddrLen;
    assertEquals("producer epoch", tr.getEpoch(), buffer.getLong(idx));
    idx += 8;
    assertEquals("producer request id (clientRequestId)", tr.getId(), buffer.getLong(idx));
    idx += 8;

    assertEquals("crc checksum", expectedChecksum, buffer.getInt(idx));
    idx += 4;
    assertEquals("compression id", compression.id, buffer.getByte(idx));
    idx += 1;
    assertEquals("logmessage count", logMessageCount, buffer.getInt(idx));
    idx += 4;
    int expectedPayloadLength = buffer.readableBytes() - headerLength;
    assertEquals("payload length", expectedPayloadLength, buffer.getInt(idx));
    idx += 4;

    assertEquals("final idx must equal headerLength", headerLength, idx);

    buffer.release();
  }

  /**
   * Verify that writeHeader via Request (producer2 path) writes each field
   * at the correct absolute offset and that the final idx equals getHeaderLength().
   */
  @Test
  public void testWriteHeaderIdxPositionsViaRequest() throws Exception {
    int messageCount = 3;
    Compression compression = Compression.ZSTD;
    short expectedVersion = 1_0_0;
    long epoch = 12345L;
    int clientRequestId = 99;

    Request mockRequest = mock(Request.class);
    when(mockRequest.getVersion()).thenReturn(expectedVersion);
    when(mockRequest.getCompression()).thenReturn(compression);
    when(mockRequest.getMessageCount()).thenReturn(messageCount);
    when(mockRequest.getEpoch()).thenReturn(epoch);
    when(mockRequest.getClientRequestId()).thenReturn(clientRequestId);

    byte[] payload = "some payload bytes".getBytes();
    short headerLength = MemqMessageHeader.getHeaderLength();
    ByteBuf buffer = Unpooled.buffer(headerLength + payload.length);
    buffer.writerIndex(headerLength);
    buffer.writeBytes(payload);

    MemqMessageHeader header = new MemqMessageHeader(mockRequest);
    header.writeHeader(buffer);

    int expectedExtraHeaderLen = MemqUtils.HOST_IPV4_ADDRESS.length + 1 + 8 + 8;

    CRC32 expectedCrc = new CRC32();
    ByteBuf bodySlice = buffer.slice(headerLength, payload.length);
    expectedCrc.update(bodySlice.nioBuffer());
    int expectedChecksum = (int) expectedCrc.getValue();

    int idx = 0;
    assertEquals("headerLength at offset 0", headerLength, buffer.getShort(idx));
    idx += 2;
    assertEquals("version at offset 2", expectedVersion, buffer.getShort(idx));
    idx += 2;
    assertEquals("extraHeaderContentLength at offset 4", expectedExtraHeaderLen, buffer.getShort(idx));
    idx += 2;
    idx += expectedExtraHeaderLen;

    assertEquals("crc checksum", expectedChecksum, buffer.getInt(idx));
    idx += 4;
    assertEquals("compression id", compression.id, buffer.getByte(idx));
    idx += 1;
    assertEquals("message count", messageCount, buffer.getInt(idx));
    idx += 4;
    int expectedPayloadLength = buffer.readableBytes() - headerLength;
    assertEquals("payload length", expectedPayloadLength, buffer.getInt(idx));
    idx += 4;

    assertEquals("final idx must equal headerLength", headerLength, idx);

    buffer.release();
  }

  /**
   * Write a header via TaskRequest, then read it back using the ByteBuf
   * constructor and verify all parsed fields match.
   */
  @Test
  public void testWriteHeaderRoundTripViaByteBuf() throws Exception {
    int logMessageCount = 5;
    Compression compression = Compression.NONE;
    long epoch = 9999L;
    long clientRequestId = 77L;

    StubTaskRequest tr = new StubTaskRequest("roundtrip", clientRequestId, 20000);
    tr.setEpochForTest(epoch);
    tr.setCompressionForTest(compression);
    for (int i = 0; i < logMessageCount; i++) {
      tr.incrementLogMessageCountForTest();
    }

    byte[] payload = new byte[256];
    for (int i = 0; i < payload.length; i++) {
      payload[i] = (byte) (i & 0xFF);
    }
    short headerLength = MemqMessageHeader.getHeaderLength();
    ByteBuf buffer = Unpooled.buffer(headerLength + payload.length);
    buffer.writerIndex(headerLength);
    buffer.writeBytes(payload);

    new MemqMessageHeader(tr).writeHeader(buffer);

    ByteBuf readBuf = buffer.duplicate();
    readBuf.readerIndex(0);
    MemqMessageHeader readHeader = new MemqMessageHeader(readBuf);

    assertEquals("version", tr.getVersion(), readHeader.getVersion());
    assertEquals("compression", compression.id, readHeader.getCompression());
    assertEquals("logmessage count", logMessageCount, readHeader.getLogmessageCount());
    assertEquals("message length (payload)", payload.length, readHeader.getMessageLength());
    assertEquals("producer epoch", epoch, readHeader.getProducerEpoch());
    assertEquals("producer request id", clientRequestId, readHeader.getProducerRequestId());
    assertArrayEquals("producer address", MemqUtils.HOST_IPV4_ADDRESS, readHeader.getProducerAddress());

    CRC32 expectedCrc = new CRC32();
    expectedCrc.update(payload);
    assertEquals("crc", (int) expectedCrc.getValue(), readHeader.getCrc());

    buffer.release();
  }

  /**
   * Write a header, then read it back using the ByteBuffer constructor
   * and verify all parsed fields match.
   */
  @Test
  public void testWriteHeaderRoundTripViaByteBuffer() throws Exception {
    int logMessageCount = 10;
    Compression compression = Compression.GZIP;
    long epoch = 42L;
    long clientRequestId = 101L;

    StubTaskRequest tr = new StubTaskRequest("bytebuffer", clientRequestId, 20000);
    tr.setEpochForTest(epoch);
    tr.setCompressionForTest(compression);
    for (int i = 0; i < logMessageCount; i++) {
      tr.incrementLogMessageCountForTest();
    }

    byte[] payload = "round trip via ByteBuffer".getBytes();
    short headerLength = MemqMessageHeader.getHeaderLength();
    ByteBuf buffer = Unpooled.buffer(headerLength + payload.length);
    buffer.writerIndex(headerLength);
    buffer.writeBytes(payload);

    new MemqMessageHeader(tr).writeHeader(buffer);

    byte[] allBytes = new byte[buffer.readableBytes()];
    buffer.getBytes(0, allBytes);
    ByteBuffer bb = ByteBuffer.wrap(allBytes);

    MemqMessageHeader readHeader = new MemqMessageHeader(bb);

    assertEquals("version", tr.getVersion(), readHeader.getVersion());
    assertEquals("compression", compression.id, readHeader.getCompression());
    assertEquals("logmessage count", logMessageCount, readHeader.getLogmessageCount());
    assertEquals("message length", payload.length, readHeader.getMessageLength());
    assertEquals("producer epoch", epoch, readHeader.getProducerEpoch());
    assertEquals("producer request id", clientRequestId, readHeader.getProducerRequestId());

    buffer.release();
  }

  /**
   * writeHeader on a PooledSlicedByteBuf — the scenario the absolute-index
   * set methods were introduced to fix.
   */
  @Test
  public void testWriteHeaderOnSlicedByteBuf() throws Exception {
    int logMessageCount = 2;
    Compression compression = Compression.NONE;

    StubTaskRequest tr = new StubTaskRequest("slicetest", 55, 20000);
    tr.setCompressionForTest(compression);
    for (int i = 0; i < logMessageCount; i++) {
      tr.incrementLogMessageCountForTest();
    }

    byte[] payload = "sliced payload data".getBytes();
    short headerLength = MemqMessageHeader.getHeaderLength();

    int prefixSize = 64;
    ByteBuf parent = PooledByteBufAllocator.DEFAULT.buffer(prefixSize + headerLength + payload.length);
    parent.writeBytes(new byte[prefixSize]);
    parent.writeBytes(new byte[headerLength]);
    parent.writeBytes(payload);

    ByteBuf slice = parent.slice(prefixSize, headerLength + payload.length);

    new MemqMessageHeader(tr).writeHeader(slice);

    ByteBuf readBuf = slice.duplicate();
    readBuf.readerIndex(0);
    MemqMessageHeader readHeader = new MemqMessageHeader(readBuf);

    assertEquals("version", tr.getVersion(), readHeader.getVersion());
    assertEquals("compression", compression.id, readHeader.getCompression());
    assertEquals("logmessage count", logMessageCount, readHeader.getLogmessageCount());
    assertEquals("message length", payload.length, readHeader.getMessageLength());

    CRC32 expectedCrc = new CRC32();
    expectedCrc.update(payload);
    assertEquals("crc", (int) expectedCrc.getValue(), readHeader.getCrc());

    parent.release();
  }

  /**
   * Simulate the real producer path: reserve headerLength zero bytes via
   * OutputStream, write payload, then call writeHeader to fill in the
   * reserved space.
   */
  @Test
  public void testWriteHeaderWithOutputStreamReservedSpace() throws Exception {
    int logMessageCount = 4;
    Compression compression = Compression.NONE;

    StubTaskRequest tr = new StubTaskRequest("realpath", 88, 20000);
    tr.setCompressionForTest(compression);
    for (int i = 0; i < logMessageCount; i++) {
      tr.incrementLogMessageCountForTest();
    }

    short headerLength = MemqMessageHeader.getHeaderLength();
    ByteBuf buffer = PooledByteBufAllocator.DEFAULT.buffer(1024);
    ByteBufOutputStream os = new ByteBufOutputStream(buffer);
    os.write(new byte[headerLength]);
    byte[] payload = "output stream test payload".getBytes();
    os.write(payload);
    os.close();

    new MemqMessageHeader(tr).writeHeader(buffer);

    ByteBuf readBuf = buffer.duplicate();
    readBuf.readerIndex(0);
    MemqMessageHeader readHeader = new MemqMessageHeader(readBuf);

    assertEquals("version", tr.getVersion(), readHeader.getVersion());
    assertEquals("compression", compression.id, readHeader.getCompression());
    assertEquals("logmessage count", logMessageCount, readHeader.getLogmessageCount());
    assertEquals("message length", payload.length, readHeader.getMessageLength());

    CRC32 expectedCrc = new CRC32();
    expectedCrc.update(payload);
    assertEquals("crc", (int) expectedCrc.getValue(), readHeader.getCrc());

    buffer.release();
  }

  private static class StubTaskRequest extends TaskRequest {
    private long epoch = 0;

    public StubTaskRequest(String topicName, long currentRequestId,
                           int maxPayLoadBytes) throws IOException {
      super(topicName, currentRequestId, Compression.NONE, null, false, maxPayLoadBytes, 10000,
          null, 10000);
    }

    @Override
    protected MemqWriteResult runRequest() throws Exception {
      return null;
    }

    @Override
    public long getEpoch() {
      return epoch;
    }

    public void setEpochForTest(long epoch) {
      this.epoch = epoch;
    }

    public void setCompressionForTest(Compression c) {
      this.compression = c;
    }

    public void incrementLogMessageCountForTest() {
      incrementLogMessageCount();
    }
  }
}

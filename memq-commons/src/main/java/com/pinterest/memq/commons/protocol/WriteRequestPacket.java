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

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;

public class WriteRequestPacket implements Packet {

  private static final Logger logger = LoggerFactory.getLogger(WriteRequestPacket.class);

  protected boolean disableAcks;
  protected byte[] topicName;
  protected boolean checksumExists = false;
  protected int checksum;
  protected int dataLength;
  protected ByteBuf data;
  protected String producerId;
  /**
   * Per-broker slot ownership snapshot. Maps broker IP to the number of
   * slots this producer holds there.
   * <p>
   * On the v4+ wire each connection entry carries its IP plus the producer's
   * per-target slot count (in insertion order). On v3 the map is {@code null}
   * (the field is not part of the v3 protocol).
   * <p>
   * The broker eviction strategy uses these counts to choose consolidation
   * targets where the producer already has substantial slot share
   * (anti-ping-pong) and to break ties in regular target selection. When every
   * entry happens to carry an equal weight, that ranking degenerates to
   * "freest target wins".
   */
  protected Map<String, Integer> currentConnectionSlots;

  public WriteRequestPacket() {
  }

  public WriteRequestPacket(boolean disableAcks,
                            byte[] topicName,
                            boolean checksumExists,
                            int checksum,
                            ByteBuf payload) {
    this.disableAcks = disableAcks;
    this.topicName = topicName;
    this.checksumExists = checksumExists;
    this.checksum = checksum;
    setData(payload);
  }

  @Override
  public int getSize(short protocolVersion) {
    if (protocolVersion >= RequestType.V4) {
      return Byte.BYTES + Short.BYTES + topicName.length + Integer.BYTES
          + ProtocolUtils.getStringSerializedSizeWithTwoByteEncoding(producerId)
          + getConnectionsSerializedSize()
          + Integer.BYTES + dataLength;
    }
    return Byte.BYTES + Short.BYTES + topicName.length + Integer.BYTES + dataLength
        + (protocolVersion >= 1 ? Integer.BYTES : 0);
  }

  public static int getHeaderSize(short protocolVersion, String topicName) {
    return Byte.BYTES + Short.BYTES + topicName.length() + Integer.BYTES
        + (protocolVersion >= 1 ? Integer.BYTES : 0);
  }

  public static int getHeaderSize(short protocolVersion, String topicName,
                                   String producerId,
                                   Map<String, Integer> currentConnectionSlots) {
    if (protocolVersion >= RequestType.V4) {
      int size = Byte.BYTES + Short.BYTES + topicName.length() + Integer.BYTES
          + ProtocolUtils.getStringSerializedSizeWithTwoByteEncoding(producerId)
          + Short.BYTES + Integer.BYTES;
      if (currentConnectionSlots != null) {
        for (Map.Entry<String, Integer> e : currentConnectionSlots.entrySet()) {
          size += ProtocolUtils.getStringSerializedSizeWithTwoByteEncoding(e.getKey());
          size += Integer.BYTES;
        }
      }
      return size;
    }
    return getHeaderSize(protocolVersion, topicName);
  }

  @Override
  public void readFields(ByteBuf inBuffer, short protocolVersion) {
    if (protocolVersion >= RequestType.V4) {
      readFieldsV4(inBuffer);
    } else {
      readFieldsV3(inBuffer, protocolVersion);
    }
  }

  private void readFieldsV4(ByteBuf inBuffer) {
    disableAcks = inBuffer.readBoolean();
    short topicNameLength = inBuffer.readShort();
    topicName = new byte[topicNameLength];
    inBuffer.readBytes(topicName);
    checksum = inBuffer.readInt();
    checksumExists = true;

    producerId = ProtocolUtils.readStringWithTwoByteEncoding(inBuffer);

    short connCount = inBuffer.readShort();
    if (connCount > 0) {
      currentConnectionSlots = new LinkedHashMap<>(connCount);
      for (int i = 0; i < connCount; i++) {
        String ip = ProtocolUtils.readStringWithTwoByteEncoding(inBuffer);
        // Each connection entry carries the producer's per-target slot count.
        int slots = inBuffer.readInt();
        currentConnectionSlots.put(ip, slots);
      }
    } else {
      currentConnectionSlots = Collections.emptyMap();
    }

    dataLength = inBuffer.readInt();
    data = inBuffer;
    if (data.readableBytes() != dataLength) {
      logger.warn("Invalid length: {} vs {}", data.readableBytes(), dataLength);
    }
  }

  private void readFieldsV3(ByteBuf inBuffer, short protocolVersion) {
    disableAcks = inBuffer.readBoolean();
    short topicNameLength = inBuffer.readShort();
    topicName = new byte[topicNameLength];
    inBuffer.readBytes(topicName);
    if (protocolVersion >= 2) {
      checksum = inBuffer.readInt();
      checksumExists = true;
    }
    dataLength = inBuffer.readInt();
    data = inBuffer;
    if (data.readableBytes() != dataLength) {
      logger.warn("Invalid length: {} vs {}", data.readableBytes(), dataLength);
    }
  }

  @Override
  public void write(ByteBuf buf, short protocolVersion) {
    writeHeader(buf, protocolVersion);
    buf.writeBytes(data);
  }

  @Override
  public void writeHeader(ByteBuf headerBuf, short protocolVersion) {
    if (protocolVersion >= RequestType.V4) {
      writeHeaderV4(headerBuf);
    } else {
      writeHeaderV3(headerBuf);
    }
  }

  private void writeHeaderV4(ByteBuf headerBuf) {
    headerBuf.writeBoolean(disableAcks);
    headerBuf.writeShort((short) topicName.length);
    headerBuf.writeBytes(topicName);
    headerBuf.writeInt(checksum);

    ProtocolUtils.writeStringWithTwoByteEncoding(headerBuf, producerId);

    if (currentConnectionSlots != null && !currentConnectionSlots.isEmpty()) {
      headerBuf.writeShort((short) currentConnectionSlots.size());
      for (Map.Entry<String, Integer> e : currentConnectionSlots.entrySet()) {
        ProtocolUtils.writeStringWithTwoByteEncoding(headerBuf, e.getKey());
        // Each connection entry carries the producer's per-target slot count.
        headerBuf.writeInt(e.getValue() == null ? 0 : e.getValue());
      }
    } else {
      headerBuf.writeShort(0);
    }

    headerBuf.writeInt(dataLength);
  }

  private void writeHeaderV3(ByteBuf headerBuf) {
    headerBuf.writeBoolean(disableAcks);
    headerBuf.writeShort((short) topicName.length);
    headerBuf.writeBytes(topicName);
    headerBuf.writeInt(checksum);
    headerBuf.writeInt(dataLength);
  }

  private int getConnectionsSerializedSize() {
    int size = Short.BYTES;
    if (currentConnectionSlots != null) {
      for (Map.Entry<String, Integer> e : currentConnectionSlots.entrySet()) {
        size += ProtocolUtils.getStringSerializedSizeWithTwoByteEncoding(e.getKey());
        size += Integer.BYTES;
      }
    }
    return size;
  }

  public boolean isDisableAcks() {
    return disableAcks;
  }

  public void setDisableAcks(boolean disableAcks) {
    this.disableAcks = disableAcks;
  }

  public String getTopicName() {
    return new String(topicName, Charset.forName("utf-8"));
  }

  public void setTopicName(String topicName) {
    this.topicName = topicName.getBytes(Charset.forName("utf-8"));
  }

  public int getDataLength() {
    return dataLength;
  }

  public ByteBuf getData() {
    return data;
  }

  public void setData(ByteBuf data) {
    this.dataLength = data.readableBytes();
    this.data = data;
  }

  public void setData(byte[] data) {
    this.dataLength = data.length;
    this.data = PooledByteBufAllocator.DEFAULT.buffer(data.length);
    this.data.writeBytes(data);
  }

  public int getChecksum() {
    return checksum;
  }

  public void setChecksum(int checksum) {
    this.checksum = checksum;
  }

  public boolean isChecksumExists() {
    return checksumExists;
  }

  public void setChecksumExists(boolean checksumExists) {
    this.checksumExists = checksumExists;
  }

  public String getProducerId() {
    return producerId;
  }

  public void setProducerId(String producerId) {
    this.producerId = producerId;
  }

  public Map<String, Integer> getCurrentConnectionSlots() {
    return currentConnectionSlots;
  }

  public void setCurrentConnectionSlots(Map<String, Integer> currentConnectionSlots) {
    this.currentConnectionSlots = currentConnectionSlots;
  }

  @Override
  public void release() throws IOException {
    Packet.super.release();
  }
}

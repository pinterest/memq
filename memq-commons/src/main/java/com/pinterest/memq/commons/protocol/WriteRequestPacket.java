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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;

public class WriteRequestPacket implements Packet {

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
   * Single source of truth for both the v4 and v5 connection-set wire
   * formats. On v5, the wire carries IP + slot count for each entry. On
   * v4, the wire carries only the IPs (in insertion order) and the map
   * is populated with placeholder weight {@code 1} for each entry --
   * "equal weighting" semantics. On v3 the map is {@code null} (the
   * field is not part of the v3 protocol).
   * <p>
   * The broker eviction strategy uses these counts to choose
   * consolidation targets where the producer already has substantial
   * slot share (anti-ping-pong) and to break ties in regular target
   * selection. A v4 producer's equal-weight map degenerates that ranking
   * to "freest target wins", matching the legacy behavior.
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
    if (protocolVersion >= 4) {
      return Byte.BYTES + Short.BYTES + topicName.length + Integer.BYTES
          + ProtocolUtils.getStringSerializedSizeWithTwoByteEncoding(producerId)
          + getConnectionsSerializedSize(protocolVersion)
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
    if (protocolVersion >= 4) {
      int size = Byte.BYTES + Short.BYTES + topicName.length() + Integer.BYTES
          + ProtocolUtils.getStringSerializedSizeWithTwoByteEncoding(producerId)
          + Short.BYTES + Integer.BYTES;
      if (currentConnectionSlots != null) {
        for (Map.Entry<String, Integer> e : currentConnectionSlots.entrySet()) {
          size += ProtocolUtils.getStringSerializedSizeWithTwoByteEncoding(e.getKey());
          if (protocolVersion >= 5) {
            size += Integer.BYTES;
          }
        }
      }
      return size;
    }
    return getHeaderSize(protocolVersion, topicName);
  }

  @Override
  public void readFields(ByteBuf inBuffer, short protocolVersion) {
    if (protocolVersion >= 4) {
      readFieldsV4(inBuffer, protocolVersion);
    } else {
      readFieldsV3(inBuffer, protocolVersion);
    }
  }

  private void readFieldsV4(ByteBuf inBuffer, short protocolVersion) {
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
        // v5 carries per-entry slot counts; v4 has only the IPs and the
        // broker treats each entry as equal weight (placeholder 1).
        int slots = protocolVersion >= 5 ? inBuffer.readInt() : 1;
        currentConnectionSlots.put(ip, slots);
      }
    } else {
      currentConnectionSlots = Collections.emptyMap();
    }

    dataLength = inBuffer.readInt();
    data = inBuffer;
    if (data.readableBytes() != dataLength) {
      System.out.println("Invalid length:" + data.readableBytes() + "vs" + dataLength);
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
      System.out.println("Invalid length:" + data.readableBytes() + "vs" + dataLength);
    }
  }

  @Override
  public void write(ByteBuf buf, short protocolVersion) {
    writeHeader(buf, protocolVersion);
    buf.writeBytes(data);
  }

  @Override
  public void writeHeader(ByteBuf headerBuf, short protocolVersion) {
    if (protocolVersion >= 4) {
      writeHeaderV4(headerBuf, protocolVersion);
    } else {
      writeHeaderV3(headerBuf);
    }
  }

  private void writeHeaderV4(ByteBuf headerBuf, short protocolVersion) {
    headerBuf.writeBoolean(disableAcks);
    headerBuf.writeShort((short) topicName.length);
    headerBuf.writeBytes(topicName);
    headerBuf.writeInt(checksum);

    ProtocolUtils.writeStringWithTwoByteEncoding(headerBuf, producerId);

    if (currentConnectionSlots != null && !currentConnectionSlots.isEmpty()) {
      headerBuf.writeShort((short) currentConnectionSlots.size());
      for (Map.Entry<String, Integer> e : currentConnectionSlots.entrySet()) {
        ProtocolUtils.writeStringWithTwoByteEncoding(headerBuf, e.getKey());
        // v5 carries per-entry slot counts; v4 drops them and the receiver
        // reconstructs as equal-weight 1s.
        if (protocolVersion >= 5) {
          headerBuf.writeInt(e.getValue() == null ? 0 : e.getValue());
        }
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

  private int getConnectionsSerializedSize(short protocolVersion) {
    int size = Short.BYTES;
    if (currentConnectionSlots != null) {
      for (Map.Entry<String, Integer> e : currentConnectionSlots.entrySet()) {
        size += ProtocolUtils.getStringSerializedSizeWithTwoByteEncoding(e.getKey());
        if (protocolVersion >= 5) {
          size += Integer.BYTES;
        }
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

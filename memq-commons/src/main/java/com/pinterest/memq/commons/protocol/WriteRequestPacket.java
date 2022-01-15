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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;

public class WriteRequestPacket implements Packet {

  protected boolean disableAcks;
  protected byte[] topicName;
  protected boolean checksumExists = false;
  protected int checksum;
  protected int dataLength;
  protected ByteBuf data;

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
    return Byte.BYTES + Short.BYTES + topicName.length + Integer.BYTES + dataLength
        + (protocolVersion >= 1 ? Integer.BYTES : 0);
  }

  @Override
  public void readFields(ByteBuf inBuffer, short protocolVersion) {
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
      // request error
      System.out.println("Invalid length:" + data.readableBytes() + "vs" + dataLength);
    }
  }

  @Override
  public void write(ByteBuf buf, short protocolVersion) {
    buf.writeBoolean(disableAcks); // if ack all is enabled or not
    buf.writeShort((short) topicName.length); // topic name length
    buf.writeBytes(topicName); // topic name
    buf.writeInt(checksum);
    buf.writeInt(dataLength); // payload length
    buf.writeBytes(data); // payload
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

  @Override
  public void release() throws IOException {
    if (data != null) {
      data.release();
    }
    Packet.super.release();
  }
}

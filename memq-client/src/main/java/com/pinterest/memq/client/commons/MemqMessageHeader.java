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

import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.zip.CRC32;

import com.pinterest.memq.client.producer.TaskRequest;
import com.pinterest.memq.client.producer2.Request;
import com.pinterest.memq.core.utils.MemqUtils;

import io.netty.buffer.ByteBuf;

public class MemqMessageHeader {

  private short headerLength;
  private short version;
  private short additionalHeaderLength;
  private int crc;
  private byte compression;
  private int logmessageCount;
  private int messageLength;
  private long producerEpoch;
  private long producerRequestId;
  private byte[] producerAddress;

  private TaskRequest taskRequest = null;
  private Request request = null;

  public MemqMessageHeader(ByteBuf byteBuf) {
    headerLength = byteBuf.readShort();
    version = byteBuf.readShort();
    additionalHeaderLength = byteBuf.readShort();
    if (additionalHeaderLength > 0) {
      producerAddress = new byte[byteBuf.readByte()];
      byteBuf.readBytes(producerAddress);
      producerEpoch = byteBuf.readLong();
      producerRequestId = byteBuf.readLong();
    }
    crc = byteBuf.readInt();
    compression = byteBuf.readByte();
    logmessageCount = byteBuf.readInt();
    messageLength = byteBuf.readInt();
  }

  public MemqMessageHeader(ByteBuffer buf) {
    headerLength = buf.getShort();
    version = buf.getShort();
    additionalHeaderLength = buf.getShort();
    if (additionalHeaderLength > 0) {
      byte[] additionalInfo = new byte[additionalHeaderLength];
      buf.get(additionalInfo);
      ByteBuffer wrap = ByteBuffer.wrap(additionalInfo);
      producerAddress = new byte[wrap.get()];
      wrap.get(producerAddress);
      producerEpoch = wrap.getLong();
      producerRequestId = wrap.getLong();
    }
    crc = buf.getInt();
    compression = buf.get();
    logmessageCount = buf.getInt();
    messageLength = buf.getInt();
  }

  public MemqMessageHeader(DataInputStream stream) throws IOException {
    headerLength = stream.readShort();
    version = stream.readShort();
    additionalHeaderLength = stream.readShort();
    if (additionalHeaderLength > 0) {
      producerAddress = new byte[stream.readByte()];
      stream.readFully(producerAddress);
      producerEpoch = stream.readLong();
      producerRequestId = stream.readLong();
    }
    crc = stream.readInt();
    compression = stream.readByte();
    logmessageCount = stream.readInt();
    messageLength = stream.readInt();
  }

  /**
   * @param taskRequest
   */
  public MemqMessageHeader(TaskRequest taskRequest) {
    this.taskRequest = taskRequest;
  }

  public MemqMessageHeader(Request request) {
    this.request = request;
  }

  public static short getHeaderLength() {
    return (short) (2 // header length encoding
        + 2 // version of the header
        + 2 // extra header content
        + MemqUtils.HOST_IPV4_ADDRESS.length + 17
        // placeholder for any additional headers info to add
        + 4 // bytes for crc of the message body
        + 1 // compression scheme
        + 4 // for count of logmessages in the body
        + 4 // bytes for length of the message body
    );
  }

  public void writeHeader(final ByteBuf buffer) {
    boolean useTaskRequest = taskRequest != null;
    CRC32 crc = new CRC32();
    ByteBuf wrap = buffer.duplicate();
    wrap.writerIndex(0);
    wrap.writeShort(getHeaderLength()); // 2bytes
    if (useTaskRequest) {
      wrap.writeShort(taskRequest.getVersion()); // 2bytes
    } else {
      wrap.writeShort(request.getVersion()); // 2bytes
    }
    // add extra stuff
    byte[] extraHeaderContent = getExtraHeaderContent();
    wrap.writeShort((short) extraHeaderContent.length);// 2bytes
    wrap.writeBytes(extraHeaderContent);

    ByteBuf tmp = buffer.duplicate();
    tmp.readerIndex(getHeaderLength());
    ByteBuf slice = tmp.slice();
    crc.update(slice.nioBuffer());
    int checkSum = (int) crc.getValue();
    // write crc checksum of the body
    wrap.writeInt(checkSum);// 4bytes
    // compression scheme encoding
    if (useTaskRequest) {
      wrap.writeByte(taskRequest.getCompression().id);// 1byte
      wrap.writeInt(taskRequest.getLogmessageCount()); // 4bytes
    } else {
      wrap.writeByte(request.getCompression().id);// 1byte
      wrap.writeInt(request.getMessageCount()); // 4bytes
    }

    // write the length of remaining bytes
    int payloadLength = buffer.readableBytes() - getHeaderLength();
    wrap.writeInt(payloadLength); // 4bytes
  }

  private byte[] getExtraHeaderContent() {
    boolean useTaskRequest = taskRequest != null;
    // add tracking info
    byte[] hostAddress = MemqUtils.HOST_IPV4_ADDRESS;
    ByteBuffer extrainfo = ByteBuffer.allocate(hostAddress.length + 1 + 8 + 8);
    extrainfo.put((byte) hostAddress.length);
    extrainfo.put(hostAddress);
    if (useTaskRequest) {
      // add producer epoch
      extrainfo.putLong(taskRequest.getEpoch());
      // add client request id
      extrainfo.putLong(taskRequest.getId());
    } else {
      // add producer epoch
      extrainfo.putLong(request.getEpoch());
      // add client request id
      extrainfo.putLong(request.getClientRequestId());
    }
    return extrainfo.array();
  }

  public short getVersion() {
    return version;
  }

  public short getBytesToSkip() {
    return additionalHeaderLength;
  }

  public int getCrc() {
    return crc;
  }

  public byte getCompression() {
    return compression;
  }

  public int getMessageLength() {
    return messageLength;
  }

  public int getLogmessageCount() {
    return logmessageCount;
  }

  public long getProducerEpoch() {
    return producerEpoch;
  }

  public long getProducerRequestId() {
    return producerRequestId;
  }

  public byte[] getProducerAddress() {
    return producerAddress;
  }

  @Override
  public String toString() {
    return "BatchHeader [headerLength=" + headerLength + ", version=" + version + ", bytesToSkip="
        + additionalHeaderLength + ", additionalInfo=["
        + (producerAddress != null && producerAddress.length == 4
            ? MemqUtils.getStringFromByteAddress(producerAddress)
            : "N/A")
        + "," + producerEpoch + ", " + producerRequestId + "], crc=" + crc + ", compression="
        + compression + ", logmessageCount=" + logmessageCount + ", batchLength=" + messageLength
        + "]";
  }

}

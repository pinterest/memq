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
package com.pinterest.memq.client.producer2;

import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import com.pinterest.memq.commons.MessageId;

import io.netty.util.Recycler;

public class RawRecord {
  public static final AtomicLong counter = new AtomicLong(0);
  private static final Recycler<RawRecord> RECYCLER = new Recycler<RawRecord>() {
    @Override
    protected RawRecord newObject(Handle<RawRecord> handle) {
      counter.incrementAndGet();
      return new RawRecord(handle);
    }
  };

  private byte[] messageIdBytes;
  private byte[] headerBytes;
  private byte[] keyBytes;
  private byte[] valueBytes;
  private long writeTimestamp;
  private final Recycler.Handle<RawRecord> handle;

  private RawRecord(Recycler.Handle<RawRecord> handle) {
    this.handle = handle;
  }

  public static RawRecord newInstance(MessageId messageId, Map<String, byte[]> headers, byte[] keyBytes,
                   byte[] valueBytes, long writeTimestamp)
      throws IOException {
    RawRecord record = RECYCLER.get();
    if (valueBytes == null) {
      throw new IOException("value can not be null");
    }
    record.messageIdBytes = messageId != null ? messageId.toByteArray() : null;
    record.headerBytes = serializeHeadersToByteArray(headers);
    record.keyBytes = keyBytes;
    record.valueBytes = valueBytes;
    record.writeTimestamp = writeTimestamp;
    return record;
  }

  public void recycle() {
    this.messageIdBytes = null;
    this.headerBytes = null;
    this.keyBytes = null;
    this.valueBytes = null;
    this.writeTimestamp = 0;
    handle.recycle(this);
  }

  public byte[] getMessageIdBytes() {
    return messageIdBytes;
  }

  public byte[] getHeaderBytes() {
    return headerBytes;
  }

  public byte[] getKeyBytes() {
    return keyBytes;
  }

  public byte[] getValueBytes() {
    return valueBytes;
  }

  public long getWriteTimestamp() {
    return writeTimestamp;
  }

  protected static byte[] serializeHeadersToByteArray(Map<String, byte[]> headers) throws IOException {
    if (headers != null) {
      ByteArrayOutputStream out = new ByteArrayOutputStream(11);
      DataOutputStream str = new DataOutputStream(out);
      for (Map.Entry<String, byte[]> entry : headers.entrySet()) {
        byte[] k = entry.getKey().getBytes();
        byte[] v = entry.getValue();
        str.writeShort(k.length);
        str.write(k);
        str.writeShort(v.length);
        str.write(v);
      }
      str.close();
      return out.toByteArray();
    } else {
      return null;
    }
  }

  public int calculateEncodedLogMessageLength() {
    /* LogMessage layout
     * |--Additional Header Length (2B)----|-------------Timestamp (8B)---------------| //
     * Additional Header Fields
     * |--MessageId Length (1B)--|-----------------------MessageId (Var.)-------------| //
     * |--Header Length (2B)-----|-----------------------Headers (Var.)---------------| //
     * |------------------------------Key Length (4B)---------------------------------| // Key
     * Fields
     * |------------------------------Key (Var.)--------------------------------------| //
     * |------------------------------Value Length (4B)-------------------------------| // Value
     * Fields
     * |------------------------------Value (Var.)------------------------------------| //
     *
     * Fixed Overhead: 2 + 8 + 1 + 2 + 4 + 4 = 21 B
     */
    return
        + 2 + getAdditionalHeaderLength() // request additional headers length + additional fields
        + 4 + (keyBytes != null ? keyBytes.length : 0)  // key length + key
        + 4 + valueBytes.length // value length + value
        ;
  }

  public void writeToOutputStream(OutputStream outputStream) throws IOException {
    /* LogMessage layout
     * |-Additional Header Len (2B)------|------------------------Timestamp (8B)-------------------| // Additional Header Fields
     * |-MsgId Len (1B)-|-----------------------MessageId (Var.)-----------------------------------| //
     * |-Header Len (2B)-----------------|------------------------Headers (Var.)-------------------| //
     * |--------------------------------Key Len (4B)-----------------------------------------------| // Key Fields
     * |--------------------------------Key (Var.)-------------------------------------------------| //
     * |--------------------------------Value Length (4B)------------------------------------------| // Value Fields
     * |--------------------------------Value (Var.)-----------------------------------------------| //
     *
     * Fixed Overhead: 2 + 8 + 1 + 2 + 4 + 4 = 21 B
     */

    // #######################################
    // write additional fields here in future
    // #######################################
    // 8 bytes for write ts
    // 1 byte for messageId length
    // 2 bytes for header length

    // #######################################
    // write additional fields here in future
    // #######################################

    // buffered OS with underlying buffer of the first fields
    DataOutputStream dataOutputStream = new DataOutputStream(new BufferedOutputStream(outputStream, 2 + getAdditionalHeaderLength() + 4));

    dataOutputStream.writeShort((short) getAdditionalHeaderLength());
    // write timestamp
    dataOutputStream.writeLong(writeTimestamp);
    // message id
    if (messageIdBytes != null) {
      dataOutputStream.write((byte) messageIdBytes.length);
      dataOutputStream.write(messageIdBytes);
    } else {
      dataOutputStream.write((byte) 0);
    }
    // encode and write user defined headers
    if (headerBytes != null) {
      dataOutputStream.writeShort(headerBytes.length);
      dataOutputStream.write(headerBytes);
    } else {
      dataOutputStream.writeShort(0);
    }

    if (keyBytes != null) {
      // mark keys present
      dataOutputStream.writeInt(keyBytes.length);
      dataOutputStream.write(keyBytes);
    } else {
      dataOutputStream.writeInt(0);
    }
    dataOutputStream.writeInt(valueBytes.length);
    dataOutputStream.flush();
    outputStream.write(valueBytes);
    outputStream.flush();
  }

  private int getAdditionalHeaderLength() {
    return  + 8 // timestamp (long)
            + 1 + (messageIdBytes != null ? messageIdBytes.length : 0) // messageId length + messageId
            + 2 + (headerBytes != null ? headerBytes.length : 0) // message header length + message header
    ;
  }
}

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
package com.pinterest.memq.commons;

import java.io.DataInputStream;
import java.io.IOException;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.logging.Logger;

import com.pinterest.memq.commons.protocol.Packet;

import io.netty.buffer.ByteBuf;

/**
 * Header is used for improving speed of seeks in a batch.
 * 
 * This is used to maintain a SortedMap of Relative Id to IndexEntry where
 * {@link IndexEntry} contains the offset and size of the Message.
 * 
 * This functionality is usually used by any consumer that looking to perform a
 * peek style lookup of data. e.g. check {@link MemqLogMessage} at specific
 * position.
 */
public class BatchHeader {
  private static final Logger logger = Logger.getLogger(BatchHeader.class.getName());
  private SortedMap<Integer, IndexEntry> messageIndex;
  private int headerLength;

  public BatchHeader(DataInputStream stream) throws IOException {
    messageIndex = new TreeMap<>();
    headerLength = stream.readInt();
    int numIndexEntries = stream.readInt();// number of entries
    logger.fine(() -> "header len: " + headerLength + ", num entries: " + numIndexEntries);
    for (int i = 0; i < numIndexEntries; i++) {
      int idx = stream.readInt();
      int offset = stream.readInt();
      int size = stream.readInt();
      messageIndex.put(idx, new IndexEntry(offset, size));
    }
  }

  public ByteBuf writeHeaderToByteBuf(ByteBuf buf) {
    buf.writeInt(headerLength);
    buf.writeInt(messageIndex.size());
    for (Entry<Integer, IndexEntry> entry : messageIndex.entrySet()) {
      buf.writeInt(entry.getKey());
      buf.writeInt(entry.getValue().getOffset());
      buf.writeInt(entry.getValue().getSize());
    }
    return buf;
  }

  public SortedMap<Integer, IndexEntry> getMessageIndex() {
    return messageIndex;
  }

  @Override
  public String toString() {
    return "BatchHeader [messageIndex=" + messageIndex + "]";
  }

  /**
   * Used by {@link BatchHeader} to locate the byte offset (seek position) and
   * length of the Message in Batch so seeks and access can be performed in an
   * optimal fashion.
   */
  public static class IndexEntry implements Packet {

    private int offset;
    private int size;

    public IndexEntry() {
    }

    public IndexEntry(int offset, int size) {
      this.offset = offset;
      this.size = size;
    }

    @Override
    public void readFields(ByteBuf buf, short protocolVersion) throws IOException {
      offset = buf.readInt();
      size = buf.readInt();
    }

    @Override
    public void write(ByteBuf buf, short protocolVersion) {
      buf.writeInt(offset);
      buf.writeInt(size);
    }

    @Override
    public int getSize(short protocolVersion) {
      return Integer.BYTES * 2;
    }

    public int getOffset() {
      return offset;
    }

    public void setOffset(int offset) {
      this.offset = offset;
    }

    public int getSize() {
      return size;
    }

    public void setSize(int size) {
      this.size = size;
    }

    @Override
    public String toString() {
      return "IndexEntry [offset=" + offset + ", size=" + size + "]";
    }

  }

}

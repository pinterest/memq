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
package com.pinterest.memq.core.commons;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import com.codahale.metrics.Counter;

import io.netty.buffer.ByteBuf;

/**
 * Converts a list of Messages into an Inputstream so bytes of the messages can
 * we read in sequence i.e. concatenated together.
 */
public class MessageBufferInputStream extends InputStream {

  private List<Message> list;
  private int bufferIdx;
  private int markBufferIdx = -1;
  private int markBufferPosition = -1;
  private int readLimit = -1;
  private int bytesRead;
  private ByteBuf buf;
  private int readLimitCheck = 0;

  public MessageBufferInputStream(List<Message> list, Counter streamResetCounter) {
    this.list = list;
    this.buf = list.get(bufferIdx).getBuf();
    this.buf.resetReaderIndex();
  }

  @Override
  public int read() throws IOException {
    if (bufferIdx > list.size() - 1) {
      return -1;
    }
    if (buf == null) {
      buf = list.get(bufferIdx).getBuf();
      buf.resetReaderIndex();
    }
    if (buf.readableBytes() > 0) {
      bytesRead++;
      // map from negative to positive (0-255) range before returning
      return buf.readByte() & 0xff;
    } else {
      buf = null;
      bufferIdx++;
      return read();
    }
  }

  @Override
  public synchronized void mark(int readlimit) {
    throw new UnsupportedOperationException();
  }

  @Override
  public synchronized void reset() throws IOException {
    throw new UnsupportedOperationException();
  }

  public void resetToBeginnging() {
    this.bufferIdx = 0;
    this.bytesRead = 0;
    this.readLimitCheck = 0;
    this.readLimit = -1;
    this.markBufferIdx = -1;
    this.markBufferPosition = -1;
    this.buf = list.get(bufferIdx).getBuf();
    this.buf.resetReaderIndex();
  }

  @Override
  public boolean markSupported() {
    return true;
  }

  public int getBytesRead() {
    return bytesRead;
  }

  @Override
  public void close() throws IOException {
  }

  public void printDiagnosticsInfo() {
    System.err.println(toString());
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString() {
    return "MessageBufferInputStream [list=" + list + ", bufferIdx=" + bufferIdx
        + ", markBufferIdx=" + markBufferIdx + ", markBufferPosition=" + markBufferPosition
        + ", readLimit=" + readLimit + ", bytesRead=" + bytesRead + ", buf=" + buf
        + ", readLimitCheck=" + readLimitCheck + "]";
  }

}
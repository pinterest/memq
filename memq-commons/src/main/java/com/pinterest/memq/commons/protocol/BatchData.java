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

import io.netty.buffer.ByteBuf;

public class BatchData implements Packet {

  private ByteBuf dataAsBuf;
  private int length;
  private Object sendFileRef;

  public BatchData() {
  }

  public BatchData(int length, ByteBuf dataBuf) {
    this.length = length;
    this.dataAsBuf = dataBuf;
  }

  public BatchData(int length, Object sendFileRef) {
    this.length = length;
    this.sendFileRef = sendFileRef;
  }

  @Override
  public void readFields(ByteBuf buf, short protocolVersion) throws IOException {
    length = buf.readInt();
    if (length > 0) {
      buf.retain();
      dataAsBuf = buf.readSlice(length);
    }
  }

  @Override
  public void write(ByteBuf buf, short protocolVersion) {
    buf.writeInt(length);
    if (dataAsBuf != null) {
      // this is an optimization hack to enable sendFile calls
      // if dataAsBuf is not null then send it using that else just write the length
      // and use other workarounds for writing the payload
      dataAsBuf.resetReaderIndex();
      buf.writeBytes(dataAsBuf);
    }
  }

  @Override
  public int getSize(short protocolVersion) {
    return Integer.BYTES + (length > 0 ? length : 0);
  }

  public int getLength() {
    return length;
  }

  public void setLength(int length) {
    this.length = length;
  }

  public ByteBuf getDataAsBuf() {
    return dataAsBuf;
  }

  public void setDataAsBuf(ByteBuf dataAsBuf) {
    this.dataAsBuf = dataAsBuf;
  }

  public Object getSendFileRef() {
    return sendFileRef;
  }

  public void setSendFileRef(Object sendFileRef) {
    this.sendFileRef = sendFileRef;
  }

}

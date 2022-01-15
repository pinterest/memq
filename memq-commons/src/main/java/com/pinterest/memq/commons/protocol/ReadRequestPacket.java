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
import java.util.Arrays;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.pinterest.memq.commons.BatchHeader.IndexEntry;

import io.netty.buffer.ByteBuf;

public class ReadRequestPacket implements Packet {

  public static final int DISABLE_READ_AT_INDEX = -1;
  private static final Gson gson = new Gson();
  protected byte[] topicName;
  protected byte[] notification;
  // for header requests only
  protected boolean readHeaderOnly;
  protected IndexEntry readIndex = new IndexEntry(DISABLE_READ_AT_INDEX, DISABLE_READ_AT_INDEX);

  public ReadRequestPacket() {
  }

  public ReadRequestPacket(String topicName, JsonObject notification) {
    this.topicName = topicName.getBytes(Charset.forName("utf-8"));
    this.notification = gson.toJson(notification).getBytes(Charset.forName("utf-8"));
  }

  public ReadRequestPacket(String topicName, JsonObject notification, boolean readHeaderOnly) {
    this(topicName, notification);
    this.readHeaderOnly = readHeaderOnly;
  }

  public ReadRequestPacket(String topicName,
                           JsonObject notification,
                           boolean readHeaderOnly,
                           IndexEntry readIndex) {
    this(topicName, notification, readHeaderOnly);
    this.readIndex = readIndex;
  }

  @Override
  public void readFields(ByteBuf buf, short protocolVersion) throws IOException {
    short topicNameLength = buf.readShort();
    topicName = new byte[topicNameLength];
    buf.readBytes(topicName);

    short notificationLength = buf.readShort();
    notification = new byte[notificationLength];
    buf.readBytes(notification);

    readHeaderOnly = buf.readBoolean();

    readIndex = new IndexEntry();
    readIndex.readFields(buf, protocolVersion);
  }

  @Override
  public void write(ByteBuf buf, short protocolVersion) {
    buf.writeShort(topicName.length);
    buf.writeBytes(topicName);

    buf.writeShort(notification.length);
    buf.writeBytes(notification);

    buf.writeBoolean(readHeaderOnly);

    readIndex.write(buf, protocolVersion);
  }

  @Override
  public int getSize(short protocolVersion) {
    return Short.BYTES + topicName.length + Short.BYTES + notification.length + 1
        + readIndex.getSize(protocolVersion);
  }

  public String getTopicName() {
    return new String(topicName, Charset.forName("utf-8"));
  }

  public void setTopicName(String topicName) {
    this.topicName = topicName.getBytes(Charset.forName("utf-8"));
  }

  public boolean isReadHeaderOnly() {
    return readHeaderOnly;
  }

  public JsonObject getNotification() {
    return gson.fromJson(new String(notification, Charset.forName("utf-8")), JsonObject.class);
  }

  public void setNotification(JsonObject notification) {
    this.notification = notification.toString().getBytes(Charset.forName("utf-8"));
  }

  public IndexEntry getReadIndex() {
    return readIndex;
  }

  @Override
  public String toString() {
    return "ReadRequestPacket [topicName=" + Arrays.toString(topicName) + ", notification="
        + Arrays.toString(notification) + ", readHeaderOnly=" + readHeaderOnly + ", readIndex="
        + readIndex + "]";
  }

}
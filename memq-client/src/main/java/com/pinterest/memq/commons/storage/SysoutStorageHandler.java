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
package com.pinterest.memq.commons.storage;

import java.util.List;
import java.util.Properties;

import com.codahale.metrics.MetricRegistry;
import com.pinterest.memq.core.commons.Message;

import io.netty.buffer.ByteBuf;

@StorageHandlerName(name = "sysout")
public class SysoutStorageHandler implements StorageHandler {

  @Override
  public void initWriter(Properties outputHandlerConfig,
                   String topic,
                   MetricRegistry registry) throws Exception {
  }

  @Override
  public void writeOutput(int sizeInBytes,
                          int checksum,
                          List<Message> messages) throws WriteFailedException {
    for (Message message : messages) {
      System.out.println(new String(readToByteArray(message.getBuf())));
    }
  }

  public static byte[] readToByteArray(ByteBuf buf) {
    byte[] ary = new byte[buf.readableBytes()];
    for (int i = 0; i < buf.readableBytes(); i++) {
      ary[i] = buf.readByte();
    }
    return ary;
  }

  @Override
  public String getReadUrl() {
    return "System.in";
  }

}
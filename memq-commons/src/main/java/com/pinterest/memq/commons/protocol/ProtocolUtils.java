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

import io.netty.buffer.ByteBuf;

public class ProtocolUtils {

  public static String readStringWithTwoByteEncoding(ByteBuf buf) {
    short length = buf.readShort();
    byte[] str = new byte[length];
    buf.readBytes(str);
    return new String(str);
  }

  public static void writeStringWithTwoByteEncoding(ByteBuf buf, String str) {
    if (str == null || str.isEmpty()) {
      buf.writeShort(0);
    } else {
      byte[] bytes = str.getBytes();
      buf.writeShort(bytes.length);
      buf.writeBytes(bytes);
    }
  }

  public static int getStringSerializedSizeWithTwoByteEncoding(String str) {
    return (str == null || str.isEmpty() ? 0 : (str.getBytes().length)) + Short.BYTES;
  }

}

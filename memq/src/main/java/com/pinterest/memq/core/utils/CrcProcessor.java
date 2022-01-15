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
package com.pinterest.memq.core.utils;

import io.netty.util.ByteProcessor;

import java.util.zip.CRC32;

public final class CrcProcessor implements ByteProcessor {

  private CRC32 crc32 = new CRC32();

  @Override
  public boolean process(byte value) throws Exception {
    crc32.update(value);
    return true;
  }

  public int getChecksum() {
    return (int) crc32.getValue();
  }
}

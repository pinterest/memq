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
import java.io.InputStream;
import java.util.zip.CRC32;

public class CommonUtils {

  /**
   * Validate whether CRC checksum for a batch matches the given headerCrc
   *
   * @param batch
   * @param headerCrc
   * @return
   */
  public static boolean crcChecksumMatches(byte[] batch, int headerCrc) {
    CRC32 crc = new CRC32();
    crc.update(batch);
    if ((int) crc.getValue() != headerCrc) {
      return false;
    }
    return true;
  }

  /**
   * Given a compression id and compressed InputStream, return an uncompressed
   * DataInputStream
   *
   * @param compression the compression id
   * @param original    the compressed InputStream
   * @return an uncompressed DataInputStream
   * @throws IOException
   * @throws UnknownCompressionException
   */
  public static DataInputStream getUncompressedInputStream(byte compression,
                                                           InputStream original) throws IOException,
                                                                                        UnknownCompressionException {
    if (compression == 0) {
      return new DataInputStream(original);
    }
    for (Compression comp : Compression.values()) {
      if (comp.id == compression) {
        return new DataInputStream(comp.getCompressStream(original));
      }
    }
    throw new UnknownCompressionException("Compression id " + compression + " is not supported");
  }

}

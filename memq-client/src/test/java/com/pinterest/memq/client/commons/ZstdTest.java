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

import static org.junit.Assert.assertTrue;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Random;

import org.apache.commons.io.IOUtils;
import org.apache.commons.io.output.TeeOutputStream;

import com.github.luben.zstd.ZstdInputStream;
import com.github.luben.zstd.ZstdOutputStream;

// Used to verify zstd version compatibility
public class ZstdTest {

  //@Test
  public void write() throws Exception {
    FileOutputStream originalFile = new FileOutputStream("test-original");
    FileOutputStream compressedFile = new FileOutputStream("test-compressed");
    OutputStream compressionStream = new ZstdOutputStream(compressedFile);
    byte[] bytes = new byte[2 * 1024 * 1024];
    new Random().nextBytes(bytes);
    TeeOutputStream tos = new TeeOutputStream(originalFile, compressionStream);
    tos.write(bytes);
    tos.close();
  }

  //@Test
  public void read() throws Exception {
    FileInputStream originalFile = new FileInputStream("test-original");
    FileInputStream compressedFile = new FileInputStream("test-compressed");
    InputStream decompressionStream = new ZstdInputStream(compressedFile);
    assertTrue(IOUtils.contentEquals(originalFile, decompressionStream));
  }
}

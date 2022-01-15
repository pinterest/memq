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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import com.github.luben.zstd.RecyclingBufferPool;
import com.github.luben.zstd.ZstdInputStreamNoFinalizer;
import com.github.luben.zstd.ZstdOutputStreamNoFinalizer;

public enum Compression {
       NONE(0, 0, is -> is, os -> os),
       GZIP(1, 512,
           GZIPInputStream::new,
           outputStream -> new GZIPOutputStream(outputStream, true)
       ),
       ZSTD(2, 0,
           is -> new ZstdInputStreamNoFinalizer(is, RecyclingBufferPool.INSTANCE),
           os -> new ZstdOutputStreamNoFinalizer(os, RecyclingBufferPool.INSTANCE)
       );

  public byte id;
  public int minBufferSize;
  private final CompressionWrapper<InputStream> inputStreamCompressionWrapper;
  private final CompressionWrapper<OutputStream> outputStreamCompressionWrapper;


  Compression(int id, int minBufferSize,
              CompressionWrapper<InputStream> inputStreamCompressionWrapper,
              CompressionWrapper<OutputStream> outputStreamCompressionWrapper) {
    this.id = (byte) id;
    this.minBufferSize = minBufferSize;
    this.inputStreamCompressionWrapper = inputStreamCompressionWrapper;
    this.outputStreamCompressionWrapper = outputStreamCompressionWrapper;
  }

  @FunctionalInterface
  private interface CompressionWrapper<T> {
    T getWrappedInstance(T t) throws IOException;
  }

  public InputStream getCompressStream(InputStream is) throws IOException {
    return inputStreamCompressionWrapper.getWrappedInstance(is);
  }

  public OutputStream getDecompressStream(OutputStream os) throws IOException {
    return outputStreamCompressionWrapper.getWrappedInstance(os);
  }
}
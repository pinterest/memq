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
package com.pinterest.memq.client.producer;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Semaphore;
import java.util.zip.GZIPOutputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.luben.zstd.ZstdOutputStream;
import com.google.common.annotations.VisibleForTesting;
import com.pinterest.memq.client.commons.Compression;
import com.pinterest.memq.client.commons.MemqMessageHeader;
import com.pinterest.memq.commons.MessageId;
import com.pinterest.memq.core.utils.MemqUtils;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.buffer.PooledByteBufAllocator;

public abstract class TaskRequest implements Callable<MemqWriteResult> {

  public static final int COMPRESSION_WINDOW = 512;
  private static final Logger LOG = LoggerFactory.getLogger(TaskRequest.class);
  private static final int READY_CHECK_FREQUENCY = 100;
  protected Compression compression;
  protected int logmessageCount;
  protected MemqMessageHeader header = new MemqMessageHeader(this);
  protected long clientRequestId;
  protected long serverRequestId;
  protected OutputStream payloadOutputStream;
  protected boolean disableAcks;
  private int maxPayLoadBytes;
  private String topicName;
  private Semaphore maxRequestLock;
  private volatile boolean ready = false;
  protected Map<Long, TaskRequest> requestMap;
  protected int ackCheckPollIntervalMs;
  protected int requestAckTimeout;
  protected byte[] messageIdHash;
  protected ByteBuf buffer;

  public TaskRequest(String topicName,
                     long currentRequestId,
                     Compression compression,
                     Semaphore maxRequestLock,
                     boolean disableAcks,
                     int maxPayLoadBytes,
                     int ackCheckPollIntervalMs,
                     Map<Long, TaskRequest> requestMap,
                     int requestAckTimeout) throws IOException {
    this.topicName = topicName;
    this.clientRequestId = currentRequestId;
    this.compression = compression;
    this.disableAcks = disableAcks;
    this.maxPayLoadBytes = maxPayLoadBytes;
    this.maxRequestLock = maxRequestLock;
    this.ackCheckPollIntervalMs = ackCheckPollIntervalMs;
    this.requestMap = requestMap;
    this.requestAckTimeout = requestAckTimeout;
    this.buffer = PooledByteBufAllocator.DEFAULT.buffer(maxPayLoadBytes - COMPRESSION_WINDOW);
    ByteBufOutputStream buf = new ByteBufOutputStream(buffer);
    this.payloadOutputStream = prepareOutputStream(buf);
  }

  public String getTopicName() {
    return topicName;
  }

  public int getMaxPayLoadBytes() {
    return maxPayLoadBytes;
  }

  public OutputStream getOutputStream() {
    return payloadOutputStream;
  }

  @VisibleForTesting
  public byte[] getPayloadAsByteArrays() {
    ByteBuf duplicate = buffer.duplicate();
    duplicate.resetReaderIndex();
    byte[] bytes = new byte[duplicate.readableBytes()];
    duplicate.readBytes(bytes);
    return bytes;
  }

  public int remaining() {
    return maxPayLoadBytes - buffer.readableBytes();
  }

  public void markReady() throws IOException {
    payloadOutputStream.close();
    header.writeHeader(buffer);
    this.ready = true;
  }

  @Override
  public MemqWriteResult call() throws Exception {
    while (!ready) {
      // wait while this request is ready to be processed
      Thread.sleep(READY_CHECK_FREQUENCY);
    }
    try {
      return runRequest();
    } catch (Exception e) {
      LOG.error("Request failed clientRequestId(" + clientRequestId + ") serverRequestId("
          + serverRequestId + ")", e);

      // handle nested ExecutionExceptions
      Exception ee = e;
      if (ee instanceof ExecutionException) {
        while (ee.getCause() instanceof Exception) {
          ee = (Exception) ee.getCause();
        }
      }

      throw ee;
    } finally {
      requestMap.remove(clientRequestId);
      maxRequestLock.release();
    }
  }

  protected abstract MemqWriteResult runRequest() throws Exception;

  public int size() {
    return buffer.readableBytes();
  }

  public long getId() {
    return clientRequestId;
  }

  public OutputStream prepareOutputStream(ByteBufOutputStream stream) throws IOException {
    OutputStream byteBuffer;
    int headerLength = MemqMessageHeader.getHeaderLength();
    stream.write(new byte[headerLength]);
    if (compression == Compression.GZIP) {
      byteBuffer = new GZIPOutputStream(stream, COMPRESSION_WINDOW, true);
    } else if (compression == Compression.ZSTD) {
      byteBuffer = new ZstdOutputStream(stream);
    } else {
      byteBuffer = stream;
    }
    return byteBuffer;
  }

  public boolean isReady() {
    return ready;
  }

  public void addMessageId(MessageId messageId) {
    if (messageId == null) {
      return;
    }
    byte[] byteArray = messageId.toByteArray();
    messageIdHash = MemqUtils.calculateMessageIdHash(messageIdHash, byteArray);
  }

  protected void incrementLogMessageCount() {
    logmessageCount++;
  }

  public MemqMessageHeader getHeader() {
    return header;
  }

  public Compression getCompression() {
    return compression;
  }

  protected void setCompression(Compression compression) {
    this.compression = compression;
  }

  public byte[] getMessageIdHash() {
    return messageIdHash;
  }

  @VisibleForTesting
  public short getVersion() {
    return 1_0_0;
  }

  @VisibleForTesting
  public ByteBuf getBuffer() {
    return buffer;
  }

  public int getLogmessageCount() {
    return logmessageCount;
  }

  public abstract long getEpoch();

}

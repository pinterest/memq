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
package com.pinterest.memq.client.producer2;

import com.pinterest.memq.client.commons.Compression;
import com.pinterest.memq.client.commons.MemqMessageHeader;
import com.pinterest.memq.client.producer.MemqWriteResult;
import com.pinterest.memq.core.utils.MemqUtils;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.buffer.PooledByteBufAllocator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class BufferedRequest {
    private static final Logger logger = LoggerFactory.getLogger(Request.class);
    private final BufferedRequestManager bufferedRequestManager;
    private final String topic;
    private final int clientRequestId;
    private final int maxRequestSize;
    private final int lingerMs;
    private final Compression compression;

    private final AtomicBoolean available = new AtomicBoolean(true);
    private final AtomicInteger activeWrites = new AtomicInteger(0);
    private final CompletableFuture<MemqWriteResult> resultFuture = new CompletableFuture<>();
    private final int bufferCapacity;
    private final ByteBuf byteBuf;
    private OutputStream outputStream;
    private byte[] messageIdHash;
    private int messageCount;
    private MemqMessageHeader header = new MemqMessageHeader(this);
    public BufferedRequest(BufferedRequestManager bufferedRequestManager,
                           String topic,
                           int clientRequestId,
                           int maxPayloadSize,
                           int lingerMs,
                           Compression compression) throws IOException {
        this.bufferedRequestManager = bufferedRequestManager;
        this.topic = topic;
        this.clientRequestId = clientRequestId;
        this.maxRequestSize = maxPayloadSize;
        this.lingerMs = lingerMs;
        this.compression = compression;
        this.bufferCapacity = getByteBufCapacity(maxRequestSize, compression);
        this.byteBuf = PooledByteBufAllocator.DEFAULT.buffer(bufferCapacity, bufferCapacity);
        try {
            initializeOutputStream();
        } catch (IOException ioe) {
            // release bytebuf if exception happened to avoid bytebuf leaks
            this.byteBuf.release();
            throw ioe;
        }
    }

    public int getAllocatedCapacity() {
        return bufferCapacity;
    }

    public Future<MemqWriteResult> getResultFuture() {
        return resultFuture;
    }

    private void initializeOutputStream() throws IOException {
        OutputStream stream = new ByteBufOutputStream(this.byteBuf);
        int headerLength = MemqMessageHeader.getHeaderLength();
        stream.write(new byte[headerLength]);
        if (compression != null) {
            outputStream = compression.getDecompressStream(stream);
        } else {
            outputStream = Compression.NONE.getDecompressStream(stream);
        }
    }

    private int getByteBufCapacity(int maxRequestSize, Compression compression) {
        return Math.max(maxRequestSize, compression.minBufferSize);
    }

    public Future<MemqWriteResult> write(RawRecord record) throws IOException {
        activeWrites.getAndIncrement();
        try {

            // synchronized to ensure bytebuf doesn't get out-of-order writes
            synchronized (byteBuf) {
                try {
                    writeMemqLogMessage(record);
                } finally {
                    record.recycle();
                }
            }

            if (lingerMs == 0) {
                seal();
            }

            return resultFuture;
        } finally {
            activeWrites.decrementAndGet();
        }
    }

    // true if there are no active writes on this request
    protected boolean isReadyToUpload() {
        return activeWrites.get() == 0;
    }

    public boolean isAvailable() {
        return available.get();
    }

    public boolean isWritable(RawRecord record) {
        return record.calculateEncodedLogMessageLength() <= byteBuf.writableBytes();
    }

    /**
     *
     * @return true if sealed by this call
     */
    public boolean seal() {
        return available.getAndSet(false);
    }

    public void writeMemqLogMessage(RawRecord record) throws IOException {
        record.writeToOutputStream(outputStream);
        // record the messageId
        addMessageId(record.getMessageIdBytes());
        messageCount++;
    }

    protected void addMessageId(byte[] messageIdBytes) {
        if (messageIdBytes == null) {
            return;
        }
        messageIdHash = MemqUtils.calculateMessageIdHash(messageIdHash, messageIdBytes);
    }

    public short getVersion() {
        return 1_0_0;
    }

    public Compression getCompression() {
        return compression;
    }

    public int getMessageCount() {
        return messageCount;
    }

    public long getEpoch() {
        return bufferedRequestManager.getProducer() != null ? bufferedRequestManager.getProducer().getEpoch() : System.currentTimeMillis();
    }

    public int getClientRequestId() {
        return clientRequestId;
    }
}

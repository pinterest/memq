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
import com.pinterest.memq.client.commons.MemqMessageHeader2;
import com.pinterest.memq.client.commons2.MemqPooledByteBufAllocator;
import com.pinterest.memq.client.producer.MemqWriteResult;
import com.pinterest.memq.commons.protocol.RequestPacket;
import com.pinterest.memq.commons.protocol.RequestType;
import com.pinterest.memq.commons.protocol.WriteRequestPacket;
import com.pinterest.memq.core.utils.MemqUtils;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.CRC32;

public class BufferedRequest {
    private static final Logger logger = LoggerFactory.getLogger(BufferedRequest.class);
    private final long epoch;
    private final String topic;
    private final int clientRequestId;
    private final int maxRequestSize;
    private final int lingerMs;
    private final Compression compression;
    private final AtomicBoolean isInitialized = new AtomicBoolean(false);
    private final AtomicBoolean isSealed = new AtomicBoolean(false);
    private final AtomicInteger activeWrites = new AtomicInteger(0);
    private final CompletableFuture<MemqWriteResult> resultFuture = new CompletableFuture<>();
    private final int bufferCapacity;
    private final MemqMessageHeader2 header = new MemqMessageHeader2(this);
    private ByteBuf byteBuf;
    private final boolean disableAcks;
    private OutputStream outputStream;
    private byte[] messageIdHash;
    private int messageCount;
    private volatile long startTime;
    private RequestPacket writeRequestPacket = null;
    public BufferedRequest(long epoch,
                           String topic,
                           int clientRequestId,
                           int maxPayloadSize,
                           int lingerMs,
                           boolean disableAcks,
                           Compression compression) {
        this.epoch = epoch;
        this.topic = topic;
        this.clientRequestId = clientRequestId;
        this.maxRequestSize = maxPayloadSize;
        this.lingerMs = lingerMs;
        this.disableAcks = disableAcks;
        this.compression = compression;
        this.bufferCapacity = getRequestCapacity(maxRequestSize, compression);
    }

    public Future<MemqWriteResult> getResultFuture() {
        return resultFuture;
    }

    public int getBufferCapacity() {
        return bufferCapacity;
    }

    /**
     * Allocate and initialize the bytebuf and outputstream. This method should be called only once.
     *
     * If the allocation of the ByteBuf fails with an OutOfMemoryError, this method will retry for maxBlockMs
     * milliseconds before throwing an IOException.
     *
     * @param maxBlockMs the maximum time to block while waiting for the ByteBuf allocation to succeed
     * @throws IOException if the ByteBuf allocation fails after maxBlockMs
     */
    public void allocateAndInitialize(long maxBlockMs) throws IOException {
        if (isInitialized.get()) {
            throw new IllegalStateException("BufferedRequest is already initialized");
        }
        startTime = System.currentTimeMillis();
        try {
            this.byteBuf = MemqPooledByteBufAllocator.buffer(bufferCapacity, bufferCapacity, maxBlockMs);
            initializeOutputStream();
            isInitialized.set(true);
        } catch (IOException ioe) {
            // release bytebuf if exception happened to avoid bytebuf leaks
            if (this.byteBuf != null) {
                this.byteBuf.release();
            }
            throw ioe;
        }
    }

    protected static int getRequestCapacity(int maxRequestSize, Compression compression) {
        return Math.max(maxRequestSize, compression.minBufferSize);
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

    public Future<MemqWriteResult> write(RawRecord record) throws IOException {
        if (!isInitialized.get()) {
            throw new IllegalStateException("BufferedRequest is not initialized");
        }
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
                sealRequest();
            }

            return resultFuture;
        } finally {
            activeWrites.decrementAndGet();
        }
    }

    protected boolean hasActiveWrites() {
        return activeWrites.get() != 0;
    }

    public boolean isSealed() {
        return isSealed.get();
    }

    public boolean isWritable(RawRecord record) {
        return record.calculateEncodedLogMessageLength() <= byteBuf.writableBytes();
    }

    public boolean sealRequest() {
        if (isSealed.get()) {
            return true;
        }
        try {
            outputStream.close();
        } catch (IOException e) {
            logger.warn("Failed to close output stream: ", e);
        }
        isSealed.set(true);
        return isSealed.get();
    }

    public void writeMemqLogMessage(RawRecord record) throws IOException {
        record.writeToOutputStream(outputStream);
        // record the messageId
        addMessageId(record.getMessageIdBytes());
        messageCount++;
    }

    public RequestPacket getOrCreateWriteRequestPacket() {
        if (!isSealed.get()) {
            throw new IllegalStateException("BufferedRequest is not sealed");
        }
        if (writeRequestPacket != null) {
            return writeRequestPacket;
        }
        try {
            header.writeHeader(byteBuf);
            int payloadSizeBytes = byteBuf.readableBytes();
            if (payloadSizeBytes == 0) { // don't upload 0 byte payloads
                resultFuture.complete(new MemqWriteResult(clientRequestId, 0, 0, 0));
                return null;
            }
            writeRequestPacket = createWriteRequestPacket(byteBuf.asReadOnly().retainedDuplicate());
            return writeRequestPacket;
        } finally {
            byteBuf.release();
        }
    }

    private RequestPacket createWriteRequestPacket(ByteBuf payload) {
        CRC32 crc32 = new CRC32();
        crc32.update(payload.duplicate().nioBuffer());
        int checksum = (int) crc32.getValue();

        // this payload needs to be released by the Dispatcher once done
        WriteRequestPacket writeRequestPacket = new WriteRequestPacket(disableAcks,
                topic.getBytes(), true, checksum, payload.duplicate());
        return new RequestPacket(RequestType.PROTOCOL_VERSION, clientRequestId, RequestType.WRITE,
                writeRequestPacket);
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

    public String getTopic() {
        return topic;
    }

    public int getMaxRequestSize() {
        return maxRequestSize;
    }

    public int getLingerMs() {
        return lingerMs;
    }

    public int getMessageCount() {
        return messageCount;
    }

    public long getEpoch() {
        return epoch;
    }

    public int getClientRequestId() {
        return clientRequestId;
    }

    public boolean isDisableAcks() {
        return disableAcks;
    }

    public boolean maybeTimeThresholdSeal() {
        if (hasActiveWrites()) {
            return false;
        }
        if (System.currentTimeMillis() - startTime > lingerMs) {
            return sealRequest();
        }
        return false;
    }

    protected void resolve(MemqWriteResult writeResult) {
        resultFuture.complete(writeResult);
    }

    protected void resolve(Throwable e) {
        resultFuture.completeExceptionally(e);
    }
}
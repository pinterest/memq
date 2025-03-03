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
import com.pinterest.memq.client.commons2.MemqCommonClient;
import com.pinterest.memq.client.commons2.retry.RetryStrategy;
import com.pinterest.memq.client.producer.MemqWriteResult;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class BufferedRequestManager implements Closeable {
    private final MemqCommonClient client;
    private final String topic;
    private final MemqProducer<?, ?> producer;
    private final long sendRequestTimeout;
    private final int maxPayloadBytes;
    private final int lingerMs;
//    private final int maxInflightRequests;
    private final MemoryBoundRequestBuffer requestBuffer;
    private final Compression compression;
    private final boolean disableAcks;
    private final RetryStrategy retryStrategy;

    private final AtomicInteger requestIdGenerator = new AtomicInteger(0);
    private final MetricRegistry metricRegistry;
    private Counter requestCounter;
    private volatile BufferedRequest currentRequest;

    public BufferedRequestManager(MemqCommonClient client,
                                  String topic,
                                  MemqProducer<?, ?> producer,
                                  long sendRequestTimeout,
                                  RetryStrategy retryStrategy,
                                  int maxPayloadBytes,
                                  int lingerMs,
                                  int maxInflightRequests,
                                  long maxBufferMemoryBytes,
                                  long maxBlockMs,
                                  Compression compression,
                                  boolean disableAcks,
                                  MetricRegistry metricRegistry) {
        this.client = client;
        this.topic = topic;
        this.producer = producer;
        this.sendRequestTimeout = sendRequestTimeout;
        this.maxPayloadBytes = maxPayloadBytes;
        this.lingerMs = lingerMs;
        this.requestBuffer = new MemoryBoundRequestBuffer(maxBufferMemoryBytes, maxBlockMs);
        this.compression = compression;
        this.disableAcks = disableAcks;
        this.retryStrategy = retryStrategy;
        this.metricRegistry = metricRegistry;
        initializeMetrics();
    }

    private void initializeMetrics() {
        requestCounter = metricRegistry.counter("requests.created");
//        metricRegistry.gauge("requests.inflight", () -> () -> maxInflightRequests - maxInflightRequestLock.availablePermits());
    }

    public Future<MemqWriteResult> write(RawRecord record) throws IOException, InterruptedException, TimeoutException {
        if (client.isClosed()) {
            throw new IOException("Cannot write to topic " + topic + " when client is closed");
        }
        if (currentRequest == null) {
            currentRequest = createNewRequestAndAddToBuffer();
        } else if (!currentRequest.isAvailable() || !currentRequest.isWritable(record)) {
            currentRequest.seal();
            currentRequest = createNewRequestAndAddToBuffer();
        }
        return currentRequest.write(record);
    }

    private BufferedRequest createNewRequestAndAddToBuffer() throws IOException, InterruptedException, TimeoutException {
        BufferedRequest newRequest = new BufferedRequest(
                this,
                topic,
                requestIdGenerator.getAndIncrement(),
                maxPayloadBytes,
                lingerMs,
                compression);
        requestCounter.inc();
        requestBuffer.add(newRequest);
        return newRequest;
    }

    public MemqProducer<?, ?> getProducer() {
        return producer;
    }


    @Override
    public void close() throws IOException {

    }

    protected class MemoryBoundRequestBuffer {

        private final long maxMemoryBytes;
        private final AtomicLong currentMemoryBytes = new AtomicLong(0);
        private final long maxBlockMs;
        private final ConcurrentLinkedQueue<BufferedRequest> buffer = new ConcurrentLinkedQueue<>();

        public MemoryBoundRequestBuffer(long maxMemoryBytes, long maxBlockMs) {
            this.maxMemoryBytes = maxMemoryBytes;
            this.maxBlockMs = maxBlockMs;
        }

        public Future<MemqWriteResult> add(BufferedRequest request) throws TimeoutException, InterruptedException {
            int allocatedCapacity = request.getAllocatedCapacity();
            long startTime = System.currentTimeMillis();
            while (currentMemoryBytes.get() + allocatedCapacity > maxMemoryBytes) {
                Thread.sleep(10);
                if (System.currentTimeMillis() - startTime > maxBlockMs) {
                    throw new TimeoutException("Failed to add record to buffer after " + maxBlockMs + " ms");
                }
            }
            buffer.add(request);
            currentMemoryBytes.addAndGet(allocatedCapacity);
            return request.getResultFuture();
        }

        public BufferedRequest poll() {
            BufferedRequest request = buffer.poll();
            if (request != null) {
                currentMemoryBytes.addAndGet(-request.getAllocatedCapacity());
            }
            return request;
        }

        public BufferedRequest peek() {
            return buffer.peek();
        }
    }
}

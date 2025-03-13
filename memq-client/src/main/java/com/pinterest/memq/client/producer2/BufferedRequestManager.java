package com.pinterest.memq.client.producer2;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import com.pinterest.memq.client.commons.Compression;
import com.pinterest.memq.client.commons.audit.Auditor;
import com.pinterest.memq.client.commons2.MemqCommonClient;
import com.pinterest.memq.client.commons2.retry.RetryStrategy;
import com.pinterest.memq.client.producer.MemqWriteResult;
import com.pinterest.memq.core.utils.MemqUtils;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

public class BufferedRequestManager implements Closeable {

    private final MemqCommonClient client;
    private final String topic;
    private final MemqProducer<?, ?> producer;
    private final int maxPayloadBytes;
    private final int lingerMs;
    private final Compression compression;
    private final boolean disableAcks;
    private final RetryStrategy retryStrategy;
    private final MetricRegistry metricRegistry;
    private final AtomicInteger requestIdGenerator = new AtomicInteger(0);
    private final Counter requestCounter = new Counter();
    private final RequestBuffer requestBuffer;
    private volatile BufferedRequest currentRequest;

    public BufferedRequestManager(MemqCommonClient client,
                                  String topic,
                                  MemqProducer<?, ?> producer,
                                  RequestBuffer requestBuffer,
                                  RetryStrategy retryStrategy,
                                  int maxPayloadBytes,
                                  int lingerMs,
                                  Compression compression,
                                  boolean disableAcks,
                                  MetricRegistry metricRegistry) {
        this.client = client;
        this.topic = topic;
        this.producer = producer;
        this.requestBuffer = requestBuffer;
        this.retryStrategy = retryStrategy;
        this.maxPayloadBytes = maxPayloadBytes;
        this.lingerMs = lingerMs;
        this.compression = compression;
        this.disableAcks = disableAcks;
        this.metricRegistry = metricRegistry;
    }

    public Future<MemqWriteResult> write(RawRecord record) throws IOException, InterruptedException, TimeoutException {
        if (client.isClosed()) {
            throw new IOException("Cannot write to topic " + topic + " when client is closed");
        }
        if (currentRequest == null || currentRequest.isSealed()) {
            // create a new request and add it to the buffer
            currentRequest = createNewRequestAndAddToBuffer();
        } else if (!currentRequest.isWritable(record)) {
            // seal the current request and create a new one
            boolean sealed = currentRequest.sealRequest();
            System.out.println("sealed request: " + currentRequest.getClientRequestId());
            if (!sealed) {
                throw new IOException("Failed to seal request");
            }
            currentRequest = createNewRequestAndAddToBuffer();
        }
        return currentRequest.write(record);
    }

    private BufferedRequest createNewRequestAndAddToBuffer() throws IOException, TimeoutException {
        // TimeoutException if buffer full, IOException if ByteBuf allocation fails
        BufferedRequest newRequest = requestBuffer.enqueueRequest(
                producer.getEpoch(),
                topic,
                requestIdGenerator.getAndIncrement(),
                maxPayloadBytes,
                lingerMs,
                disableAcks,
                compression,
                metricRegistry,
                null
        );
        requestCounter.inc();
        return newRequest;
    }

    public MemqProducer<?, ?> getProducer() {
        return producer;
    }

    public void flush() {
        if (currentRequest != null) {
            currentRequest.sealRequest();
        }
        // implement force flush
    }

    @Override
    public void close() throws IOException {
        flush();
    }
}

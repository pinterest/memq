package com.pinterest.memq.client.producer2;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import com.pinterest.memq.client.commons.Compression;
import com.pinterest.memq.client.commons.audit.Auditor;
import com.pinterest.memq.client.commons2.MemqCommonClient;
import com.pinterest.memq.client.commons2.retry.RetryStrategy;
import com.pinterest.memq.client.producer.MemqWriteResult;
import com.pinterest.memq.core.utils.MemqUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

public class BufferedRequestManager implements Closeable {
    private static final Logger logger = LoggerFactory.getLogger(BufferedRequestManager.class);
    private final MemqCommonClient client;
    private final String topic;
    private final MemqProducer<?, ?> producer;
    private final int maxPayloadBytes;
    private final int lingerMs;
    private final Compression compression;
    private final boolean disableAcks;
    private final MetricRegistry metricRegistry;
    private final AtomicInteger requestIdGenerator = new AtomicInteger(0);
    private Counter requestCounter;
    private final RequestBuffer requestBuffer;
    private final ScheduledThreadPoolExecutor scheduler;
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
        this.scheduler = new ScheduledThreadPoolExecutor(1);
        this.topic = topic;
        this.producer = producer;
        this.requestBuffer = requestBuffer;
        this.maxPayloadBytes = maxPayloadBytes;
        this.lingerMs = lingerMs;
        this.compression = compression;
        this.disableAcks = disableAcks;
        this.metricRegistry = metricRegistry;
        initializeMetrics();
    }

    private void initializeMetrics() {
        requestCounter = metricRegistry.counter("requests.created");
        metricRegistry.gauge("request.buffer.size.bytes", () -> requestBuffer::getCurrentSizeBytes);
        metricRegistry.gauge("request.buffer.size.count", () -> requestBuffer::getRequestCount);
    }

    public Future<MemqWriteResult> write(RawRecord record) throws IOException, InterruptedException, TimeoutException {
        if (client.isClosed()) {
            throw new IOException("Cannot write to topic " + topic + " when client is closed");
        }
        BufferedRequest request = getAvailableRequest();
        while (request != null) {
            Future<MemqWriteResult> returnFuture = request.write(record);
            if (returnFuture != null) {
                return returnFuture;
            } else {
                request = getAvailableRequest();
            }
        }
        return null;
    }

    private BufferedRequest getAvailableRequest() throws IOException, TimeoutException {
        if (currentRequest == null || currentRequest.isSealed()) {
            synchronized (this) {
                if (currentRequest == null || currentRequest.isSealed()) {
                    if (client.isClosed()) {
                        throw new IOException("Cannot write to topic " + topic + " when client is closed");
                    }
                    currentRequest = createNewRequestAndAddToBuffer();
                }
                return currentRequest;
            }
        }
        return currentRequest;
    }

    private BufferedRequest createNewRequestAndAddToBuffer() throws IOException, TimeoutException {
        // TimeoutException if buffer full, IOException if ByteBuf allocation fails
        BufferedRequest newRequest = requestBuffer.enqueueRequest(
                producer.getEpoch(),
                scheduler,
                topic,
                requestIdGenerator.get(),
                maxPayloadBytes,
                lingerMs,
                disableAcks,
                compression,
                metricRegistry,
                null
        );
        requestCounter.inc();
        requestIdGenerator.incrementAndGet();
        return newRequest;
    }

    public MemqProducer<?, ?> getProducer() {
        return producer;
    }

    public void flush() {
        if (currentRequest != null) {
            currentRequest.flush();
        }
    }

    @Override
    public void close() throws IOException {
        flush();
        scheduler.shutdown();
    }
}

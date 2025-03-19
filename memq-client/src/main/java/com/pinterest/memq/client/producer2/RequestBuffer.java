package com.pinterest.memq.client.producer2;

import com.codahale.metrics.MetricRegistry;
import com.google.common.annotations.VisibleForTesting;
import com.pinterest.memq.client.commons.Compression;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.time.Duration;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class RequestBuffer {
    private static final Logger logger = LoggerFactory.getLogger(RequestBuffer.class);
    private final long maxSizeBytes;
    private final AtomicLong currentSizeBytes = new AtomicLong(0);
    private final int maxBlockMs;
    private final ConcurrentHashMap<Integer, BufferedRequest> requests = new ConcurrentHashMap<>();
    private final BlockingQueue<Integer> queue = new LinkedBlockingQueue<>();
    private final Lock sizeReadLock = new ReentrantLock(true);
    private final Lock lock = new ReentrantLock(true);
    private final ScheduledThreadPoolExecutor retryScheduler;

    public RequestBuffer(long maxSizeBytes, int maxBlockMs) {
        this.maxSizeBytes = maxSizeBytes;
        this.maxBlockMs = maxBlockMs;
        this.retryScheduler = new ScheduledThreadPoolExecutor(1);
    }

    /**
     * Enqueue a request if one is provided. If no request is provided, a new request is created and enqueued.
     *
     * This method will block until there is enough space in the buffer,
     * or until the maxBlockMs timeout is reached. If the timeout is reached while waiting for buffer space,
     * a TimeoutException is thrown.
     *
     * If enough space is available in the buffer, the capacity required by the request is allocated and the request is
     * initialized. If at this point the request's buffer allocation fails (e.g. due to an OutOfMemoryError), the allocation
     * will be retried until the maxBlockMs timeout is reached.
     *
     * If the request is successfully allocated and initialized, it is added to the buffer and the buffer's size is updated.
     *
     * @throws TimeoutException if the request cannot be added to the buffer within maxBlockMs
     * @throws IOException if the request's buffer allocation fails within maxBlockMs
     */
    public BufferedRequest createAndAllocateNewRequest(long epoch,
                                                       ScheduledThreadPoolExecutor scheduler,
                                                       String topic,
                                                       int requestId,
                                                       int maxPayloadBytes,
                                                       int lingerMs,
                                                       boolean disableAcks,
                                                       Compression compression,
                                                       MetricRegistry metricRegistry,
                                                       @Nullable BufferedRequest request) throws TimeoutException, IOException {

        long startTime = System.currentTimeMillis();
        int requestCapacity = BufferedRequest.getRequestCapacity(maxPayloadBytes, compression);
        while (System.currentTimeMillis() - startTime < maxBlockMs) {
            if (sizeReadLock.tryLock()) {
                try {
                    // lock acquired, check buffer capacity
                    if (currentSizeBytes.get() + requestCapacity <= maxSizeBytes) {
                        if (request == null) {
                            // create and initialize a new request
                            request = createAndInitializeNewRequest(
                                    epoch,
                                    scheduler,
                                    topic,
                                    requestId,
                                    maxPayloadBytes,
                                    lingerMs,
                                    disableAcks,
                                    compression,
                                    maxBlockMs - (System.currentTimeMillis() - startTime),
                                    metricRegistry
                            );
                        }
                        return request;
                    }
                } finally {
                    sizeReadLock.unlock();
                }
            }
        }
        throw new TimeoutException("Failed to allocate " + requestCapacity + " bytes " +
                "for requestId=" + requestId + " within maxBlockMs=" + maxBlockMs + "ms. " +
                "Current buffer size: " + currentSizeBytes.get() + " bytes, " +
                "Max buffer size: " + maxSizeBytes + " bytes");
    }


    /**
     * Create and initialize a new request with the given parameters.
     *
     * @param epoch the epoch of the request
     * @param topic the topic of the request
     * @param requestId the client request ID of the request
     * @param maxPayloadBytes the maximum payload size of the request
     * @param lingerMs the linger time of the request
     * @param disableAcks whether acks are disabled for the request
     * @param compression the compression type of the request
     * @param timeout the maximum time to block while waiting for the ByteBuf allocation to succeed
     * @return the newly created and initialized request
     * @throws IOException if the ByteBuf allocation fails after maxBlockMs
     */
    private BufferedRequest createAndInitializeNewRequest(long epoch,
                                                          ScheduledThreadPoolExecutor scheduler,
                                                          String topic,
                                                          int requestId,
                                                          int maxPayloadBytes,
                                                          int lingerMs,
                                                          boolean disableAcks,
                                                          Compression compression,
                                                          long timeout,
                                                          MetricRegistry metricRegistry) throws IOException {
        BufferedRequest request = new BufferedRequest(
                epoch,
                scheduler,
                this,
                topic,
                requestId,
                maxPayloadBytes,
                lingerMs,
                disableAcks,
                compression,
                metricRegistry);
        request.allocateAndInitialize(timeout);   // throws IOException if allocation fails within blocking time
        return request;
    }

    public void enqueueRequest(BufferedRequest request) {
        lock.lock();
        try {
            requests.put(request.getClientRequestId(), request);
            queue.add(request.getClientRequestId());
            currentSizeBytes.getAndAdd(request.getCapacityBytes());
        } finally {
            lock.unlock();
        }
    }

    /**
     * Retry a request by re-enqueuing it in the buffer.
     *
     * @param request the request to retry
     * @throws IOException if the request's buffer allocation fails within maxBlockMs
     * @throws TimeoutException if the request cannot be re-enqueued within maxBlockMs
     */
    public void retryRequest(BufferedRequest request, Duration nextRetryIntervalDuration) throws IOException, TimeoutException {
        retryScheduler.schedule(() -> {
            lock.lock();
            try {
                if (queue.contains(request.getClientRequestId())) {
                    // request is already in the queue
                    return;
                }
                request.retry(nextRetryIntervalDuration);
                queue.add(request.getClientRequestId());
            } finally {
                lock.unlock();
            }
        }, nextRetryIntervalDuration.toMillis(), TimeUnit.MILLISECONDS);
    }

    /**
     * Get the next request that is ready for dispatch from the buffer, or null if no request is ready.
     *
     * @return the next request that is ready for dispatch, or null if no request is ready
     */
    public BufferedRequest getReadyRequestForDispatch() throws InterruptedException {
        int requestId = queue.take();
        lock.lock();
        try {
            BufferedRequest request = requests.get(requestId);
            if (request == null) {
                throw new IllegalStateException("Request " + requestId + " is not in the buffer");
            }
            return request;
        } finally {
            lock.unlock();
        }
    }

    public void removeRequest(int requestId) {
        lock.lock();
        try {
            if (queue.contains(requestId)) {
                throw new IllegalStateException("Cannot remove request " + requestId + " from the buffer while it is in the queue");
            }
            BufferedRequest request = requests.remove(requestId);
            if (request == null) {
                logger.warn("Request " + requestId + " is not in the buffer when attempting to remove it");
                return;
            }
            currentSizeBytes.addAndGet(-request.getCapacityBytes());
        } finally {
            lock.unlock();
        }
    }

    protected void flush() {
        long startTime = System.currentTimeMillis();
        int pendingRequests = requests.size();
        logger.info("Flushing " + pendingRequests + " requests from the buffer");
        while (!requests.isEmpty()) {
            // waiting to flush
        }
        logger.info("Flushed " + pendingRequests + " requests from the buffer in " + (System.currentTimeMillis() - startTime) + "ms");
    }

    @VisibleForTesting
    public long getCurrentSizeBytes() {
        return currentSizeBytes.get();
    }

    @VisibleForTesting
    public long getMaxSizeBytes() {
        return maxSizeBytes;
    }

    @VisibleForTesting
    public int getRequestCount() {
        return requests.size();
    }

    @VisibleForTesting
    public Set<Integer> getRequestIds() {
        return requests.keySet();
    }
}

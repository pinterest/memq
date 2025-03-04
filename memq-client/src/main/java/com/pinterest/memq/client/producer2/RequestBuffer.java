package com.pinterest.memq.client.producer2;

import com.pinterest.memq.client.commons.Compression;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class RequestBuffer {
    private final long maxSizeBytes;
    private final AtomicLong currentSizeBytes = new AtomicLong(0);
    private final int maxBlockMs;
    private final ConcurrentLinkedQueue<BufferedRequest> buffer = new ConcurrentLinkedQueue<>();
    private final Lock sizeReadLock = new ReentrantLock(true);
    private final Lock queueReadLock = new ReentrantLock(true);

    public RequestBuffer(long maxSizeBytes, int maxBlockMs) {
        this.maxSizeBytes = maxSizeBytes;
        this.maxBlockMs = maxBlockMs;
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
    public BufferedRequest enqueueRequest(long epoch,
                                          String topic,
                                          int requestId,
                                          int maxPayloadBytes,
                                          int lingerMs,
                                          boolean disableAcks,
                                          Compression compression,
                                          @Nullable BufferedRequest request) throws TimeoutException, IOException {

        long startTime = System.currentTimeMillis();
        int bufferCapacity = BufferedRequest.getRequestCapacity(maxPayloadBytes, compression);
        while (System.currentTimeMillis() - startTime < maxBlockMs) {
            if (sizeReadLock.tryLock()) {
                try {
                    // lock acquired, check buffer capacity
                    if (currentSizeBytes.get() + bufferCapacity <= maxSizeBytes) {
                        if (request == null) {
                            // create and initialize a new request
                            request = createAndInitializeNewRequest(
                                    epoch,
                                    topic,
                                    requestId,
                                    maxPayloadBytes,
                                    lingerMs,
                                    disableAcks,
                                    compression,
                                    maxBlockMs - (System.currentTimeMillis() - startTime)
                            );
                        }
                        buffer.add(request);
                        currentSizeBytes.addAndGet(bufferCapacity);
                        return request;
                    }
                } finally {
                    sizeReadLock.unlock();
                }
            }
        }
        throw new TimeoutException("Failed to allocate buffer for request within " + maxBlockMs + "ms");
    }

    /**
     * Retry a request by re-enqueuing it in the buffer.
     *
     * @param request the request to retry
     * @return the re-enqueued request
     * @throws IOException if the request's buffer allocation fails within maxBlockMs
     * @throws TimeoutException if the request cannot be re-enqueued within maxBlockMs
     */
    public BufferedRequest retryRequest(BufferedRequest request) throws IOException, TimeoutException {
        return enqueueRequest(
                request.getEpoch(),
                request.getTopic(),
                request.getClientRequestId(),
                request.getMaxRequestSize(),
                request.getLingerMs(),
                request.isDisableAcks(),
                request.getCompression(),
                request
        );
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
                                                          String topic,
                                                          int requestId,
                                                          int maxPayloadBytes,
                                                          int lingerMs,
                                                          boolean disableAcks,
                                                          Compression compression,
                                                          long timeout) throws IOException {
        BufferedRequest request = new BufferedRequest(
                epoch,
                topic,
                requestId,
                maxPayloadBytes,
                lingerMs,
                disableAcks,
                compression);
        request.allocateAndInitialize(timeout);   // throws IOException if allocation fails within blocking time
        return request;
    }

    /**
     * Get the next request that is ready for dispatch from the buffer, or null if no request is ready.
     *
     * @return the next request that is ready for dispatch, or null if no request is ready
     */
    public BufferedRequest getReadyRequestForDispatch() {
        if (!queueReadLock.tryLock()) {
            throw new IllegalStateException("Unexpected contention on buffer read lock. Only one thread (the RequestDispatcher) should be reading from the buffer.");
        }
        try {
            BufferedRequest request = buffer.peek();
            if (request != null) {
                if (isRequestReadyForDispatch(request)) {
                    currentSizeBytes.addAndGet(-request.getBufferCapacity());
                    return buffer.poll();
                }
            }
        } finally {
            queueReadLock.unlock();
        }
        return null;
    }

    /**
     * Check if a request is ready to be dispatched. A request is ready if it is sealed and has no active writes,
     * or if it has reached linger.ms since creation and there are no active writes.
     *
     * If linger.ms has been reached, but there are active writes, the request is not ready for dispatch and
     * the method will return false.
     *
     * @param request
     * @return true if the request is ready for dispatch, false otherwise
     */
    private boolean isRequestReadyForDispatch(BufferedRequest request) {
        return (request.isSealed() && !request.hasActiveWrites()) || request.maybeTimeThresholdSeal();
    }
}

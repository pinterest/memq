package com.pinterest.memq.client.producer2;

import com.pinterest.memq.client.commons2.MemqCommonClient;
import com.pinterest.memq.client.commons2.network.ClosedConnectionException;
import com.pinterest.memq.commons.protocol.RequestPacket;
import com.pinterest.memq.commons.protocol.ResponsePacket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

public class BufferedRequestDispatcher implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(BufferedRequestDispatcher.class);
    private final MemqCommonClient client;
    private final RequestBuffer requestBuffer;
    private final AtomicBoolean running;
    private final long dispatchTimeoutMs;
    private final Semaphore maxInflightRequestSemaphore;
    private final int maxBlockMs;

    public BufferedRequestDispatcher(MemqCommonClient client,
                                     RequestBuffer requestBuffer,
                                     int maxBlockMs,
                                     long dispatchTimeoutMs,
                                     int maxInflightRequests) {
        this.client = client;
        this.requestBuffer = requestBuffer;
        this.maxBlockMs = maxBlockMs;
        this.running = new AtomicBoolean(true);
        this.dispatchTimeoutMs = dispatchTimeoutMs;     // max time for client dispatch
        this.maxInflightRequestSemaphore = new Semaphore(maxInflightRequests);
    }


    @Override
    public void run() {
        while (running.get()) {
            if (acquireInflightRequestPermit()) {
                final BufferedRequest request = requestBuffer.getReadyRequestForDispatch();
                if (request == null) {
                    // no request is ready yet, proceed to next iteration
                    continue;
                }
                try {
                    RequestPacket requestPacket = request.getOrCreateWriteRequestPacket();  // when should this be released?
                    CompletableFuture<ResponsePacket> responsePacketFuture;
                    try {
                        responsePacketFuture = client.sendRequestPacketAndReturnResponseFuture(requestPacket, dispatchTimeoutMs);
                    } catch (Exception e) {
                        // complete the future exceptionally if the request fails
                        logger.error("Failed to send request " + request.getClientRequestId(), e);
                        tryRelease(requestPacket);
                        request.resolve(e);
                        continue;   // continue with the next iteration
                    }
                    responsePacketFuture.whenCompleteAsync((responsePacket, throwable) -> {
                        if (throwable != null) {
                            handleResponsePacketFutureException(request, throwable);
                        } else {
                            handleResponse(request, responsePacket);
                        }
                    });
                } catch (Exception e) {
                    logger.error("Unexpected exception in request dispatcher during inflight request processing", e);
                    continue;
                } finally {
                    maxInflightRequestSemaphore.release();
                }
            }
            // otherwise, max inflight requests reached, wait for the next iteration
        }
    }


    private void handleResponsePacketFutureException(BufferedRequest request, Throwable throwable) {
        if (throwable instanceof ClosedConnectionException) {
            // handle closed connection

        } else {
            // handle other exceptions
        }
    }

    private void handleResponse(BufferedRequest request, ResponsePacket responsePacket) {
        // handle response
        if (responsePacket == null) {
            request.resolve(new Exception("Response packet is null"));
            return;
        }


    }

    private static void tryRelease(RequestPacket requestPacket) {
        try {
            requestPacket.release();
        } catch (IOException ex) {
            logger.warn("Failed to release request packet", ex);
        }
    }

    private boolean acquireInflightRequestPermit() {
        try {
            return maxInflightRequestSemaphore.tryAcquire(maxBlockMs, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            logger.warn("Interrupted while waiting for inflight request semaphore", e);
        }
        return false;
    }

    private void retryRequest(BufferedRequest request) throws IOException, TimeoutException {
        requestBuffer.retryRequest(request);
    }
}

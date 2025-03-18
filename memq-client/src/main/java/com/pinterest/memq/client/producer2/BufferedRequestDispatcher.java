package com.pinterest.memq.client.producer2;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.pinterest.memq.client.commons.audit.Auditor;
import com.pinterest.memq.client.commons2.MemqCommonClient;
import com.pinterest.memq.client.commons2.network.ClosedConnectionException;
import com.pinterest.memq.client.commons2.retry.RetryStrategy;
import com.pinterest.memq.client.producer.MemqWriteResult;
import com.pinterest.memq.commons.protocol.RequestPacket;
import com.pinterest.memq.commons.protocol.ResponseCodes;
import com.pinterest.memq.commons.protocol.ResponsePacket;
import com.pinterest.memq.core.utils.MemqUtils;
import com.pinterest.memq.core.utils.MiscUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
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
    private final RetryStrategy retryStrategy;
    private final MemqProducer<?, ?> producer;
    private Counter sentBytesCounter;
    private Counter ackedBytesCounter;
    private Timer sendTimer;
    private Timer dispatchTimer;
    private Counter successCounter;


    public BufferedRequestDispatcher(MemqCommonClient client,
                                     RequestBuffer requestBuffer,
                                     MemqProducer<?, ?> producer,
                                     int maxBlockMs,
                                     long dispatchTimeoutMs,
                                     int maxInflightRequests,
                                     RetryStrategy retryStrategy,
                                     MetricRegistry metricRegistry) {
        this.client = client;
        this.requestBuffer = requestBuffer;
        this.producer = producer;
        this.maxBlockMs = maxBlockMs;
        this.running = new AtomicBoolean(true);
        this.dispatchTimeoutMs = dispatchTimeoutMs;     // max time for client dispatch
        this.maxInflightRequestSemaphore = new Semaphore(maxInflightRequests);
        this.retryStrategy = retryStrategy;
        initializeMetrics(metricRegistry);
    }

    private void initializeMetrics(MetricRegistry metricRegistry) {
        sentBytesCounter = metricRegistry.counter("requests.sent.bytes");
        ackedBytesCounter = metricRegistry.counter("requests.acked.bytes");
        successCounter = metricRegistry.counter("requests.success.count");
        sendTimer = MiscUtils.oneMinuteWindowTimer(metricRegistry, "requests.send.time");
        dispatchTimer = MiscUtils.oneMinuteWindowTimer(metricRegistry, "requests.dispatch.time");
    }


    @Override
    public void run() {
        while (running.get()) {
            if (acquireInflightRequestPermit()) {
                final BufferedRequest request = requestBuffer.getReadyRequestForDispatch();
                if (request == null) {
                    // no request is ready yet, proceed to next iteration
                    maxInflightRequestSemaphore.release();
                    continue;
                }
                try {
                    RequestPacket requestPacket = request.getOrCreateWriteRequestPacket();  // when should this be released?
                    CompletableFuture<ResponsePacket> responsePacketFuture;
                    sentBytesCounter.inc(request.getActualPayloadSizeBytes());
                    Timer.Context dispatchTime = dispatchTimer.time();
                    request.setDispatchTimeMs(System.currentTimeMillis());
                    long writeTimestamp = System.currentTimeMillis();
                    Timer.Context sendTime = sendTimer.time();
                    int writeLatency;
                    try {
                        responsePacketFuture = client.sendRequestPacketAndReturnResponseFuture(requestPacket, dispatchTimeoutMs);
                        sendTime.stop();
                        writeLatency = (int) (System.currentTimeMillis() - writeTimestamp);
                    } catch (Exception e) {
                        // complete the future exceptionally if the request fails
                        logger.error("Failed to send request " + request.getClientRequestId(), e);
                        cleanup(request, e);
                        maxInflightRequestSemaphore.release();
                        continue;   // continue with the next iteration
                    } finally {
                        dispatchTime.stop();
                    }
                    responsePacketFuture.whenCompleteAsync((responsePacket, throwable) -> {
                        if (throwable != null) {
                            handleResponsePacketFutureException(request, responsePacket, throwable);
                        } else {
                            handleResponse(request, responsePacket, writeTimestamp, writeLatency);
                        }
                        maxInflightRequestSemaphore.release();
                    });
                } catch (Exception e) {
                    logger.error("Unexpected exception in request dispatcher during inflight request processing", e);
                    maxInflightRequestSemaphore.release();
                }
            }
            // otherwise, max inflight requests reached, wait for the next iteration
        }
    }

    private void handleResponsePacketFutureException(BufferedRequest request, ResponsePacket responsePacket, Throwable throwable) {
        if (throwable instanceof ClosedConnectionException) {
            // handle closed connection
            maybeRetryRequest(request, responsePacket, throwable, Integer.MAX_VALUE);
        } else if (throwable instanceof Exception){
            // handle other exceptions
            Exception resultException = (Exception) throwable;
            while (resultException instanceof ExecutionException && resultException.getCause() instanceof Exception) {
                resultException = (Exception) resultException.getCause();
            }
            logger.error("Failed to send request " + request.getClientRequestId(), resultException);
            cleanupResponseError(request, responsePacket, resultException);
        } else {
            logger.error("Failed to send request " + request.getClientRequestId(), throwable);
            cleanupResponseError(request, responsePacket, throwable);
        }
    }

    private void maybeRetryRequest(BufferedRequest request, ResponsePacket responsePacket, Throwable throwable, int customMaxRetryLimit) {
        Duration nextRetryIntervalDuration = retryStrategy.calculateNextRetryInterval(request.getRetries());
        if (nextRetryIntervalDuration == null || request.getDispatchTimeMs() + dispatchTimeoutMs <= System.currentTimeMillis() + nextRetryIntervalDuration.toMillis()) {
            cleanupResponseError(request, responsePacket, new TimeoutException("Request " + request.getClientRequestId() + " timed out after " + dispatchTimeoutMs + " ms and " + request.getRetries() + " retries : " + throwable.getMessage()));
        } else if (request.getRetries() >= customMaxRetryLimit) {
            cleanupResponseError(request, responsePacket, new Exception("Request " + request.getClientRequestId() + " failed after maximum " + customMaxRetryLimit + " retries: " + throwable.getMessage()));
        } else {
            logger.warn(throwable.getMessage() + ", retrying request " + request.getClientRequestId() + " after " + nextRetryIntervalDuration.toMillis() + " ms");
            try {
                requestBuffer.retryRequest(request, nextRetryIntervalDuration);
            } catch (IOException | TimeoutException e) {
                // retry failed due to buffer full or allocation failure
                cleanupResponseError(request, responsePacket, e);
            }
        }
    }

    private void handleResponse(BufferedRequest request, ResponsePacket responsePacket, long writeTimestamp, int writeLatency) {
        // handle response
        if (responsePacket == null) {
            cleanupResponseError(request, responsePacket, new Exception("Response packet is null"));
            return;
        }
        short responseCode = responsePacket.getResponseCode();
        switch (responseCode) {
            case ResponseCodes.OK:
                ackedBytesCounter.inc(request.getActualPayloadSizeBytes());
                sendAuditMessageIfAuditEnabled(request);
                int ackLatency = (int) (System.currentTimeMillis() - writeTimestamp);
                logger.debug("Request " + request.getClientRequestId() + " acked in:" + ackLatency + "ms");
                cleanupResponseSuccess(request, writeLatency, ackLatency);
                break;
            case ResponseCodes.REDIRECT:
                try {
                    client.reconnect(request.getTopic(), false);
                } catch (Exception e) {
                    cleanupResponseError(request, responsePacket, e);
                    return;
                }
                maybeRetryRequest(request, responsePacket, new Exception("Redirected to another server"), 2);
                break;
            case ResponseCodes.BAD_REQUEST:
                cleanupResponseError(request, responsePacket, new Exception("Bad request, id: " + request.getClientRequestId()));
                break;
            case ResponseCodes.NOT_FOUND:
                cleanupResponseError(request, responsePacket, new Exception("Topic not found: " + request.getTopic()));
                break;
            case ResponseCodes.INTERNAL_SERVER_ERROR:
                cleanupResponseError(request, responsePacket, new Exception("Unknown server error: " + request.getClientRequestId()));
                break;
            case ResponseCodes.REQUEST_FAILED:
                cleanupResponseError(request, responsePacket, new Exception("Request failed: " + request.getClientRequestId()));
                break;
            case ResponseCodes.SERVICE_UNAVAILABLE:
                cleanupResponseError(request, responsePacket, new Exception("Server out of capacity: " + request.getTopic()));
                break;
            default:
                cleanupResponseError(request, responsePacket, new Exception("Unknown response code: " + responseCode));
                break;
        }

    }


    private void cleanupResponseSuccess(BufferedRequest request, int writeLatency, int ackLatency) {
        successCounter.inc();
        request.resolveAndRelease(new MemqWriteResult(request.getClientRequestId(), writeLatency, ackLatency, (int) request.getActualPayloadSizeBytes()));
        requestBuffer.removeRequest(request);
    }

    private void cleanupResponseError(BufferedRequest request, ResponsePacket responsePacket, Throwable throwable) {
        cleanup(request, throwable);
        tryRelease(responsePacket);
    }

    private void cleanup(BufferedRequest request, Throwable throwable) {
        request.resolveAndRelease(throwable);
        requestBuffer.removeRequest(request);
    }

    private static void tryRelease(@Nullable ResponsePacket responsePacket) {
        try {
            if (responsePacket != null)
                responsePacket.release();
        } catch (IOException ex) {
            logger.warn("Failed to release response packet", ex);
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

    private void sendAuditMessageIfAuditEnabled(BufferedRequest request) {
        Auditor auditor = producer.getAuditor();
        if (auditor != null) {
            try {
                auditor.auditMessage(producer.getCluster().getBytes(MemqUtils.CHARSET),
                        request.getTopic().getBytes(MemqUtils.CHARSET), MemqUtils.HOST_IPV4_ADDRESS,
                        request.getEpoch(), request.getClientRequestId(), request.getMessageIdHash(), request.getMessageCount(), true, "producer");
            } catch (IOException e) {
                logger.error("Failed to log audit record for topic:" + request.getTopic(), e);
            }
        }
    }

    protected int getAvailablePermits() {
        return maxInflightRequestSemaphore.availablePermits();
    }

    public void flush() {
        int pendingRequests = requestBuffer.getRequestCount();
        logger.info("Flushing " + pendingRequests + " pending requests in buffer");
        long startTime = System.currentTimeMillis();
        while (requestBuffer.getRequestCount() > 0) {
            // busy wait while flushing
        }
        logger.info("Flushed " + pendingRequests + " requests in buffer in " + (System.currentTimeMillis() - startTime) + " ms");
    }

    public void close() {
        flush();
        running.set(false);
        int permitsDrained = maxInflightRequestSemaphore.drainPermits();
        maxInflightRequestSemaphore.release(permitsDrained);
    }
}

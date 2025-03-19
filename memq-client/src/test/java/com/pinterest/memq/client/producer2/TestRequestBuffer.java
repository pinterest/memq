package com.pinterest.memq.client.producer2;

import com.pinterest.memq.client.commons.Compression;
import org.junit.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeoutException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TestRequestBuffer {

    private static final ScheduledThreadPoolExecutor scheduler = new ScheduledThreadPoolExecutor(1);

    @Test
    public void testSimpleEnqueueDequeue() throws IOException, TimeoutException, InterruptedException {
        RequestBuffer buffer = new RequestBuffer(8192, 1000);
        Semaphore semaphore = new Semaphore(100);
        enqueueRequests(buffer, 0, 4, 1024, 0);

        Thread.sleep(2);  // wait for linger to expire
        assertEquals(4 * 1024, buffer.getCurrentSizeBytes());
        assertEquals(4, buffer.getRequestCount());

        BufferedRequest readyRequest = buffer.getReadyRequestForDispatch();
        assertEquals(0, readyRequest.getClientRequestId());
        readyRequest = buffer.getReadyRequestForDispatch();
        assertEquals(1, readyRequest.getClientRequestId());
        readyRequest = buffer.getReadyRequestForDispatch();
        assertEquals(2, readyRequest.getClientRequestId());
        readyRequest = buffer.getReadyRequestForDispatch();
        assertEquals(3, readyRequest.getClientRequestId());

        enqueueRequests(buffer, 4, 1, 1024, 0);

        Thread.sleep(2);  // wait for linger to expire

        readyRequest = buffer.getReadyRequestForDispatch();
        assertEquals(4, readyRequest.getClientRequestId());

        assertEquals(5 * 1024, buffer.getCurrentSizeBytes());
        assertEquals(5, buffer.getRequestCount());

        buffer.removeRequest(4);
        assertEquals(4 * 1024, buffer.getCurrentSizeBytes());
        assertEquals(4, buffer.getRequestCount());

        buffer.removeRequest(0);
        assertEquals(3 * 1024, buffer.getCurrentSizeBytes());
        assertEquals(3, buffer.getRequestCount());

        buffer.removeRequest(1);
        assertEquals(2 * 1024, buffer.getCurrentSizeBytes());
        assertEquals(2, buffer.getRequestCount());

        buffer.removeRequest(2);
        assertEquals(1 * 1024, buffer.getCurrentSizeBytes());
        assertEquals(1, buffer.getRequestCount());

        buffer.removeRequest(3);
        assertEquals(0, buffer.getCurrentSizeBytes());
        assertEquals(0, buffer.getRequestCount());

        semaphore.release(semaphore.drainPermits());
    }

    @Test
    public void testLinger() throws IOException, TimeoutException, InterruptedException {
        RequestBuffer buffer = new RequestBuffer(8192, 1000);
        Semaphore semaphore = new Semaphore(100);

        enqueueRequests(buffer, 0, 4, 1024, 500);  // 500ms linger

        Thread.sleep(600);  // wait for linger to expire

        assertEquals(4 * 1024, buffer.getCurrentSizeBytes());
        assertEquals(4, buffer.getRequestCount());

        // all requests should be ready now
        BufferedRequest readyRequest = buffer.getReadyRequestForDispatch();
        assertEquals(0, readyRequest.getClientRequestId());
        readyRequest = buffer.getReadyRequestForDispatch();
        assertEquals(1, readyRequest.getClientRequestId());
        readyRequest = buffer.getReadyRequestForDispatch();
        assertEquals(2, readyRequest.getClientRequestId());
        readyRequest = buffer.getReadyRequestForDispatch();
        assertEquals(3, readyRequest.getClientRequestId());

        buffer.removeRequest(0);
        buffer.removeRequest(1);
        buffer.removeRequest(2);
        buffer.removeRequest(3);

        assertEquals(0, buffer.getCurrentSizeBytes());
        assertEquals(0, buffer.getRequestCount());

        semaphore.release(semaphore.drainPermits());
    }

    @Test
    public void testMaxSize() throws IOException, TimeoutException, InterruptedException {
        RequestBuffer buffer = new RequestBuffer(8192, 1000);
        Semaphore semaphore = new Semaphore(100);

        enqueueRequests(buffer, 0, 8, 1024, 0);
        Thread.sleep(2);    // wait for linger to expire
        assertEquals(8 * 1024, buffer.getCurrentSizeBytes());
        assertEquals(8, buffer.getRequestCount());

        BufferedRequest readyRequest = buffer.getReadyRequestForDispatch();
        assertEquals(0, readyRequest.getClientRequestId());

        try {
            enqueueRequests(buffer, 8, 1, 1024, 0);
            fail("Should throw TimeoutException");
        } catch (Exception e) {
            assertEquals(TimeoutException.class, e.getClass());
            assertTrue(e.getMessage().contains("Failed to allocate"));
        }

        buffer.removeRequest(0);
        assertEquals(7 * 1024, buffer.getCurrentSizeBytes());
        assertEquals(7, buffer.getRequestCount());

        enqueueRequests(buffer, 8, 1, 1024, 0); // success
        Thread.sleep(2);    // wait for linger to expire
        assertEquals(8 * 1024, buffer.getCurrentSizeBytes());
        assertEquals(8, buffer.getRequestCount());
    }

    @Test
    public void testRetry() throws IOException, TimeoutException, InterruptedException {
        RequestBuffer buffer = new RequestBuffer(8192, 1000);
        Semaphore semaphore = new Semaphore(100);
        enqueueRequests(buffer, 0, 4, 1024, 0);

        Thread.sleep(2);  // wait for linger to expire

        BufferedRequest readyRequest = buffer.getReadyRequestForDispatch();
        assertEquals(0, readyRequest.getClientRequestId());
        buffer.removeRequest(0);

        BufferedRequest retryRequest = buffer.getReadyRequestForDispatch();
        assertEquals(1, retryRequest.getClientRequestId());
        buffer.retryRequest(retryRequest, Duration.ofMillis(0));

        readyRequest = buffer.getReadyRequestForDispatch();
        assertEquals(2, readyRequest.getClientRequestId());

        readyRequest = buffer.getReadyRequestForDispatch();
        assertEquals(3, readyRequest.getClientRequestId());

        readyRequest = buffer.getReadyRequestForDispatch();
        assertEquals(1, readyRequest.getClientRequestId());

        assertEquals(3, buffer.getRequestCount());

        buffer.removeRequest(2);
        buffer.removeRequest(3);

        buffer.retryRequest(readyRequest, Duration.ofMillis(100));
        assertEquals(1, buffer.getRequestCount());

        Thread.sleep(100);
        readyRequest = buffer.getReadyRequestForDispatch();
        assertEquals(1, readyRequest.getClientRequestId());

        assertEquals(1, buffer.getRequestCount());

        buffer.retryRequest(readyRequest, Duration.ofMillis(200));
        Thread.sleep(250);  // wait for retry deadline
        try {
            buffer.removeRequest(1);
            fail("Should throw IllegalStateException");
        } catch (Exception e) {
            assertEquals(IllegalStateException.class, e.getClass());
            assertTrue(e.getMessage().contains("Cannot remove request 1"));
        }

        Thread.sleep(200);
        readyRequest = buffer.getReadyRequestForDispatch();
        assertEquals(1, readyRequest.getClientRequestId());

        // idempotence of retry set prior to removal. this shouldn't add a new retry (even though this situation shouldn't happen)
        buffer.retryRequest(readyRequest, Duration.ofMillis(0));
        buffer.retryRequest(readyRequest, Duration.ofMillis(0));

        readyRequest = buffer.getReadyRequestForDispatch();
        assertEquals(1, readyRequest.getClientRequestId());;

        buffer.removeRequest(1);
        assertEquals(0, buffer.getRequestCount());

        semaphore.release(semaphore.drainPermits());
    }

    private static void enqueueRequests(RequestBuffer buffer, int startingRequestId, int numRequests, int maxPayloadBytes, int lingerMs) throws IOException, TimeoutException {
        for (int requestId = startingRequestId; requestId < startingRequestId + numRequests; requestId++) {
            BufferedRequest request = buffer.createAndAllocateNewRequest(0, scheduler, "topic", requestId, maxPayloadBytes, lingerMs, false, Compression.ZSTD, null, null);
        }
    }
}

package com.pinterest.memq.client.commons2;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A wrapper around Netty's PooledByteBufAllocator that provides a retry mechanism for memory allocation.
 *
 * The behavior is as follows:
 * 1. If an OutOfMemoryError occurs during buffer allocation, it will retry allocation up to a specified maximum block time.
 * 2. The retry interval starts at a base value and doubles with each retry, up to a maximum block time.
 * 3. If the allocation fails after all retries, a MemoryAllocationException is thrown.
 *
 * If maxBlockMs is set to 0, it will try once and throw an exception immediately if allocation fails.
 */
public class MemqPooledByteBufAllocator {
    private static final Logger logger = LoggerFactory.getLogger(MemqPooledByteBufAllocator.class);
    private static final long BASE_RETRY_INTERVAL_MS = 5;
    private static final int DEFAULT_MAX_BLOCK_MS = 0;

    public static ByteBuf buffer(int initialCapacity) throws MemoryAllocationException {
        return buffer(initialCapacity, Integer.MAX_VALUE);
    }

    public static ByteBuf buffer(int initialCapacity, int maxCapacity) throws MemoryAllocationException {
        return buffer(initialCapacity, maxCapacity, DEFAULT_MAX_BLOCK_MS);
    }

    public static ByteBuf buffer(int initialCapacity, int maxCapacity, int maxBlockMs) throws MemoryAllocationException {
        long retryIntervalMs = Math.max(BASE_RETRY_INTERVAL_MS, maxBlockMs / 100);
        long startTimeMs = System.currentTimeMillis();
        int tries = 0;
        while (tries == 0 || System.currentTimeMillis() - startTimeMs < maxBlockMs || maxBlockMs <= 0) {
            try {
                return PooledByteBufAllocator.DEFAULT.buffer(initialCapacity, maxCapacity);
            } catch (OutOfMemoryError oom) {
                if (maxBlockMs <= 0) {
                    break;
                }
                logger.trace("Not enough memory to allocate buffer with initialCapacity=" + initialCapacity + ", maxCapacity=" + maxCapacity + ", retrying in " + retryIntervalMs + "ms");
                retryIntervalMs = backoff(maxBlockMs, retryIntervalMs);
                tries++;
            }
        }
        throw new MemoryAllocationException("Failed to allocate buffer with initialCapacity=" + initialCapacity + ", maxCapacity=" + maxCapacity + " within " + maxBlockMs + "ms");
    }

    private static long backoff(int maxBlockMs, long retryIntervalMs) throws MemoryAllocationException {
        try {
            Thread.sleep(retryIntervalMs);
            retryIntervalMs = Math.max(retryIntervalMs * 2, maxBlockMs / 10);
        } catch (InterruptedException e) {
            throw new MemoryAllocationException("Interrupted while waiting to allocate buffer");
        }
        return retryIntervalMs;
    }

    public static long usedDirectMemory() {
        return PooledByteBufAllocator.DEFAULT.metric().usedDirectMemory();
    }

}

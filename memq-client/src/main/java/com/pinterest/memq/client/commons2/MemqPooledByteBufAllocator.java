package com.pinterest.memq.client.commons2;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class MemqPooledByteBufAllocator {
  private static final Logger logger = LoggerFactory.getLogger(MemqPooledByteBufAllocator.class);
  private static final PooledByteBufAllocator INSTANCE = PooledByteBufAllocator.DEFAULT;
  private static final long BASE_RETRY_INTERVAL_MS = 5;

  public static ByteBuf buffer() {
    return INSTANCE.buffer();
  }

  public static ByteBuf buffer(int initialCapacity) {
    return INSTANCE.buffer(initialCapacity);
  }

  public static ByteBuf buffer(int initialCapacity, int maxCapacity) {
    return INSTANCE.buffer(initialCapacity, maxCapacity);
  }

  public static ByteBuf buffer(int initialCapacity, int maxCapacity, long maxBlockMs) throws IOException {
    long startTimeMs = System.currentTimeMillis();
    long retryIntervalMs = Math.max(BASE_RETRY_INTERVAL_MS, maxBlockMs / 100);
    while (System.currentTimeMillis() - startTimeMs < maxBlockMs) {
        try {
            return INSTANCE.buffer(initialCapacity, maxCapacity);
        } catch (OutOfMemoryError oom) {
            logger.trace("Not enough memory to allocate buffer with initialCapacity=" + initialCapacity + ", maxCapacity=" + maxCapacity + ", retrying...");
            try {
                Thread.sleep(retryIntervalMs);
                retryIntervalMs = Math.max(retryIntervalMs * 2, maxBlockMs / 10);
            } catch (InterruptedException e) {
                // do nothing
            }
        }
    }
    throw new IOException("Failed to allocate buffer with initialCapacity=" + initialCapacity + ", maxCapacity=" + maxCapacity + " within " + maxBlockMs + "ms");
  }

  public static long usedDirectMemory() {
    return INSTANCE.metric().usedDirectMemory();
  }

}

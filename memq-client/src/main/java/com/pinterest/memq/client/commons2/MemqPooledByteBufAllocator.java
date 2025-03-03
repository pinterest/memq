package com.pinterest.memq.client.commons2;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class MemqPooledByteBufAllocator {
  private static final Logger logger = LoggerFactory.getLogger(MemqPooledByteBufAllocator.class);
  private static final PooledByteBufAllocator INSTANCE = PooledByteBufAllocator.DEFAULT;

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
    while (System.currentTimeMillis() - startTimeMs < maxBlockMs) {
        try {
            return INSTANCE.buffer(initialCapacity, maxCapacity);
        } catch (OutOfMemoryError oom) {
            logger.trace("Not enough memory to allocate buffer with initialCapacity=" + initialCapacity + ", maxCapacity=" + maxCapacity + ", retrying...");
        }
    }
    throw new IOException("Failed to allocate buffer with initialCapacity=" + initialCapacity + ", maxCapacity=" + maxCapacity + " within " + maxBlockMs + "ms");
  }

}

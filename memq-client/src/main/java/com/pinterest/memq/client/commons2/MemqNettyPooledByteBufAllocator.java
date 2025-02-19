package com.pinterest.memq.client.commons2;

import io.netty.buffer.PooledByteBufAllocator;

public class MemqNettyPooledByteBufAllocator {

    private static final PooledByteBufAllocator singletonAllocator = PooledByteBufAllocator.DEFAULT;

    public static PooledByteBufAllocator getAllocator() {
        return singletonAllocator;
    }
}

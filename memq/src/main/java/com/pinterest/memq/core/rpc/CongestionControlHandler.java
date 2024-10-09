package com.pinterest.memq.core.rpc;

import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.SocketAddress;

public final class CongestionControlHandler extends ChannelDuplexHandler {

  private final Logger logger = LoggerFactory.getLogger(CongestionControlHandler.class);

  public CongestionControlHandler() {
  }

  @Override
  public void connect(ChannelHandlerContext ctx, SocketAddress remoteAddress,
                      SocketAddress localAddress, ChannelPromise promise) throws Exception {
    super.connect(ctx, remoteAddress, localAddress, promise);
  }

  @Override
  public void channelActive(ChannelHandlerContext ctx) throws Exception {
    super.channelActive(ctx);
  }

  @Override
  public void channelInactive(ChannelHandlerContext ctx) throws Exception {
    super.channelInactive(ctx);
  }

  @Override
  public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
    // Check runtime memory usage.
    Runtime runtime = Runtime.getRuntime();
    long memoryMax = runtime.maxMemory();
    long memoryUsed = runtime.totalMemory() - runtime.freeMemory();
    double memoryUsedPercent = (double) memoryUsed / memoryMax * 100;
    long unpooledMemoryUsed = UnpooledByteBufAllocator.DEFAULT.metric().usedHeapMemory();
    long pooledMemoryUsed = PooledByteBufAllocator.DEFAULT.metric().usedHeapMemory();
    long unpooledHeapMemoryUsed = UnpooledByteBufAllocator.DEFAULT.metric().usedDirectMemory();
    long pooledHeapMemoryUsed = PooledByteBufAllocator.DEFAULT.metric().usedDirectMemory();

    logger.info("[TEST] channelRead metrics: Timestamp:" + System.currentTimeMillis() +
        "; Memory used: " + memoryUsedPercent +
        "%; Unpooled memory used: " + unpooledMemoryUsed +
        "; Pooled memory used: " + pooledMemoryUsed +
        "; Unpooled heap memory used: " + unpooledHeapMemoryUsed +
        "; Pooled heap memory used: " + pooledHeapMemoryUsed + ";");
    super.channelRead(ctx, msg);
  }
}

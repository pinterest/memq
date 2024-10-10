package com.pinterest.memq.core.rpc;

import com.sun.management.OperatingSystemMXBean;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.management.ManagementFactory;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public final class CongestionControlHandler extends ChannelDuplexHandler {

  private final Logger logger = LoggerFactory.getLogger(CongestionControlHandler.class);
  private static final Random random = new Random();

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
    int randomValue = random.nextInt(100);
    if (randomValue < 90) {
      // Drop log messages
      return;
    }
    // Check runtime memory usage.
    Runtime runtime = Runtime.getRuntime();
    long memoryMax = runtime.maxMemory();
    long memoryUsed = runtime.totalMemory() - runtime.freeMemory();
    double memoryUsedPercent = (double) memoryUsed / memoryMax * 100;
    // Check netty memory usage.
    long unpooledMemoryUsed = UnpooledByteBufAllocator.DEFAULT.metric().usedHeapMemory();
    long pooledMemoryUsed = PooledByteBufAllocator.DEFAULT.metric().usedHeapMemory();
    long unpooledHeapMemoryUsed = UnpooledByteBufAllocator.DEFAULT.metric().usedDirectMemory();
    long pooledHeapMemoryUsed = PooledByteBufAllocator.DEFAULT.metric().usedDirectMemory();
    // Check system memory usage.
    OperatingSystemMXBean osBean = ManagementFactory.getPlatformMXBean(
        OperatingSystemMXBean.class);
    long physicalMemorySize = osBean.getTotalPhysicalMemorySize();
    long freePhysicalMemorySize = osBean.getFreePhysicalMemorySize();
    long usedPhysicalMemorySize = physicalMemorySize - freePhysicalMemorySize;
    // Check CPU usage
    double processCpuLoad = osBean.getProcessCpuLoad() * 100;
    double systemCpuLoad = osBean.getSystemCpuLoad() * 100;

    List<String> usageList = new ArrayList<String>() {
      {
        add(String.valueOf(System.currentTimeMillis()));
        add(String.valueOf(memoryMax));
        add(String.valueOf(memoryUsed));
        add(String.valueOf(memoryUsedPercent));
        add(String.valueOf(unpooledMemoryUsed));
        add(String.valueOf(pooledMemoryUsed));
        add(String.valueOf(unpooledHeapMemoryUsed));
        add(String.valueOf(pooledHeapMemoryUsed));
        add(String.valueOf(physicalMemorySize));
        add(String.valueOf(freePhysicalMemorySize));
        add(String.valueOf(usedPhysicalMemorySize));
        add(String.valueOf(processCpuLoad));
        add(String.valueOf(systemCpuLoad));
      }
    };
    logger.info("[TEST_METRICS] " + usageList.toString());
    super.channelRead(ctx, msg);
  }
}

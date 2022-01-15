/**
 * Copyright 2022 Pinterest, Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *    http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.pinterest.memq.client.commons2.network;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.pinterest.memq.client.commons2.TransportPacketIdentifier;
import com.pinterest.memq.client.commons2.network.netty.ClientChannelInitializer;
import com.pinterest.memq.client.commons2.retry.ExponentialBackoffRetryStrategy;
import com.pinterest.memq.client.commons2.retry.RetryStrategy;
import com.pinterest.memq.client.producer.http.DaemonThreadFactory;
import com.pinterest.memq.commons.config.SSLConfig;
import com.pinterest.memq.commons.protocol.RequestPacket;
import com.pinterest.memq.commons.protocol.RequestType;
import com.pinterest.memq.commons.protocol.ResponsePacket;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.ReferenceCountUtil;

// No thread-safety guarantees
public class NetworkClient implements Closeable {
  private static final Logger logger = LoggerFactory.getLogger(NetworkClient.class);
  public static final String CONFIG_INITIAL_RETRY_INTERVAL_MS = "initialRetryIntervalMs";
  public static final String CONFIG_MAX_RETRY_COUNT = "maxRetryCount";
  public static final String CONFIG_IDLE_TIMEOUT_MS = "idleTimeoutMs";
  public static final String CONFIG_CONNECT_TIMEOUT_MS = "connectTimeoutMs";
  public static final String CONFIG_IRRESPONSIVE_TIMEOUT_MS = "irresponsiveTimeoutMs";

  private ExponentialBackoffRetryStrategy retryStrategy = new ExponentialBackoffRetryStrategy();
  private int idleTimeoutMs = 60000;
  private int connectTimeoutMs = 500;
  private int irresponsiveTimeoutMs = 60000;
  private final ScheduledExecutorService scheduler;

  private final ResponseHandler responseHandler;
  private final Bootstrap bootstrap;
  private final EventLoopGroup eventLoopGroup;
  private final AtomicBoolean closed = new AtomicBoolean(false);

  private volatile ChannelFuture connectFuture;

  public NetworkClient() {
    this(null, null);
  }
  public NetworkClient(Properties properties) {
    this(properties, null);
  }

  public NetworkClient(Properties properties, SSLConfig sslConfig) {
    if (properties != null) {
      if (properties.containsKey(CONFIG_INITIAL_RETRY_INTERVAL_MS)) {
        retryStrategy.setBaseRetryIntervalMs(Integer.parseInt(properties.get(CONFIG_INITIAL_RETRY_INTERVAL_MS).toString()));
      }
      if (properties.containsKey(CONFIG_MAX_RETRY_COUNT)) {
        retryStrategy.setMaxRetries(Integer.parseInt(properties.get(CONFIG_MAX_RETRY_COUNT).toString()));
      }
      if (properties.containsKey(CONFIG_CONNECT_TIMEOUT_MS)) {
        connectTimeoutMs = Integer.parseInt(properties.get(CONFIG_CONNECT_TIMEOUT_MS).toString());
      }
      if (properties.containsKey(CONFIG_IDLE_TIMEOUT_MS)) {
        idleTimeoutMs = Integer.parseInt(properties.get(CONFIG_IDLE_TIMEOUT_MS).toString());
      }
      if (properties.containsKey(CONFIG_IRRESPONSIVE_TIMEOUT_MS)) {
        irresponsiveTimeoutMs = Integer.parseInt(properties.get(CONFIG_IRRESPONSIVE_TIMEOUT_MS).toString());
      }
    }
    this.responseHandler = new ResponseHandler();
    bootstrap = new Bootstrap();
    if (Epoll.isAvailable()) {
      eventLoopGroup = new EpollEventLoopGroup(1, new DaemonThreadFactory("MemqCommonClientNettyGroup"));
      bootstrap.channel(EpollSocketChannel.class);
    } else {
      eventLoopGroup = new NioEventLoopGroup(1, new DaemonThreadFactory("MemqCommonClientNettyGroup"));
      bootstrap.channel(NioSocketChannel.class);
    }
    bootstrap.group(eventLoopGroup);
    bootstrap.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, connectTimeoutMs);
    bootstrap.handler(new ClientChannelInitializer(responseHandler, sslConfig, idleTimeoutMs));
    ScheduledThreadPoolExecutor tmpScheduler = new ScheduledThreadPoolExecutor(1);
    tmpScheduler.setRemoveOnCancelPolicy(true);
    this.scheduler = tmpScheduler;
  }

  public CompletableFuture<ResponsePacket> send(InetSocketAddress socketAddress, RequestPacket request) throws Exception {
    return send(socketAddress, request, Duration.ofMillis(irresponsiveTimeoutMs));
  }

  public CompletableFuture<ResponsePacket> send(InetSocketAddress socketAddress, RequestPacket request, Duration timeout)
      throws ExecutionException, InterruptedException {
    final long startMs = System.currentTimeMillis();
    if (closed.get()) {
      throw new IllegalStateException("Cannot send since client is closed");
    }
    CompletableFuture<ResponsePacket> returnFuture = new CompletableFuture<>();
    final TransportPacketIdentifier identifier = new TransportPacketIdentifier(request);

    // no need to remove listeners since they are removed by Netty after fired
    acquireChannel(socketAddress).addListener((ChannelFutureListener) channelFuture -> {
      if (channelFuture.isSuccess()) {
        long elapsedMs = System.currentTimeMillis() - startMs;
        responseHandler.addRequest(identifier, returnFuture);
        try {
          final ScheduledFuture<?> scheduledCleanup = scheduler.schedule(() -> {
            responseHandler.cancelRequest(identifier, new TimeoutException("Failed to receive response after " + timeout.toMillis() + " ms"));
          }, timeout.toMillis() - elapsedMs, TimeUnit.MILLISECONDS);
          returnFuture.handleAsync((responsePacket, throwable) -> {
            if (!scheduledCleanup.isDone()) {
             scheduledCleanup.cancel(true);
            }
            return null;
          });
        } catch (RejectedExecutionException ree) {
          if (!isClosed()) {
            logger.error("Failed to schedule clean up task: ", ree);
          }
        }
        ByteBuf buffer = null;
        try {
          buffer = PooledByteBufAllocator.DEFAULT.buffer(request.getSize(RequestType.PROTOCOL_VERSION));
          request.write(buffer, RequestType.PROTOCOL_VERSION);
          channelFuture.channel().writeAndFlush(buffer);
        } catch (Exception e) {
          logger.warn("Failed to write request " + request.getClientRequestId(), e);
          ReferenceCountUtil.release(buffer);
          responseHandler.cancelRequest(identifier, e);
        }
      } else {
        responseHandler.cancelRequest(identifier, channelFuture.cause());
      }
    });
    return returnFuture;
  }

  protected ChannelFuture acquireChannel(InetSocketAddress socketAddress) throws ExecutionException, InterruptedException {
    if (isChannelUnavailable(socketAddress)) {
      synchronized (this) {
        if (isChannelUnavailable(socketAddress)) {
          if (connectFuture != null && connectFuture.channel() != null) {
            // destination address is different from current connection's remote address
            connectFuture.channel().close().await();
          }
          CompletableFuture<ChannelFuture> connectReadyFuture = new CompletableFuture<>();
          doConnect(socketAddress, connectReadyFuture, 0);
          connectFuture = connectReadyFuture.get();
        }
      }
    }
    return connectFuture;
  }

  private boolean isChannelUnavailable(InetSocketAddress socketAddress) {
    if (connectFuture == null || !connectFuture.channel().isActive()) return true;
    InetSocketAddress currentAddr = (InetSocketAddress) (connectFuture.channel().remoteAddress());

    return !currentAddr.getHostString().equals(socketAddress.getHostString()) || !(currentAddr.getPort() == socketAddress.getPort());
  }

  private void doConnect(InetSocketAddress socketAddress, CompletableFuture<ChannelFuture> connectReadyFuture, int attempts) {
    logger.debug("Connecting to " + socketAddress.getHostString() + ", attempt " + (attempts + 1));
    // no need to remove listeners since they are removed by Netty after fired
    bootstrap.connect(socketAddress).addListener(new RetryListener(socketAddress, connectReadyFuture, attempts, retryStrategy));
  }

  @Override
  public void close() throws IOException {
    logger.debug("Closing network client");
    closed.set(true);
    connectFuture = null;
    responseHandler.close();
    scheduler.shutdown();
    eventLoopGroup.shutdownGracefully();
  }

  public boolean isClosed() {
    return closed.get();
  }

  // blocking
  public void reset() throws IOException, InterruptedException {
    if (connectFuture != null && connectFuture.channel() != null) {
      connectFuture.channel().close().await(); // should reset the response map after channel is closed
      connectFuture = null;
    }
  }

  private final class RetryListener implements ChannelFutureListener {
    private final CompletableFuture<ChannelFuture> connectReadyFuture;
    private final InetSocketAddress socketAddress;
    private final int attempts;
    private final RetryStrategy retryStrategy;

    public RetryListener(
        InetSocketAddress socketAddress,
        CompletableFuture<ChannelFuture> connectReadyFuture,
        int attempts,
        RetryStrategy retryStrategy
    ) {
      this.connectReadyFuture = connectReadyFuture;
      this.socketAddress = socketAddress;
      this.attempts = attempts;
      this.retryStrategy = retryStrategy;
    }

    @Override
    public void operationComplete(ChannelFuture future) throws Exception {
      if (!future.isSuccess()) {
        Duration nextRetryInterval = retryStrategy.calculateNextRetryInterval(attempts);
        if (nextRetryInterval == null) {
          connectReadyFuture.completeExceptionally(future.cause());
        } else {
          logger.warn("Failed to connect to " + socketAddress + ", retry in " + nextRetryInterval.toMillis()
              + " ms, reason:" + future.cause());
          scheduler.schedule(
              () -> doConnect(
                  socketAddress,
                  connectReadyFuture,
                  attempts + 1
              ),
              nextRetryInterval.toMillis(),
              TimeUnit.MILLISECONDS);
          future.channel().close().await();
        }
      } else {
        connectReadyFuture.complete(future);
      }
    }
  }

  public int getInflightRequestCount() {
    return responseHandler.getInflightRequestCount();
  }

  @VisibleForTesting
  protected ChannelFuture getConnectFuture() {
    return connectFuture;
  }
}

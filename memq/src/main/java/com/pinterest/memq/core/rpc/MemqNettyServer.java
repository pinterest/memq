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
package com.pinterest.memq.core.rpc;

import java.net.UnknownHostException;
import java.nio.ByteOrder;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.TrustManagerFactory;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.ScheduledReporter;
import com.pinterest.memq.commons.config.SSLConfig;
import com.pinterest.memq.commons.mon.OpenTSDBClient;
import com.pinterest.memq.commons.mon.OpenTSDBReporter;
import com.pinterest.memq.core.MemqManager;
import com.pinterest.memq.core.clustering.MemqGovernor;
import com.pinterest.memq.core.config.AuthorizerConfig;
import com.pinterest.memq.core.config.MemqConfig;
import com.pinterest.memq.core.config.NettyServerConfig;
import com.pinterest.memq.core.security.Authorizer;
import com.pinterest.memq.core.utils.DaemonThreadFactory;
import com.pinterest.memq.core.utils.MemqUtils;
import com.pinterest.memq.core.utils.MiscUtils;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.PoolArenaMetric;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.ssl.ClientAuth;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslHandler;

public class MemqNettyServer {

  public static final String SSL_HANDLER_NAME = "ssl";

  private static final Logger logger = Logger.getLogger(MemqNettyServer.class.getName());

  private EventLoopGroup childGroup;
  private EventLoopGroup parentGroup;
  private ChannelFuture serverChannelFuture;
  private MemqConfig configuration;
  private MemqManager memqManager;
  private Map<String, MetricRegistry> metricsRegistryMap;
  private OpenTSDBClient client = null;
  private boolean useEpoll;
  private MemqGovernor memqGovernor;

  public MemqNettyServer(MemqConfig configuration,
                         MemqManager memqManager,
                         MemqGovernor governor,
                         Map<String, MetricRegistry> metricsRegistryMap,
                         OpenTSDBClient client) {
    this.configuration = configuration;
    this.memqManager = memqManager;
    this.memqGovernor = governor;
    this.metricsRegistryMap = metricsRegistryMap;
    this.client = client;
  }

  public void initialize() throws Exception {
    MetricRegistry registry = initializeMetrics();
    Authorizer authorizer = enableAuthenticationAuthorizationAuditing(configuration);

    NettyServerConfig nettyServerConfig = configuration.getNettyServerConfig();
    this.useEpoll = Epoll.isAvailable() && nettyServerConfig.isEnableEpoll();
    childGroup = getEventLoopGroup(nettyServerConfig.getNumEventLoopThreads());
    // there can only be maximum of 1 acceptor threads
    parentGroup = getEventLoopGroup(1);

    logger.info("Starting Netty Server with epoll:" + useEpoll);
    try {
      ServerBootstrap serverBootstrap = new ServerBootstrap();
      serverBootstrap.group(parentGroup, childGroup);
      if (useEpoll) {
        serverBootstrap.channel(EpollServerSocketChannel.class);
      } else {
        serverBootstrap.channel(NioServerSocketChannel.class);
      }
      serverBootstrap.localAddress(nettyServerConfig.getPort());
      serverBootstrap.childHandler(new ChannelInitializer<SocketChannel>() {

        protected void initChannel(SocketChannel channel) throws Exception {
          SSLConfig sslConfig = nettyServerConfig.getSslConfig();
          ChannelPipeline pipeline = channel.pipeline();
          if (sslConfig != null) {
            KeyManagerFactory kmf = MemqUtils.extractKMFFromSSLConfig(sslConfig);
            TrustManagerFactory tmf = MemqUtils.extractTMPFromSSLConfig(sslConfig);

            SslContext ctx = SslContextBuilder.forServer(kmf).clientAuth(ClientAuth.REQUIRE)
                .trustManager(tmf).protocols(sslConfig.getProtocols()).build();
            SslHandler sslHandler = ctx.newHandler(channel.alloc());
            pipeline.addLast(SSL_HANDLER_NAME, sslHandler);
          }

          pipeline.addLast(new LengthFieldBasedFrameDecoder(ByteOrder.BIG_ENDIAN,
              nettyServerConfig.getMaxFrameByteLength(), 0, Integer.BYTES, 0, 0, false));
          pipeline.addLast(new MemqResponseEncoder(registry));
          pipeline.addLast(new MemqRequestDecoder(memqManager, memqGovernor, authorizer, registry));
        }

      });
      serverChannelFuture = serverBootstrap.bind().sync();
    } catch (Exception e) {
      logger.log(Level.SEVERE, "Failed to start Netty server", e);
    }
    logger.info("\n\nNetty server started on port:" + nettyServerConfig.getPort() + "\n");
  }

  private MetricRegistry initializeMetrics() throws UnknownHostException {
    MetricRegistry registry = new MetricRegistry();
    metricsRegistryMap.put("_netty", registry);
    registry.gauge("unpooled.direct.used.bytes",
        () -> (Gauge<Long>) () -> UnpooledByteBufAllocator.DEFAULT.metric().usedDirectMemory());
    registry.gauge("unpooled.heap.used.bytes",
        () -> (Gauge<Long>) () -> UnpooledByteBufAllocator.DEFAULT.metric().usedHeapMemory());
    registry.gauge("pooled.direct.used.bytes",
        () -> (Gauge<Long>) () -> PooledByteBufAllocator.DEFAULT.metric().usedDirectMemory());
    registry.gauge("pooled.heap.used.bytes",
        () -> (Gauge<Long>) () -> PooledByteBufAllocator.DEFAULT.metric().usedHeapMemory());

    registry.gauge("pooled.direct.arenas",
        () -> (Gauge<Integer>) () -> PooledByteBufAllocator.DEFAULT.metric().numDirectArenas());

    registry.gauge("pooled.direct.total.active.allocation.count",
        () -> (Gauge<Long>) () -> PooledByteBufAllocator.DEFAULT.metric().directArenas().stream()
            .mapToLong(PoolArenaMetric::numActiveAllocations).sum());

    registry.gauge("pooled.direct.total.active.allocation.bytes",
        () -> (Gauge<Long>) () -> PooledByteBufAllocator.DEFAULT.metric().directArenas().stream()
            .mapToLong(PoolArenaMetric::numActiveBytes).sum());

    if (client != null) {
      String localHostname = MiscUtils.getHostname();
      for (String metricName : registry.getNames()) {
        ScheduledReporter reporter = OpenTSDBReporter.createReporter("netty", registry, metricName,
            (String name, Metric metric) -> true, TimeUnit.SECONDS, TimeUnit.SECONDS, client,
            localHostname);
        reporter.start(configuration.getOpenTsdbConfig().getFrequencyInSeconds(), TimeUnit.SECONDS);
      }
    }
    return registry;
  }

  private Authorizer enableAuthenticationAuthorizationAuditing(MemqConfig configuration) throws Exception {
    AuthorizerConfig authorizerConfig = configuration.getAuthorizerConfig();
    if (authorizerConfig != null) {
      Authorizer authorizer = authorizerConfig.getClass().asSubclass(Authorizer.class)
          .newInstance();
      authorizer.init(authorizerConfig);
      return authorizer;
    }
    return null;
  }

  private EventLoopGroup getEventLoopGroup(int nThreads) {
    if (useEpoll) {
      logger.info("Epoll is available and will be used");
      return new EpollEventLoopGroup(nThreads, new DaemonThreadFactory());
    } else {
      return new NioEventLoopGroup(nThreads, new DaemonThreadFactory());
    }
  }

  public EventLoopGroup getChildGroup() {
    return childGroup;
  }

  public EventLoopGroup getParentGroup() {
    return parentGroup;
  }

  public ChannelFuture getServerChannelFuture() {
    return serverChannelFuture;
  }

  public void stop() {
    serverChannelFuture.channel().close();
    if (serverChannelFuture.channel().parent() != null) {
      serverChannelFuture.channel().parent().close();
    }
  }

  public MemqGovernor getMemqGovernor() {
    return memqGovernor;
  }
}

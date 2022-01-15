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
package com.pinterest.memq.client.commons;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.TrustManagerFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.pinterest.memq.client.producer.http.DaemonThreadFactory;
import com.pinterest.memq.client.producer.netty.MemqNettyProducer;
import com.pinterest.memq.commons.config.SSLConfig;
import com.pinterest.memq.commons.protocol.Broker;
import com.pinterest.memq.commons.protocol.Broker.BrokerType;
import com.pinterest.memq.commons.protocol.RequestPacket;
import com.pinterest.memq.commons.protocol.RequestType;
import com.pinterest.memq.commons.protocol.ResponseCodes;
import com.pinterest.memq.commons.protocol.ResponsePacket;
import com.pinterest.memq.commons.protocol.TopicMetadata;
import com.pinterest.memq.commons.protocol.TopicMetadataRequestPacket;
import com.pinterest.memq.commons.protocol.TopicMetadataResponsePacket;
import com.pinterest.memq.core.utils.MemqUtils;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.ssl.ClientAuth;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;

/**
 * Common Client layer to enable connection handling for Producer and Consumer
 * without code duplication. This class defines logic for connection,
 * reconnection etc. including locality awareness.
 * 
 * This also auto attaches the common handlers regardless of specific handler
 * types.
 */
public class MemqCommonClient {

  private static final Logger logger = LoggerFactory.getLogger(MemqNettyProducer.class);
  private EventLoopGroup group;
  private volatile Channel channel;
  private CompletableFuture<Channel> channelFuture;
  private ChannelFuture connect;
  private String locality;
  private SSLConfig sslConfig;
  private ResponseHandler responseHandler;
  private Map<String, Consumer<ResponsePacket>> responseMap = new ConcurrentHashMap<>();
  private Set<Broker> brokers;

  public MemqCommonClient() {
    this.responseHandler = new ResponseHandler();
    responseHandler.setRequestMap(responseMap);
  }

  public MemqCommonClient(InetSocketAddress suppliedServer,
                          SSLConfig sslConfig) throws Exception {
    this();
    this.sslConfig = sslConfig;
    doConnect(suppliedServer, 10, 2);
    waitForConnectOrTimeout();
  }

  public MemqCommonClient(Set<Broker> brokers,
                          SSLConfig sslConfig) throws Exception {
    this();
    this.brokers = brokers;
    this.sslConfig = sslConfig;
    doConnect(null, 10, 2);
    waitForConnectOrTimeout();
  }

  public MemqCommonClient(Channel channel) {
    this();
    this.channel = channel;
  }

  public MemqCommonClient(String cluster,
                          String serversetFile,
                          String locality,
                          SSLConfig sslConfig) throws Exception {
    this();
    InetSocketAddress suppliedServer = MemqNettyProducer.tryAndGetAZLocalServer(serversetFile,
        locality);
    this.sslConfig = sslConfig;
    doConnect(suppliedServer, 10, 2);
    waitForConnectOrTimeout();
  }

  private void doConnect(final List<InetSocketAddress> suppliedServer,
                         final int retryTimeSeconds) throws Exception {
    Collections.shuffle(suppliedServer);
    doConnect(suppliedServer.get(0), retryTimeSeconds, 1);
  }

  private void doConnect(final InetSocketAddress suppliedServer,
                         final int retryTimeSeconds,
                         int maxRetries) throws Exception {
    if (maxRetries < 0) {
      throw new Exception("Failed to connect, exhausted retries");
    }

    if (group == null || group.isShutdown()) {
      this.group = new NioEventLoopGroup(new DaemonThreadFactory("MemqCommonClientNettyGroup"));
    }

    InetSocketAddress localServer = getLocalServer(suppliedServer);

    Bootstrap clientBootstrap = new Bootstrap();
    clientBootstrap.group(group);
    clientBootstrap.channel(NioSocketChannel.class);
    clientBootstrap.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 5000);
    clientBootstrap.handler(new ClientChannelInitializer(sslConfig));
    connect = clientBootstrap.connect(localServer);
    channelFuture = new CompletableFuture<>();
    logger.info("Attempting to connect to " + localServer);
    connect.addListener((ChannelFuture f) -> {
      if (!f.isSuccess()) {
        // schedule reconnect
        logger.warn("Failed to connect to " + localServer + " retry in " + retryTimeSeconds
            + "s, reason:" + f.cause().getMessage());
        f.channel().eventLoop().schedule(() -> {
          try {
            doConnect(suppliedServer, retryTimeSeconds, maxRetries - 1);
          } catch (Exception e) {
            logger.error("Failed to connect during schedule", e);
            throw new RuntimeException(e);
          }
        }, retryTimeSeconds, TimeUnit.SECONDS);
      } else {
        // set channel variable so request executors can use it
        channel = f.channel();
        channelFuture.complete(channel);
        logger.info("Connected to " + localServer);
        // listen for close and reconnect
        channel.closeFuture().addListener(new ChannelFutureListener() {

          @Override
          public void operationComplete(ChannelFuture future) throws Exception {
            if (!isActive()) {
              logger.warn("Disconnected from broker will attempt to reconnect now.");
              Channel channel = future.channel();
              channel.disconnect();
              doConnect(suppliedServer, retryTimeSeconds, maxRetries - 1);
            } else {
              logger.info("Closing connection to broker");
              channelFuture = null;
            }
          }
        });
      }
    });
  }

  private InetSocketAddress getLocalServer(final InetSocketAddress suppliedServer) throws IOException {
    InetSocketAddress localServer;
    if (suppliedServer == null) {
      List<InetSocketAddress> localServers = getLocalServers(brokers, locality);
      Collections.shuffle(localServers);
      localServer = localServers.get(0);
    } else {
      localServer = suppliedServer;
    }
    return localServer;
  }

  private void waitForConnectOrTimeout() throws InterruptedException {
    for (int i = 0; i < 10; i++) {
      if (connect == null) {
        Thread.sleep(1000);
      } else {
        try {
          connect.sync().await(5, TimeUnit.SECONDS);
          break;
        } catch (Exception e) {
          continue;
        }
      }
    }
  }

  public Future<ResponsePacket> sendRequestPacketAndReturnResponseFuture(RequestPacket request,
                                                                         boolean throwException) {
    CompletableFuture<ResponsePacket> future = new CompletableFuture<>();
    try {
      sendRequestPacket(request, response -> {
        if (response.getResponseCode() == ResponseCodes.OK) {
          future.complete(response);
        } else {
          if (throwException) {
            future.completeExceptionally(new Exception("Request failed with code:"
                + response.getResponseCode() + " and error:" + response.getErrorMessage()));
          } else {
            future.complete(response);
          }
        }
      });
    } catch (Exception e) {
      logger.error("Failed to send request packet", e);
      future.completeExceptionally(e);
    }
    return future;
  }

  protected void sendRequestPacket(RequestPacket request,
                                   Consumer<ResponsePacket> responseConsumer) throws Exception {
    ByteBuf buffer = PooledByteBufAllocator.DEFAULT
        .buffer(request.getSize(RequestType.PROTOCOL_VERSION));
    request.write(buffer, RequestType.PROTOCOL_VERSION);
    if (channel == null) {
      channel = channelFuture.get(10, TimeUnit.SECONDS);
    }
    if (responseConsumer != null) {
      responseMap.put(makeResponseKey(request), responseConsumer);
    }
    channel.writeAndFlush(buffer);
  }

  public static String makeResponseKey(RequestPacket request) {
    return request.getRequestType() + "_" + request.getClientRequestId();
  }

  public static String makeResponseKey(ResponsePacket request) {
    return request.getRequestType() + "_" + request.getClientRequestId();
  }

  public boolean isActive() {
    return channel != null;
  }

  public boolean isClosed() {
    return group.isShutdown();
  }

  public void closeChannel() throws InterruptedException {
    if (channel != null && channel.isOpen()) {
      channel.flush();
      channel.close().sync();
    }
    channel = null;
    channelFuture = null;
  }

  public synchronized void close() throws IOException {
    try {
      if (group != null) {
        group.shutdownGracefully().sync().get();
      }
      closeChannel();
    } catch (InterruptedException | ExecutionException e) {
      throw new IOException("Interrupted closing request", e);
    }
  }

  public static List<InetSocketAddress> getLocalServers(Set<Broker> brokers, String locality) {
    List<Broker> collect = brokers.stream().filter(b -> b.getLocality().equalsIgnoreCase(locality))
        .collect(Collectors.toList());
    if (collect.isEmpty()) {
      collect = new ArrayList<>(brokers);
    }
    return collect.stream().map(ep -> {
      return InetSocketAddress.createUnresolved(ep.getBrokerIP(), ep.getBrokerPort());
    }).collect(Collectors.toList());
  }

  public boolean awaitConnect(int timeout, TimeUnit timeunit) throws InterruptedException {
    return connect.sync().await(timeout, timeunit);
  }

  public static TopicMetadata getTopicMetadata(String cluster,
                                               String serverset,
                                               String topic,
                                               int timeoutMillis) throws Exception {
    MemqCommonClient client = new MemqCommonClient(cluster, serverset, "na", null);
    client.awaitConnect(5, TimeUnit.SECONDS);
    TopicMetadata topicMetadata = getTopicMetadata(client, topic, timeoutMillis);
    client.close();
    return topicMetadata;
  }
  
  public static TopicMetadata getTopicMetadata(String cluster,
                                               Set<Broker> bootstrapServer,
                                               String topic,
                                               int timeoutMillis) throws Exception {
    MemqCommonClient client = new MemqCommonClient(bootstrapServer, null);
    client.awaitConnect(5, TimeUnit.SECONDS);
    TopicMetadata topicMetadata = getTopicMetadata(client, topic, timeoutMillis);
    client.close();
    return topicMetadata;
  }

  public static TopicMetadata getTopicMetadata(MemqCommonClient client,
                                               String topic,
                                               int timeoutMillis) throws InterruptedException,
                                                                  ExecutionException,
                                                                  TimeoutException {
    Future<ResponsePacket> response = client.sendRequestPacketAndReturnResponseFuture(
        new RequestPacket(RequestType.PROTOCOL_VERSION, ThreadLocalRandom.current().nextLong(),
            RequestType.TOPIC_METADATA, new TopicMetadataRequestPacket(topic)),
        true);
    ResponsePacket responsePacket = response.get(timeoutMillis, TimeUnit.MILLISECONDS);
    TopicMetadataResponsePacket resp = ((TopicMetadataResponsePacket) responsePacket.getPacket());
    return resp.getMetadata();
  }

  public final class ClientChannelInitializer extends ChannelInitializer<SocketChannel> {

    private static final int FRAME_LENGTH_ENCODING_SIZE = 4;
    private static final int MAX_FRAME_SIZE = 4 * 1024 * 1024;
    private SSLConfig sslConfig;

    public ClientChannelInitializer(SSLConfig sslConfig) {
      this.sslConfig = sslConfig;
    }

    protected void initChannel(SocketChannel channel) throws Exception {
      try {
        ChannelPipeline pipeline = channel.pipeline();
        if (sslConfig != null) {
          KeyManagerFactory kmf = MemqUtils.extractKMFFromSSLConfig(sslConfig);
          TrustManagerFactory tmf = MemqUtils.extractTMPFromSSLConfig(sslConfig);
          SslContext ctx = SslContextBuilder.forClient().protocols(sslConfig.getProtocols())
              .keyManager(kmf).clientAuth(ClientAuth.REQUIRE).trustManager(tmf).build();
          pipeline.addLast(ctx.newHandler(channel.alloc()));
        }
        pipeline.addLast(new LengthFieldBasedFrameDecoder(ByteOrder.BIG_ENDIAN, MAX_FRAME_SIZE, 0,
            FRAME_LENGTH_ENCODING_SIZE, 0, 0, false));
        pipeline.addLast(new MemqNettyClientSideResponseHandler(responseHandler));
      } catch (Exception e) {
        e.printStackTrace();
        throw e;
      }
    }
  }

  public synchronized void reconnect(String topic) throws Exception {
    TopicMetadata md = getTopicMetadata(this, topic, 10000);
    this.close();
    Set<Broker> brokers = md.getWriteBrokers();
    List<InetSocketAddress> localServers = getLocalServers(brokers, locality);
    doConnect(localServers, 10);
  }

  public static Set<Broker> getBootstrapBrokers(String bootstrapServers) {
    Set<Broker> seedBrokers = Arrays.asList(bootstrapServers.split(",")).stream().map(e -> {
      String[] parts = e.split(":");
      return new Broker(parts[0], Short.parseShort(parts[1]), "n/a", "n/a", BrokerType.WRITE, new HashSet<>());
    }).collect(Collectors.toSet());
    return seedBrokers;
  }

}

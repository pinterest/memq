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
package com.pinterest.memq.client.commons2.network.netty;

import java.nio.ByteOrder;
import java.util.concurrent.TimeUnit;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.TrustManagerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.pinterest.memq.client.commons2.network.ResponseHandler;
import com.pinterest.memq.commons.config.SSLConfig;
import com.pinterest.memq.core.utils.MemqUtils;

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.ssl.ClientAuth;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.timeout.IdleStateHandler;

public final class ClientChannelInitializer extends ChannelInitializer<SocketChannel> {

  private static final int FRAME_LENGTH_ENCODING_SIZE = 4;
  private static final int MAX_FRAME_SIZE = 4 * 1024 * 1024;
  private static ChannelHandler wiretapper = null;

  private final ResponseHandler handler;
  private final SSLConfig sslConfig;
  private final long idleTimeoutMs;

  public ClientChannelInitializer(ResponseHandler handler,
                                  SSLConfig sslConfig, long idleTimeoutMs) {
    this.handler = handler;
    this.sslConfig = sslConfig;
    this.idleTimeoutMs = idleTimeoutMs;
  }

  protected void initChannel(SocketChannel channel) throws Exception {
    try {
      ChannelPipeline pipeline = channel.pipeline();
      if (wiretapper != null) {
        pipeline.addLast(wiretapper);
      }
      pipeline.addLast(new IdleStateHandler(0, 0, idleTimeoutMs, TimeUnit.MILLISECONDS));
      pipeline.addLast(new ConnectionLifecycleHandler(handler));
      if (sslConfig != null) {
        KeyManagerFactory kmf = MemqUtils.extractKMFFromSSLConfig(sslConfig);
        TrustManagerFactory tmf = MemqUtils.extractTMPFromSSLConfig(sslConfig);
        SslContext ctx = SslContextBuilder.forClient().protocols(sslConfig.getProtocols())
            .keyManager(kmf).clientAuth(ClientAuth.REQUIRE).trustManager(tmf).build();
        pipeline.addLast(ctx.newHandler(channel.alloc()));
      }
      pipeline.addLast(new LengthFieldBasedFrameDecoder(ByteOrder.BIG_ENDIAN, MAX_FRAME_SIZE, 0,
          FRAME_LENGTH_ENCODING_SIZE, 0, 0, false));
      pipeline.addLast(new MemqNettyClientSideResponseHandler(handler));
    } catch (Exception e) {
      e.printStackTrace();
      throw e;
    }
  }

  @VisibleForTesting
  public static void setWiretapper(ChannelHandler wiretapper) {
    ClientChannelInitializer.wiretapper = wiretapper;
  }
}

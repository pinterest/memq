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
package com.pinterest.memq.client.commons2;

import com.pinterest.memq.commons.protocol.RequestPacket;
import com.pinterest.memq.commons.protocol.RequestType;
import com.pinterest.memq.commons.protocol.ResponseCodes;
import com.pinterest.memq.commons.protocol.ResponsePacket;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.ChannelPromise;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteOrder;
import java.util.Map;
import java.util.function.BiConsumer;

public class MockMemqServer {
  private static final Logger logger = LoggerFactory.getLogger(MockMemqServer.class);

  private final ServerBootstrap bootstrap;
  private ChannelFuture channelFuture;
  private final ByteBufAllocator allocator;

  public MockMemqServer(int port, Map<RequestType, BiConsumer<ChannelHandlerContext, RequestPacket>> responseMap, boolean useDirect) {
    allocator = new PooledByteBufAllocator(useDirect);
    bootstrap = new ServerBootstrap();
    bootstrap.group(new NioEventLoopGroup(1, new ThreadFactoryBuilder().setNameFormat("boss").build()), new NioEventLoopGroup(new ThreadFactoryBuilder().setNameFormat("worker").build()));
    bootstrap.channel(NioServerSocketChannel.class);
    bootstrap.localAddress(port);
    bootstrap.childOption(ChannelOption.ALLOCATOR, allocator);
    bootstrap.childHandler(new ChannelInitializer<SocketChannel>() {
      @Override
      protected void initChannel(SocketChannel ch) throws Exception {
        ChannelPipeline pipeline = ch.pipeline();
        pipeline.addLast(new LengthFieldBasedFrameDecoder(
            ByteOrder.BIG_ENDIAN,
            4 * 1024 * 1024,
            0,
            Integer.BYTES,
            0,
            0,
            false));
        pipeline.addLast(new MockResponseHandler());
        pipeline.addLast(new MockRequestHandler(responseMap));
      }

    });
  }

  public MockMemqServer(int port, Map<RequestType, BiConsumer<ChannelHandlerContext, RequestPacket>> responseMap) {
    this(port, responseMap, true);
  }

  public ChannelFuture start() throws Exception {
    channelFuture = bootstrap.bind().sync();
    return channelFuture;
  }

  public void stop() throws Exception {
    if (channelFuture == null) {
      return;
    }
    channelFuture.channel().close();
    if (channelFuture.channel().parent() != null) {
      channelFuture.channel().parent().close();
    }
  }

  private static class MockRequestHandler extends ChannelInboundHandlerAdapter {
    private final Map<RequestType, BiConsumer<ChannelHandlerContext, RequestPacket>> responseMap;

    public MockRequestHandler(Map<RequestType, BiConsumer<ChannelHandlerContext, RequestPacket>> responseMap) {
      this.responseMap = responseMap;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
      ByteBuf inBuffer = (ByteBuf) msg;
      RequestPacket requestPacket = new RequestPacket();
      requestPacket.readFields(inBuffer, (short) 0);
      inBuffer.release();
      BiConsumer<ChannelHandlerContext, RequestPacket> consumer = responseMap.get(requestPacket.getRequestType());
      if (consumer != null) {
        consumer.accept(ctx, requestPacket);
      } else {
        ResponsePacket resp = new ResponsePacket(requestPacket.getProtocolVersion(),
            requestPacket.getClientRequestId(), requestPacket.getRequestType(), ResponseCodes.BAD_REQUEST, "No handler for request type");
        ctx.writeAndFlush(resp);
      }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
      logger.warn("Exception caught on inbound connection: {}", cause.getMessage());
      ctx.close();
    }
  }

  private class MockResponseHandler extends ChannelOutboundHandlerAdapter {

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise)
        throws Exception {
      if (msg instanceof ResponsePacket) {
        ResponsePacket response = (ResponsePacket) msg;
        int responseSize;
        try {
          responseSize = response.getSize(response.getProtocolVersion());
        } catch (Exception e) {
          logger.error("Failed to encode response packet: ", e);
          response = new ResponsePacket(
              response.getProtocolVersion(),
              response.getClientRequestId(),
              response.getRequestType(),
              ResponseCodes.INTERNAL_SERVER_ERROR,
              "Failed to encode response packet " + e.getMessage()
          );
          responseSize = response.getSize(response.getProtocolVersion());
        }
        ByteBuf buffer = allocator.buffer(responseSize, responseSize);
        response.write(buffer, response.getProtocolVersion());
        super.write(ctx, buffer, promise);
      } else {
        super.write(ctx, msg, promise);
      }
      promise.addListener((f) -> {
        if(!f.isSuccess()) {
          logger.error("Failed to respond", f.cause());
        }
      });
    }
  }
}

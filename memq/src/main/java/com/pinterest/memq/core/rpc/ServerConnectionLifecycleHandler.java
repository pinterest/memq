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

import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.SocketAddress;

public final class ServerConnectionLifecycleHandler extends ChannelDuplexHandler {

  private final Logger logger = LoggerFactory.getLogger(ServerConnectionLifecycleHandler.class);

  /**
   * Logging handler for server connection lifecycle events. Extends {@link ChannelDuplexHandler}.
   */
  public ServerConnectionLifecycleHandler() {
  }

  /**
   * Connects to the remote address. Logs the remote address.
   * @param ctx               the {@link ChannelHandlerContext} for which the connect operation is made
   * @param remoteAddress     the {@link SocketAddress} to which it should connect
   * @param localAddress      the {@link SocketAddress} which is used as source on connect
   * @param promise           the {@link ChannelPromise} to notify once the operation completes
   * @throws Exception
   */
  @Override
  public void connect(ChannelHandlerContext ctx, SocketAddress remoteAddress,
                      SocketAddress localAddress, ChannelPromise promise) throws Exception {
    logger.info("Connecting to " + remoteAddress);
    super.connect(ctx, remoteAddress, localAddress, promise);
  }

  /**
   * Disconnects from the remote address. Logs the channel id and remote address.
   * @param ctx       the {@link ChannelHandlerContext} for which the disconnect operation is made
   * @throws Exception
   */
  @Override
  public void channelActive(ChannelHandlerContext ctx) throws Exception {
    logger.info("[" + ctx.channel().id() + "] Connected to " + ctx.channel().remoteAddress());
    super.channelActive(ctx);
  }

  /**
   * Disconnects from the remote address. Logs the channel id and remote address.
   * @param ctx       the {@link ChannelHandlerContext} for which the disconnect operation is made
   * @throws Exception
   */
  @Override
  public void channelInactive(ChannelHandlerContext ctx) throws Exception {
    logger.info("[" + ctx.channel().id() + "] Closing connection to server: " + ctx.channel()
        .remoteAddress());
    super.channelInactive(ctx);
  }

  /**
   * Logs the exception caught in the inbound pipeline with channel id and closes the channel.
   * @param ctx       the {@link ChannelHandlerContext} for which the exception is caught
   * @param cause     the {@link Throwable} that was caught
   * @throws Exception
   */
  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
    logger.error("[" + ctx.channel().id() + "] Exception caught in inbound pipeline: ", cause);
    ctx.close();
  }

  /**
   * User event triggered.
   * If the event is an instance of {@link IdleStateEvent}, it logs the disconnection event and closes the channel.
   * @param ctx
   * @param evt
   * @throws Exception
   */
  @Override
  public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
    if (evt instanceof IdleStateEvent) {
      IdleStateEvent e = (IdleStateEvent) evt;
      if (e.state() == IdleState.ALL_IDLE) {
        logger.warn("Disconnecting to " + ctx.channel().remoteAddress() + " due to idle activity");
        ctx.close();
      }
    }
  }
}

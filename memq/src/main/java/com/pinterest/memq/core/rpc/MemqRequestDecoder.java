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

import java.security.Principal;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.net.ssl.SSLSession;
import javax.ws.rs.BadRequestException;
import javax.ws.rs.InternalServerErrorException;
import javax.ws.rs.NotAuthorizedException;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.RedirectionException;
import javax.ws.rs.ServiceUnavailableException;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import com.pinterest.memq.commons.protocol.RequestPacket;
import com.pinterest.memq.commons.protocol.ResponseCodes;
import com.pinterest.memq.commons.protocol.ResponsePacket;
import com.pinterest.memq.core.MemqManager;
import com.pinterest.memq.core.clustering.MemqGovernor;
import com.pinterest.memq.core.security.Authorizer;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.ssl.SslHandler;

public class MemqRequestDecoder extends ChannelInboundHandlerAdapter {

  private static final Logger logger = Logger
      .getLogger(MemqRequestDecoder.class.getCanonicalName());
  private Counter errorCounter;
  private PacketSwitchingHandler packetSwitchHandler;

  public MemqRequestDecoder(MemqManager mgr,
                            MemqGovernor governor,
                            Authorizer authorizer,
                            MetricRegistry registry) {
    errorCounter = registry.counter("request.error");
    packetSwitchHandler = new PacketSwitchingHandler(mgr, governor, authorizer, registry);
  }

  @Override
  public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
    ByteBuf inBuffer = (ByteBuf) msg;
    RequestPacket requestPacket = new RequestPacket();
    requestPacket.readFields(inBuffer, (short) 0);
    try {
      Principal principal = null;
      String clientAddress = ctx.channel().remoteAddress() != null
          ? ctx.channel().remoteAddress().toString()
          : null;
      // do principal extraction
      SslHandler sslhandler = (SslHandler) ctx.channel().pipeline()
          .get(MemqNettyServer.SSL_HANDLER_NAME);
      if (sslhandler != null) {
        SSLSession session = sslhandler.engine().getSession();
        clientAddress = session.getPeerHost();
        principal = session.getPeerPrincipal();
      }
      logger.fine(() -> "Request recived:" + requestPacket.getClientRequestId() + " type:"
          + requestPacket.getRequestType().name());
      packetSwitchHandler.handle(ctx, requestPacket, principal, clientAddress);
    } catch (RedirectionException e) {
      ctx.writeAndFlush(
          new ResponsePacket(requestPacket.getProtocolVersion(), requestPacket.getClientRequestId(),
              requestPacket.getRequestType(), ResponseCodes.REDIRECT, e.getMessage()));
    } catch (NotAuthorizedException e) {
      errorCounter.inc();
      ctx.writeAndFlush(
          new ResponsePacket(requestPacket.getProtocolVersion(), requestPacket.getClientRequestId(),
              requestPacket.getRequestType(), ResponseCodes.UNAUTHORIZED, e.getMessage()));
    } catch (ServiceUnavailableException e) {
      errorCounter.inc();
      ctx.writeAndFlush(
          new ResponsePacket(requestPacket.getProtocolVersion(), requestPacket.getClientRequestId(),
              requestPacket.getRequestType(), ResponseCodes.SERVICE_UNAVAILABLE, e.getMessage()));
    } catch (NotFoundException e) {
      errorCounter.inc();
      ctx.writeAndFlush(
          new ResponsePacket(requestPacket.getProtocolVersion(), requestPacket.getClientRequestId(),
              requestPacket.getRequestType(), ResponseCodes.NOT_FOUND, e.getMessage()));
    } catch (InternalServerErrorException e) {
      errorCounter.inc();
      ctx.writeAndFlush(
          new ResponsePacket(requestPacket.getProtocolVersion(), requestPacket.getClientRequestId(),
              requestPacket.getRequestType(), ResponseCodes.INTERNAL_SERVER_ERROR, e.getMessage()));
    } catch (BadRequestException e) {
      errorCounter.inc();
      ctx.writeAndFlush(
          new ResponsePacket(requestPacket.getProtocolVersion(), requestPacket.getClientRequestId(),
              requestPacket.getRequestType(), ResponseCodes.BAD_REQUEST, e.getMessage()));
    } catch (Exception e) {
      errorCounter.inc();
      // all other types of errors
      logger.log(Level.SEVERE, "Request failed due to unknown error", e);
      ctx.writeAndFlush(
          new ResponsePacket(requestPacket.getProtocolVersion(), requestPacket.getClientRequestId(),
              requestPacket.getRequestType(), ResponseCodes.INTERNAL_SERVER_ERROR, e.getMessage()));
    } finally {
      inBuffer.release();
    }
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
    cause.printStackTrace();
    ctx.close();
  }

}

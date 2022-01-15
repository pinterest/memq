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

import com.pinterest.memq.client.commons2.network.ResponseHandler;
import com.pinterest.memq.commons.protocol.RequestType;
import com.pinterest.memq.commons.protocol.ResponsePacket;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MemqNettyClientSideResponseHandler extends ChannelInboundHandlerAdapter {

  private static final Logger logger = LoggerFactory.getLogger(MemqNettyClientSideResponseHandler.class);
  private ResponseHandler responseHandler;

  public MemqNettyClientSideResponseHandler(ResponseHandler responseHandler) {
    this.responseHandler = responseHandler;
  }

  @Override
  public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
    ByteBuf buf = (ByteBuf) msg;
    try {
      ResponsePacket responsePacket = new ResponsePacket();
      responsePacket.readFields(buf, RequestType.PROTOCOL_VERSION);
      logger.debug("Response received " + responsePacket);
      if (responsePacket.getProtocolVersion() != RequestType.PROTOCOL_VERSION) {
        // might not be able to handle this request.
        // in future multiple protocol versions can / should be handled here
        logger.debug("Server responded in protocol different than client request: " +responsePacket.getProtocolVersion() + " vs " + RequestType.PROTOCOL_VERSION);
      } else {
        responseHandler.handle(responsePacket);
      }
    } catch (Exception e) {
      logger.error("Failed to handle server responses: ", e);
      throw e;
    } finally {
      buf.release();
    }
  }
}

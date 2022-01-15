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

import java.util.logging.Logger;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import com.pinterest.memq.commons.protocol.ResponsePacket;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.channel.DefaultFileRegion;

public class MemqResponseEncoder extends ChannelOutboundHandlerAdapter {

  private static final Logger logger = Logger
      .getLogger(MemqResponseEncoder.class.getCanonicalName());
  private Counter responseCounter;
  private MetricRegistry registry;

  public MemqResponseEncoder(MetricRegistry registry) {
    this.registry = registry;
    responseCounter = registry.counter("response.writes");
  }

  @Override
  public void write(ChannelHandlerContext ctx,
                    Object msg,
                    ChannelPromise promise) throws Exception {
    try {
      if (msg instanceof ResponsePacket) {
        ResponsePacket response = (ResponsePacket) msg;
        // Potential TODO: find opportunity to reduce the size of this buffer allocation
        // for read responses since they are transmitted using sendFile
        int responseSize = response.getSize(response.getProtocolVersion());
        ByteBuf buffer = PooledByteBufAllocator.DEFAULT.directBuffer(0, responseSize);
        // DO NOT INCLUDE THE LENGTH OF THE FRAME IN THE PAYLOAD BECAUSE THERE IS
        // THE ABOVE ENCODER WILL TAKE CARE OF THAT
        response.write(buffer, response.getProtocolVersion());
        String responseCodeMetric = getResponseCodeMetric(response);
        registry.counter(responseCodeMetric).inc();
        logger.fine(() -> ("Response sent:" + response.getClientRequestId() + " "
            + response.getResponseCode() + " " + response.getRequestType()));
        responseCounter.inc();
        super.write(ctx, buffer, promise);
      } else if (msg instanceof DefaultFileRegion) {
        super.write(ctx, msg, promise);
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  private String getResponseCodeMetric(ResponsePacket response) {
    StringBuilder stringBuilder = new StringBuilder();
    stringBuilder.append("response.");
    stringBuilder.append(response.getRequestType().name());
    stringBuilder.append(".code.");
    stringBuilder.append(response.getResponseCode());
    return stringBuilder.toString().intern();
  }

}

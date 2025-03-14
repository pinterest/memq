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
package com.pinterest.memq.client.producer2;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

import com.codahale.metrics.MetricRegistry;
import com.pinterest.memq.client.commons.Compression;
import com.pinterest.memq.client.commons.MemqMessageHeader;
import com.pinterest.memq.client.commons.serde.ByteArraySerializer;
import com.pinterest.memq.client.commons2.MemqPooledByteBufAllocator;
import com.pinterest.memq.client.commons2.MockMemqServer;
import com.pinterest.memq.client.commons2.network.netty.ClientChannelInitializer;
import com.pinterest.memq.client.producer.MemqWriteResult;
import com.pinterest.memq.commons.protocol.RequestPacket;
import com.pinterest.memq.commons.protocol.RequestType;

import com.pinterest.memq.commons.protocol.ResponseCodes;
import com.pinterest.memq.commons.protocol.ResponsePacket;
import com.pinterest.memq.commons.protocol.WriteResponsePacket;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.internal.OutOfDirectMemoryError;
import org.junit.Before;
import org.junit.Test;

public class TestMemqProducerMemory extends TestMemqProducerBase {

//  @Test
  public void testOutOfDirectMemoryResponse() throws Exception {
    Properties props = System.getProperties();
    props.setProperty("io.netty.maxDirectMemory", Integer.toString(65536 * 4));
    props.setProperty("io.netty.allocator.numDirectArenas","2");
    props.setProperty("io.netty.allocator.maxOrder","3");
    Map<RequestType, BiConsumer<ChannelHandlerContext, RequestPacket>> map = new HashMap<>();

    byte[] largeResponseBytes = new byte[65536];
    Arrays.fill(largeResponseBytes, (byte) 100);
    setupSimpleTestServerTopicMetadataHandler(map);
    map.put(RequestType.WRITE, (ctx, req) -> {
      ctx.writeAndFlush(Unpooled.wrappedBuffer(largeResponseBytes));
    });
    ClientChannelInitializer.setWiretapper(new ChannelInboundHandlerAdapter(){
      @Override
      public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        ByteBuf buf = (ByteBuf) msg;
        byte[] bytes = new byte[buf.readableBytes()];
        buf.duplicate().readBytes(bytes);
        System.out.println(bytes.length + " " + PooledByteBufAllocator.DEFAULT.metric().usedDirectMemory());
        super.channelRead(ctx, msg);
      }
    });

    MockMemqServer mockServer = new MockMemqServer(port, map, false, false, -1, -1);
    mockServer.start();

    MemqProducer.Builder<byte[], byte[]> builder = new MemqProducer.Builder<>();
    Properties networkProperties = new Properties();
    byte[] sampleValue = new byte[8192];
    builder
        .cluster("prototype")
        .topic("test")
        .bootstrapServers(LOCALHOST_STRING + ":" + port)
        .keySerializer(new ByteArraySerializer())
        .valueSerializer(new ByteArraySerializer())
        .maxPayloadBytes(
            MemqMessageHeader.getHeaderLength() +
                RawRecord.newInstance(null, null, null, sampleValue,0).calculateEncodedLogMessageLength())
        .compression(Compression.NONE)
        .networkProperties(networkProperties)
        .metricRegistry(new MetricRegistry());

    MemqProducer<byte[], byte[]> producer = builder.build();


    try {
      ThreadLocalRandom.current().nextBytes(sampleValue);
      Future<MemqWriteResult> r =  producer.write(
          null,
          sampleValue
      );
      MemqWriteResult resp = r.get();
      fail("should throw OODM");
    } catch (ExecutionException ee) {
      assertTrue(ee.getCause() instanceof OutOfDirectMemoryError);
    }

    mockServer.stop();
  }

  /**
   * This test requires the JVM flag -XX:MaxDirectMemorySize=1048576 (1mb) to be set in the test config.
   * @throws Exception
   */
  @Test
  public void testDirectMemoryAllocationFailureOnWrite() throws Exception {
    Properties props = System.getProperties();
    props.setProperty("io.netty.maxDirectMemory", "1048576");
    props.setProperty("io.netty.allocator.numDirectArenas","2");
    props.setProperty("io.netty.allocator.maxOrder","3");
    MemqProducer.Builder<byte[], byte[]> builder = new MemqProducer.Builder<>();
    Properties networkProperties = new Properties();
    byte[] sampleValue = new byte[8192];
    int maxPayloadBytes = MemqMessageHeader.getHeaderLength() +
            RawRecord.newInstance(null, null, null, sampleValue,0).calculateEncodedLogMessageLength();
    System.out.println("maxPayloadBytes: " + maxPayloadBytes);
    builder
            .cluster("prototype")
            .topic("test")
            .bootstrapServers(LOCALHOST_STRING + ":" + 20000)
            .keySerializer(new ByteArraySerializer())
            .valueSerializer(new ByteArraySerializer())
            .maxPayloadBytes(maxPayloadBytes)
            .compression(Compression.NONE)
            .maxInflightRequests(120)
            .networkProperties(networkProperties)
            .metricRegistry(new MetricRegistry());

    MemqProducer<byte[], byte[]> producer = builder.build();

    List<Future<MemqWriteResult>> futures = new ArrayList<>();

    while (true) {
      while (futures.size() < 100) {
        ThreadLocalRandom.current().nextBytes(sampleValue);
        try {
          Future<MemqWriteResult> r =  producer.write(
                  null,
                  sampleValue
          );
          futures.add(r);
          System.out.println("client write - request buffer bytes: " + producer.getCurrentBufferSizeBytes());
          System.out.println("Direct memory used: " + MemqPooledByteBufAllocator.usedDirectMemory());
//          Thread.sleep(10);
        } catch (Exception e) {
          System.out.println("write exception: " + e);
          e.printStackTrace();
        }
      }

      int numSuccess = 0;
      int numFailures = 0;
      for (Future<MemqWriteResult> future : futures) {
        try {
          MemqWriteResult resp = future.get(10000, TimeUnit.MILLISECONDS);
          System.out.println("resp: " + resp);
          numSuccess++;
        } catch (ExecutionException e) {
          System.out.println("Future exception: " + e);
          numFailures++;
        }
      }
      System.out.println("numSuccess: " + numSuccess + ", numFailures: " + numFailures);
      futures.clear();

//      MemqWriteResult resp = r.get();
//      System.out.println("resp: " + resp);
    }

//    mockServer.stop();
  }

  public static void main(String[] args) throws Exception {
    Map<RequestType, BiConsumer<ChannelHandlerContext, RequestPacket>> map = new HashMap<>();

    short port = (short) 20000;

    setupSimpleTestServerTopicMetadataHandler(map, port);
    map.put(RequestType.WRITE, (ctx, req) -> {
      ResponsePacket resp = new ResponsePacket(req.getProtocolVersion(),
              req.getClientRequestId(), req.getRequestType(), ResponseCodes.OK, new WriteResponsePacket());
      ctx.writeAndFlush(resp);
    });

    MockMemqServer mockServer = new MockMemqServer(port, map, false, true, 50000, 1000);
    mockServer.start();

  }
}


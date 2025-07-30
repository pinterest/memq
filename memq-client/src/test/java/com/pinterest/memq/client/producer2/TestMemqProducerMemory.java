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

import com.codahale.metrics.MetricRegistry;
import com.pinterest.memq.client.commons.Compression;
import com.pinterest.memq.client.commons.MemqMessageHeader;
import com.pinterest.memq.client.commons.serde.ByteArraySerializer;
import com.pinterest.memq.client.commons2.MemoryAllocationException;
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
import org.junit.Ignore;
import org.junit.Test;

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
import java.util.function.BiConsumer;
import java.util.logging.Level;
import java.util.logging.Logger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TestMemqProducerMemory extends TestMemqProducerBase {

  private static final Logger logger = Logger.getLogger(TestMemqProducerMemory.class.getName());

  /**
   * This test simulates a scenario where the producer is trying to write
   * to the mock server which has a TrafficShapingHandler attached, resulting in
   * the server throttling the writes (inbound reads in the Netty channel). It requires the MockMemqServer
   * to be running first via the main() method of this class.
   *
   * This test is designed to be run manually after launching the MockMemqServer in the main() method of the class.
   * This is so that there are 2 separate processes running the broker and the test producer
   * itself, ensuring that memory allocation failures on the producer do not interfere with memory used by the broker's
   * processing data path.
   *
   * More specifically, the producer wants to write 100 messages, each of size 8 kB, but some writes will fail
   * due to insufficient inflight memory permits. Despite running into insufficient memory permits, we expect the producer to
   * eventually succeed in writing 100 messages, as the permits are eventually released upon broker acknowledgement of previous requests
   * in order to accommodate new writes and request creation / allocation. We assert that we indeed run into MemoryAllocationExceptions,
   * but that we still manage to write 100 messages successfully by the end.
   *
   * In other words, this test simulates a backpressure scenario resulting from broker-side congestion control throttling,
   * eventually resulting in some blocked writes due to insufficient memory permits.
   *
   * @throws Exception
   */
  @Test
  @Ignore("Run this test manually")
  public void testDirectMemoryAllocationFailureOnWrite() throws Exception {
    int maxDirectMemoryBytes = 1024 * 1024; // 1 MB
    int messageValueBytes = 8192; // 8 kB
    int maxPayloadBytes = messageValueBytes * 5; // 4 messages each request due to some additional header overhead
    int maxInflightRequests = 999999999;  // effectively unlimited for this test
    int maxInflightRequestsMemoryBytes = maxDirectMemoryBytes / 2; // 512 kB so that it runs into semaphore exhaustion before direct memory exhaustion

    MemqProducer.Builder<byte[], byte[]> builder = new MemqProducer.Builder<>();
    Properties networkProperties = new Properties();
    byte[] sampleValue = new byte[8192];
    builder
            .cluster("prototype")
            .topic("test")
            .bootstrapServers(LOCALHOST_STRING + ":" + 20000)
            .keySerializer(new ByteArraySerializer())
            .valueSerializer(new ByteArraySerializer())
            .maxPayloadBytes(maxPayloadBytes)
            .compression(Compression.NONE)
            .maxInflightRequests(maxInflightRequests)
            .maxBlockMs(5)
            .maxInflightRequestsMemoryBytes(maxInflightRequestsMemoryBytes)
            .sendRequestTimeout(60 * 1000)
            .networkProperties(networkProperties)
            .metricRegistry(new MetricRegistry());

    MemqProducer<byte[], byte[]> producer = builder.build();

    int numMessagesWritten = 0;
    List<Future<MemqWriteResult>> futures = new ArrayList<>();
    int memoryAllocationNumOccurrences = 0;
    int numMessagesToWrite = 100; // we will try to write 100 messages

    while (numMessagesWritten < numMessagesToWrite) {
      try {
        Future<MemqWriteResult> r = producer.write(
                null,
                sampleValue
        );
        futures.add(r);
        numMessagesWritten++;
      } catch (MemoryAllocationException e) {
        logger.log(Level.INFO, "Direct memory allocation failed as expected: " + e.getMessage());
        memoryAllocationNumOccurrences++;
        continue;   // this is expected, we will write until we hit
      } catch (IOException e) {
        fail("Unexpected IOException: " + e.getMessage());
      }
      logger.log(Level.INFO, "Wrote message #" + numMessagesWritten);
    }

    assertTrue(memoryAllocationNumOccurrences > 0);   // we expect at least one memory allocation failure to ensure a valid test scenario
    assertEquals(numMessagesToWrite, futures.size()); // we should have successfully written exactly as many messages as we tried to write

    List<Integer> ackLatencies = new ArrayList<>();
    for (Future<MemqWriteResult> future : futures) {
      try {
        MemqWriteResult result = future.get();
        ackLatencies.add(result.getAckLatency());
        logger.log(Level.INFO, "Write succeeded for message with client request ID: " + result.getClientRequestId());
        logger.log(Level.INFO, "Write succeeded with ack latency: " + result.getAckLatency());
      } catch (Exception e) {
        fail("Future.get() exception is unexpected; all writes should have been eventually successful");
      }
    }

    // ack latencies should generally be trending up, larger values indicate that the mock server is under congestion
    // control throttling. Check test logs to see the trend, output by the log statement below
    logger.log(Level.INFO, "Ack latencies: " + ackLatencies);
    assertTrue(ackLatencies.get(ackLatencies.size() - 1) > ackLatencies.get(0)); // last ack latency should be greater than the first one
    assertTrue(ackLatencies.get(ackLatencies.size() - 1) / ackLatencies.get(0) > 20); // last ack latency should be at least 20x the first one
  }

  // Run this first before running the testDirectMemoryAllocationFailureOnWrite
  public static void main(String[] args) throws Exception {
    int readLimit = 1024 * 512;  // this is specifically tuned to 512 kB so that testDirectMemoryAllocationFailureOnWrite will hit MemoryAllocationException but the server can still eventually handle the writes
    runMockMemqServerWithTrafficShapingHandler(readLimit);

  }

  private static void runMockMemqServerWithTrafficShapingHandler(int readLimit) throws Exception {
    Map<RequestType, BiConsumer<ChannelHandlerContext, RequestPacket>> map = new HashMap<>();

    short port = (short) 20000;

    setupSimpleTestServerTopicMetadataHandler(map, port);
    map.put(RequestType.WRITE, (ctx, req) -> {
      ResponsePacket resp = new ResponsePacket(req.getProtocolVersion(),
              req.getClientRequestId(), req.getRequestType(), ResponseCodes.OK, new WriteResponsePacket());
      ctx.writeAndFlush(resp);
    });
    MockMemqServer mockServer = new MockMemqServer(port, map, false, true, readLimit, 10);
    mockServer.start();
  }
}


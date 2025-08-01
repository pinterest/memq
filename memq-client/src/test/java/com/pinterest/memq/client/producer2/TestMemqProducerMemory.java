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
import com.pinterest.memq.client.commons.serde.ByteArraySerializer;
import com.pinterest.memq.client.commons2.MemoryAllocationException;
import com.pinterest.memq.client.commons2.MockMemqServer;
import com.pinterest.memq.client.producer.MemqWriteResult;
import com.pinterest.memq.commons.protocol.RequestPacket;
import com.pinterest.memq.commons.protocol.RequestType;
import com.pinterest.memq.commons.protocol.ResponseCodes;
import com.pinterest.memq.commons.protocol.ResponsePacket;
import com.pinterest.memq.commons.protocol.WriteResponsePacket;
import io.netty.channel.ChannelHandlerContext;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Future;
import java.util.function.BiConsumer;
import java.util.logging.Level;
import java.util.logging.Logger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TestMemqProducerMemory extends TestMemqProducerBase {

  private static final Logger logger = Logger.getLogger(TestMemqProducerMemory.class.getName());

  /**
   * This test simulates a local write scenario where the producer writes 2 messages to a local broker which has
   * TrafficShapingHandler enabled with a low read limit. This should result in the first write succeeding quickly, while
   * the second write blocks until the first write is acknowledged by the broker.
   *
   * The second write blocks because we configured the producer to create one request per message, so the second write
   * must try to create a new Request. Upon attempting to create a new request, it would have realized that the first write
   * request is still in-flight, and that it cannot create a new request until the first one is acknowledged because the max
   * inflight requests memory bytes is set to the size of one request. Therefore, the producer can only accommodate one
   * inflight request at a time.
   *
   * We set the maxBlockMs config to a high enough value so that there is sufficient time for the broker to acknowledge the first write
   * before the second write times out. The second write should block for a considerable time, but eventually succeed with a much higher ack latency.
   *
   * @throws Exception
   */
  @Test
  public void testLocalBlockedWrites() throws Exception {
    int messageValueBytes = 8192; // 8 kB
    int maxPayloadBytes = messageValueBytes + 4096; // 1 message each request due to some additional header overhead
    int maxInflightRequests = 999999999;  // effectively unlimited for this test
    int maxInflightRequestsMemoryBytes = maxPayloadBytes; // it can only accommodate one inflight request

    MockMemqServer mockMemqServer = getMockMemqServerWithTrafficShaping(10, 1000); // it should be able to accept 1 write at a time
    mockMemqServer.start();

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
            .maxBlockMs(5000) // enough time for broker to ack the first write
            .maxInflightRequestsMemoryBytes(maxInflightRequestsMemoryBytes)
            .sendRequestTimeout(60 * 1000)  // we shouldn't hit this
            .networkProperties(networkProperties)
            .metricRegistry(new MetricRegistry());

    MemqProducer<byte[], byte[]> producer = builder.build();

    // r0 write should succeed almost immediately
    Future<MemqWriteResult> r0 = producer.write(
            null,
            sampleValue
    );
    assertTrue(producer.getInflightMemoryAvailablePermits() < maxPayloadBytes); // we should not have space for another request

    // r1 write should block until r0's acknowledgement is received
    long startTime = System.currentTimeMillis();
    Future<MemqWriteResult> r1 = producer.write(
            null,
            sampleValue
    );
    long r1WriteElapsedTime = System.currentTimeMillis() - startTime;
    logger.log(Level.INFO, "Elapsed time for r1 write: " + r1WriteElapsedTime + " ms");

    assertTrue(r1WriteElapsedTime > 1000); // r1 should have blocked for a considerable time but eventually succeed

    try {
      MemqWriteResult r0WriteResult = r0.get();
      logger.log(Level.INFO,"r0 ack latency: " + r0WriteResult.getAckLatency() + " ms");

      MemqWriteResult r1WriteResult = r1.get();
      logger.log(Level.INFO,"r1 ack latency: " + r1WriteResult.getAckLatency() + " ms");

      assertTrue(r1WriteResult.getAckLatency() > r0WriteResult.getAckLatency()); // r1 should have a larger ack latency than r0
      assertTrue(r0WriteResult.getAckLatency() < r1WriteElapsedTime && (double) r0WriteResult.getAckLatency() / r1WriteElapsedTime > 0.9); // r1 should have blocked for around the same amount of time it took for r0's ack
    } catch (Exception e) {
      fail("Both writes should have succeeded");
    }

    producer.close();
    mockMemqServer.stop();
  }

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

    producer.close();
  }

  // Run this first before running the testDirectMemoryAllocationFailureOnWrite
  public static void main(String[] args) throws Exception {
    int readLimit = 1024 * 512;  // this is specifically tuned to 512 kB so that testDirectMemoryAllocationFailureOnWrite will hit MemoryAllocationException but the server can still eventually handle the writes
    MockMemqServer mockServer = getMockMemqServerWithTrafficShaping(readLimit, 10);
    mockServer.start();
  }

  private static MockMemqServer getMockMemqServerWithTrafficShaping(int readLimit, int checkInterval) {
    Map<RequestType, BiConsumer<ChannelHandlerContext, RequestPacket>> map = new HashMap<>();

    short port = (short) 20000;

    setupSimpleTestServerTopicMetadataHandler(map, port);
    map.put(RequestType.WRITE, (ctx, req) -> {
      ResponsePacket resp = new ResponsePacket(req.getProtocolVersion(),
              req.getClientRequestId(), req.getRequestType(), ResponseCodes.OK, new WriteResponsePacket());
      ctx.writeAndFlush(resp);
    });
    MockMemqServer mockServer = new MockMemqServer(port, map, false, true, readLimit, checkInterval);
    return mockServer;
  }
}


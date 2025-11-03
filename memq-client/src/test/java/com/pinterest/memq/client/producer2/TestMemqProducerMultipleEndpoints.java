package com.pinterest.memq.client.producer2;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.pinterest.memq.client.commons.MemqMessageHeader;
import com.pinterest.memq.client.commons.serde.ByteArraySerializer;
import com.pinterest.memq.client.commons2.Endpoint;
import com.pinterest.memq.client.commons2.MemqCommonClient;
import com.pinterest.memq.client.commons2.MockMemqServer;
import com.pinterest.memq.client.producer.MemqWriteResult;
import com.pinterest.memq.commons.protocol.Broker;
import com.pinterest.memq.commons.protocol.RequestPacket;
import com.pinterest.memq.commons.protocol.RequestType;
import com.pinterest.memq.commons.protocol.ResponseCodes;
import com.pinterest.memq.commons.protocol.ResponsePacket;
import com.pinterest.memq.commons.protocol.TopicAssignment;
import com.pinterest.memq.commons.protocol.TopicConfig;
import com.pinterest.memq.commons.protocol.TopicMetadata;
import com.pinterest.memq.commons.protocol.TopicMetadataRequestPacket;
import com.pinterest.memq.commons.protocol.TopicMetadataResponsePacket;
import com.pinterest.memq.commons.protocol.WriteRequestPacket;
import com.pinterest.memq.commons.protocol.WriteResponsePacket;
import com.pinterest.memq.commons.protocol.Broker.BrokerType;
import com.google.common.collect.ImmutableSet;

import io.netty.channel.ChannelHandlerContext;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;

@RunWith(Parameterized.class)
public class TestMemqProducerMultipleEndpoints extends TestMemqProducerBase {

  @Parameterized.Parameters(name = "{index}: numWriteEndpoints={0}, numBrokers={1}, numDeadBrokers={2}")
  public static Collection<Integer[]> parameters() {
    int[] numWriteEndpointsParams = {1, 2, 3};
    int[] numBrokersParams = {1, 2, 3};
    int[] numDeadBrokersParams = {0, 1, 2};
    List<Integer[]> parameters = new ArrayList<>();
    for (int numWriteEndpoints : numWriteEndpointsParams) {
      for (int numBrokers : numBrokersParams) {
        for (int numDeadBrokers : numDeadBrokersParams) {
          if (numDeadBrokers >= numBrokers) {
            continue;
          }
          parameters.add(new Integer[] {numWriteEndpoints, numBrokers, numDeadBrokers});
        }
      }
    }
    return parameters;
  }

  private final int numWriteEndpoints;
  private final int numBrokers;
  private final int numDeadBrokers;


  public TestMemqProducerMultipleEndpoints(int numWriteEndpoints, int numBrokers, int numDeadBrokers) {
    this.numWriteEndpoints = numWriteEndpoints;
    this.numBrokers = numBrokers;
    this.numDeadBrokers = numDeadBrokers;
  }

  @Test
  public void testMultipleBrokerWrites() throws Exception {
    if (numDeadBrokers > 0) {
      System.out.println("Skipping testMultipleBrokerWrites with dead brokers");
      return;
    }
    System.out.println("numWriteEndpoints: " + numWriteEndpoints + ", numBrokers: " + numBrokers + ", numDeadBrokers: " + numDeadBrokers);
    AtomicInteger[] writeCounts = new AtomicInteger[numBrokers];
    for (int i = 0; i < numBrokers; i++) {
      writeCounts[i] = new AtomicInteger();
    }

    TopicConfig topicConfig = new TopicConfig("test", "dev");
    TopicAssignment topicAssignment = new TopicAssignment(topicConfig, 100.0);

    // Return all brokers in metadata so client discovers all brokers
    Set<Broker> brokers = new HashSet<>();
    for (int i = 0; i < numBrokers; i++) {
        brokers.add(new Broker(LOCALHOST_STRING, (short) (port + i), "n/a", "n/a", BrokerType.WRITE, Collections.singleton(topicAssignment)));
    }
    
    BiConsumer<ChannelHandlerContext, RequestPacket> topicMetadataHandler = (ctx, req) -> {
      TopicMetadataRequestPacket mdPkt = (TopicMetadataRequestPacket) req.getPayload();
      ResponsePacket resp = new ResponsePacket(req.getProtocolVersion(), req.getClientRequestId(),
          req.getRequestType(), ResponseCodes.OK,
          new TopicMetadataResponsePacket(new TopicMetadata(mdPkt.getTopic(), brokers,
              ImmutableSet.of(), "dev", new Properties())));
      ctx.writeAndFlush(resp);
    };

    Map<RequestType, BiConsumer<ChannelHandlerContext, RequestPacket>>[] maps = new HashMap[numBrokers];
    for (int i = 0; i < numBrokers; i++) {
        final int idx = i;
        BiConsumer<ChannelHandlerContext, RequestPacket> writeHandler = (ctx, req) -> {
            writeCounts[idx].getAndIncrement();
            ResponsePacket resp = new ResponsePacket(req.getProtocolVersion(), req.getClientRequestId(),
                req.getRequestType(), ResponseCodes.OK, new WriteResponsePacket());
            ctx.writeAndFlush(resp);
        };
        maps[i] = new HashMap<>();
        maps[i].put(RequestType.TOPIC_METADATA, topicMetadataHandler);
        maps[i].put(RequestType.WRITE, writeHandler);
    }
    
    MockMemqServer[] mockServers = new MockMemqServer[numBrokers];
    for (int i = 0; i < numBrokers; i++) {
        mockServers[i] = new MockMemqServer(port + i, maps[i]);
        mockServers[i].start();
    }
    
    Properties networkProperties = new Properties();
    networkProperties.setProperty(MemqCommonClient.CONFIG_NUM_WRITE_ENDPOINTS, String.valueOf(numWriteEndpoints));

    int payloadSize = 
      RequestPacket.getHeaderSize() + 
      RequestPacket.getHeaderSize() + 
      WriteRequestPacket.getHeaderSize(RequestType.PROTOCOL_VERSION, "test") + 
      MemqMessageHeader.getHeaderLength() + 
      RawRecord.newInstance(null, null, null, "test1".getBytes(), 0).calculateEncodedLogMessageLength();
    
    MemqProducer.Builder<byte[], byte[]> builder = new MemqProducer.Builder<>();
    builder.cluster("prototype").topic("test")
        .bootstrapServers(LOCALHOST_STRING + ":" + port)  // Start with just first server for bootstrap
        .keySerializer(new ByteArraySerializer()).valueSerializer(new ByteArraySerializer())
        .maxPayloadBytes(payloadSize)
        .maxInflightRequests(100)
        .networkProperties(networkProperties);
    
    MemqProducer<byte[], byte[]> producer = builder.build();
    
    // Perform multiple writes to trigger round-robin behavior
    List<Future<MemqWriteResult>> results = new ArrayList<>();
    int numWrites = 100;
    for (int i = 0; i < numWrites; i++) {
      Future<MemqWriteResult> r = producer.write(null, "test1".getBytes());
      results.add(r);
    }
    
    producer.flush();

    int successCount = 0;
    
    for (Future<MemqWriteResult> r : results) {
      try {
        r.get();
        successCount++;
      } catch (Exception e) {
        System.out.println("TestTwoBrokers exception: " + e);
        e.printStackTrace();
        fail("Should not throw exception");
      }
    }

    assertEquals("Success count should be numWrites", numWrites, successCount);
    
    producer.close();
    
    // Verify that writes went to both servers
    int totalWrites = 0;
    for (int i = 0; i < numBrokers; i++) {
      totalWrites += writeCounts[i].get();
      System.out.println("Server " + i + " writes: " + writeCounts[i].get());
    }
    System.out.println("Total writes: " + totalWrites);
    assertEquals("Total writes should be numWrites", numWrites, totalWrites);
    
    // Verify that the number of distinct brokers that received writes matches numWriteEndpoints (capped at total brokers)
    int brokersWithWrites = 0;
    for (int i = 0; i < numBrokers; i++) {
      brokersWithWrites += writeCounts[i].get() > 0 ? 1 : 0;
    }
    int expectedBrokersWithWrites = Math.min(numWriteEndpoints, numBrokers);
    assertEquals("Unexpected number of brokers received writes",
        expectedBrokersWithWrites, brokersWithWrites);

    // If more than one broker received writes, verify approximate balance across them
    if (expectedBrokersWithWrites > 1) {
      List<Integer> activeBrokerWrites = new ArrayList<>();
      for (int i = 0; i < numBrokers; i++) {
        if (writeCounts[i].get() > 0) activeBrokerWrites.add(writeCounts[i].get());
      }

      // Defensive: ensure we are comparing only among active brokers
      assertEquals("Active broker count mismatch",
          expectedBrokersWithWrites, activeBrokerWrites.size());

      int minWrites = Collections.min(activeBrokerWrites);
      int maxWrites = Collections.max(activeBrokerWrites);

      int avgPerBroker = numWrites / expectedBrokersWithWrites;
      int allowedSkew = Math.max(1, (int) Math.ceil(avgPerBroker * 0.05)); // allow 5% skew or at least 1

      assertTrue(
          "Writes not approximately balanced across active brokers: min=" + minWrites +
          ", max=" + maxWrites + ", allowedSkew=" + allowedSkew,
          (maxWrites - minWrites) <= allowedSkew);
    }
        
    for (int i = 0; i < numBrokers; i++) {
      mockServers[i].stop();
    }
  }

  @Test
  public void testMultipleBrokerWritesWithDeadBrokers() throws Exception {
    if (numDeadBrokers == 0) {
      System.out.println("Skipping testMultipleBrokerWritesWithDeadBrokers with no dead brokers");
      return;
    }
    System.out.println("numWriteEndpoints: " + numWriteEndpoints + ", numBrokers: " + numBrokers + ", numDeadBrokers: " + numDeadBrokers);
    AtomicInteger[] writeCounts = new AtomicInteger[numBrokers];
    for (int i = 0; i < numBrokers; i++) {
      writeCounts[i] = new AtomicInteger();
    }

    TopicConfig topicConfig = new TopicConfig("test", "dev");
    TopicAssignment topicAssignment = new TopicAssignment(topicConfig, 100.0);

    // Return all brokers in metadata so client discovers all brokers
    Set<Broker> brokers = new HashSet<>();
    for (int i = 0; i < numBrokers; i++) {
        brokers.add(new Broker(LOCALHOST_STRING, (short) (port + i), "n/a", "n/a", BrokerType.WRITE, Collections.singleton(topicAssignment)));
    }
    
    BiConsumer<ChannelHandlerContext, RequestPacket> topicMetadataHandler = (ctx, req) -> {
      TopicMetadataRequestPacket mdPkt = (TopicMetadataRequestPacket) req.getPayload();
      ResponsePacket resp = new ResponsePacket(req.getProtocolVersion(), req.getClientRequestId(),
          req.getRequestType(), ResponseCodes.OK,
          new TopicMetadataResponsePacket(new TopicMetadata(mdPkt.getTopic(), brokers,
              ImmutableSet.of(), "dev", new Properties())));
      ctx.writeAndFlush(resp);
    };

    Map<RequestType, BiConsumer<ChannelHandlerContext, RequestPacket>>[] maps = new HashMap[numBrokers];
    for (int i = 0; i < numBrokers; i++) {
        final int idx = i;
        BiConsumer<ChannelHandlerContext, RequestPacket> writeHandler = (ctx, req) -> {
            writeCounts[idx].getAndIncrement();
            ResponsePacket resp = new ResponsePacket(req.getProtocolVersion(), req.getClientRequestId(),
                req.getRequestType(), ResponseCodes.OK, new WriteResponsePacket());
            ctx.writeAndFlush(resp);
        };
        maps[i] = new HashMap<>();
        maps[i].put(RequestType.TOPIC_METADATA, topicMetadataHandler);
        maps[i].put(RequestType.WRITE, writeHandler);
    }
    
    MockMemqServer[] mockServers = new MockMemqServer[numBrokers];
    Map<Integer, MockMemqServer> portToServerMap = new HashMap<>();
    for (int i = 0; i < numBrokers; i++) {
        mockServers[i] = new MockMemqServer(port + i, maps[i]);
        mockServers[i].start();
        portToServerMap.put(mockServers[i].getPort(), mockServers[i]);
    }
    
    Properties networkProperties = new Properties();
    networkProperties.setProperty(MemqCommonClient.CONFIG_NUM_WRITE_ENDPOINTS, String.valueOf(numWriteEndpoints));

    int payloadSize = 
      RequestPacket.getHeaderSize() + 
      RequestPacket.getHeaderSize() + 
      WriteRequestPacket.getHeaderSize(RequestType.PROTOCOL_VERSION, "test") + 
      MemqMessageHeader.getHeaderLength() + 
      RawRecord.newInstance(null, null, null, "test1".getBytes(), 0).calculateEncodedLogMessageLength();
    
    MemqProducer.Builder<byte[], byte[]> builder = new MemqProducer.Builder<>();
    builder.cluster("prototype").topic("test")
        .bootstrapServers(LOCALHOST_STRING + ":" + port)  // Start with just first server for bootstrap
        .keySerializer(new ByteArraySerializer()).valueSerializer(new ByteArraySerializer())
        .maxPayloadBytes(payloadSize)
        .maxInflightRequests(1000)
        .networkProperties(networkProperties);
    
    MemqProducer<byte[], byte[]> producer = builder.build();
    
    // Perform multiple writes to trigger round-robin behavior
    List<Future<MemqWriteResult>> results = new ArrayList<>();
    List<Integer> writePorts = new ArrayList<>();
    Set<Integer> portsToKill = new HashSet<>();
    boolean killScheduled = false;
    int numWrites = 200;
    for (int i = 0; i < numWrites; i++) {
      Future<MemqWriteResult> r = producer.write(null, "test1".getBytes());
      results.add(r);
      System.out.println("Written " + i + " records");
      Thread.sleep(10);
      if (writePorts.size() < numDeadBrokers) {
        for (Endpoint e : producer.getWriteEndpoints()) {
          if (!writePorts.contains(e.getAddress().getPort())) {
            writePorts.add(e.getAddress().getPort());
          }
        }
      } else {
        // kill up to numDeadBrokers write ports
        for (int j = 0; j < numDeadBrokers; j++) {
          portsToKill.add(writePorts.get(j));
        }
        if (!killScheduled) {
            for (int port : portsToKill) {
                new ScheduledThreadPoolExecutor(1).schedule(() -> {
                    try {
                        System.out.println("Killing server on port " + port);
                        portToServerMap.get(port).stop();
                    } catch (Exception e) {
                        fail("Failed to stop server on port " + port);
                    }
                }, 100, TimeUnit.MILLISECONDS);
            }
            killScheduled = true;
        }
      }
    }
    
    producer.flush();

    int successCount = 0;
    
    for (Future<MemqWriteResult> r : results) {
      try {
        r.get();
        successCount++;
      } catch (Exception e) {
        System.out.println("TestMultipleBrokerWritesWithDeadBrokers exception: " + e);
        e.printStackTrace();
        fail("Should not throw exception");
      }
    }

    assertEquals("Success count should be numWrites", numWrites, successCount);
    
    producer.close();
    
    // Verify that writes went to both servers
    int totalWrites = 0;
    for (int i = 0; i < numBrokers; i++) {
      totalWrites += writeCounts[i].get();
      System.out.println("Server " + i + " writes: " + writeCounts[i].get());
    }
    System.out.println("Total writes: " + totalWrites);
    assertEquals("Total writes should be numWrites", numWrites, totalWrites);
    
    // Verify that the number of distinct brokers that received writes matches numWriteEndpoints (capped at total brokers)
    int brokersWithWrites = 0;
    for (int i = 0; i < numBrokers; i++) {
      brokersWithWrites += writeCounts[i].get() > 0 ? 1 : 0;
    }
    int minNumBrokersWithWrites = Math.min(numWriteEndpoints, numBrokers);
    assertTrue("Unexpected number of brokers received writes",
        minNumBrokersWithWrites <= brokersWithWrites);
        
    for (int i = 0; i < numBrokers; i++) {
      mockServers[i].stop();
    }
  }





}

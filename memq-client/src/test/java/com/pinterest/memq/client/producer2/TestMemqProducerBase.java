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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;

import org.junit.Before;
import org.junit.BeforeClass;

import com.google.common.collect.ImmutableSet;
import com.pinterest.memq.client.commons2.MockMemqServer;
import com.pinterest.memq.commons.protocol.Broker;
import com.pinterest.memq.commons.protocol.Broker.BrokerType;
import com.pinterest.memq.commons.protocol.RequestPacket;
import com.pinterest.memq.commons.protocol.RequestType;
import com.pinterest.memq.commons.protocol.ResponseCodes;
import com.pinterest.memq.commons.protocol.ResponsePacket;
import com.pinterest.memq.commons.protocol.TopicAssignment;
import com.pinterest.memq.commons.protocol.TopicConfig;
import com.pinterest.memq.commons.protocol.TopicMetadata;
import com.pinterest.memq.commons.protocol.TopicMetadataRequestPacket;
import com.pinterest.memq.commons.protocol.TopicMetadataResponsePacket;
import com.pinterest.memq.commons.protocol.WriteResponsePacket;

import io.netty.channel.ChannelHandlerContext;
import io.netty.util.ResourceLeakDetector;

public class TestMemqProducerBase {
  protected static final String LOCALHOST_STRING = "127.0.0.1";
  protected short port = -1;

  @BeforeClass
  public static void setup() {
    ResourceLeakDetector.setLevel(ResourceLeakDetector.Level.PARANOID);
  }

  @Before
  public void generateRandomPort() {
    short newPort = -1;
    while (port == newPort) {
      newPort = (short) ThreadLocalRandom.current().nextInt(20000, 30000);
    }
    port = newPort;
  }

  protected MockMemqServer newSimpleTestServer(AtomicInteger writeCount) {
    Map<RequestType, BiConsumer<ChannelHandlerContext, RequestPacket>> map = new HashMap<>();

    setupSimpleTestServerTopicMetadataHandler(map);
    map.put(RequestType.WRITE, (ctx, req) -> {
      writeCount.getAndIncrement();
      ResponsePacket resp = new ResponsePacket(req.getProtocolVersion(), req.getClientRequestId(),
          req.getRequestType(), ResponseCodes.OK, new WriteResponsePacket());
      ctx.writeAndFlush(resp);
    });

    return new MockMemqServer(port, map);
  }

  protected void setupSimpleTestServerTopicMetadataHandler(Map<RequestType, BiConsumer<ChannelHandlerContext, RequestPacket>> map) {
    map.put(RequestType.TOPIC_METADATA, (ctx, req) -> {
      TopicMetadataRequestPacket mdPkt = (TopicMetadataRequestPacket) req.getPayload();
      TopicConfig topicConfig = new TopicConfig("test", "dev");
      TopicAssignment topicAssignment = new TopicAssignment(topicConfig, 100.0);
      Set<Broker> brokers = Collections.singleton(new Broker(LOCALHOST_STRING, port, "n/a", "n/a",
          BrokerType.WRITE, Collections.singleton(topicAssignment)));
      ResponsePacket resp = new ResponsePacket(req.getProtocolVersion(), req.getClientRequestId(),
          req.getRequestType(), ResponseCodes.OK,
          new TopicMetadataResponsePacket(new TopicMetadata(mdPkt.getTopic(), brokers,
              ImmutableSet.of(), "dev", new Properties())));
      ctx.writeAndFlush(resp);
    });
  }

}

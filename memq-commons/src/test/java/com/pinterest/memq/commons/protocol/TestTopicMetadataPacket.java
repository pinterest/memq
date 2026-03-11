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
package com.pinterest.memq.commons.protocol;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;

import org.junit.Test;

import com.pinterest.memq.commons.protocol.Broker.BrokerType;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;

public class TestTopicMetadataPacket {

  @Test
  public void testTopicMetadataRequest() throws IOException {
    TopicMetadataRequestPacket request = new TopicMetadataRequestPacket("test");
    ByteBuf buf = PooledByteBufAllocator.DEFAULT.buffer();
    request.write(buf, RequestType.PROTOCOL_VERSION);

    request = new TopicMetadataRequestPacket();
    request.readFields(buf, RequestType.PROTOCOL_VERSION);
    assertEquals("test", request.getTopic());
  }

  @Test
  public void testTopicMetadataResponse() throws IOException {
    Properties storageProperties = new Properties();
    storageProperties.setProperty("prop1", "xyz");
    storageProperties.setProperty("prop2", String.valueOf(212));
    TopicMetadata md = new TopicMetadata("test23", "delayeddevnull", storageProperties);
    assertEquals(0, md.getWriteBrokers().size());
    md.getWriteBrokers().add(new Broker("127.0.0.1", (short) 9092, "2xl", "us-east-1a",
        BrokerType.WRITE, new HashSet<>()));
    TopicMetadataResponsePacket response = new TopicMetadataResponsePacket(md);
    ByteBuf buf = PooledByteBufAllocator.DEFAULT.buffer();
    response.write(buf, RequestType.PROTOCOL_VERSION);

    response = new TopicMetadataResponsePacket();
    response.readFields(buf, RequestType.PROTOCOL_VERSION);
    TopicMetadata metadata = response.getMetadata();
    assertEquals("test23", metadata.getTopicName());
    assertEquals("delayeddevnull", metadata.getStorageHandlerName());

    assertEquals(2, metadata.getStorageHandlerConfig().size());
    assertEquals(1, metadata.getWriteBrokers().size());

    md = new TopicMetadata("test23", "delayeddevnull", new Properties());
    buf = PooledByteBufAllocator.DEFAULT.buffer();
    response.write(buf, RequestType.PROTOCOL_VERSION);
    response = new TopicMetadataResponsePacket();
    response.readFields(buf, RequestType.PROTOCOL_VERSION);
    metadata = response.getMetadata();
    assertEquals("test23", metadata.getTopicName());
    assertEquals("delayeddevnull", metadata.getStorageHandlerName());
  }

  @Test
  public void testListTopicsRoundTrip() throws IOException {
    // empty request = list all topics
    TopicMetadataRequestPacket request = new TopicMetadataRequestPacket(Collections.emptyList());
    ByteBuf reqBuf = PooledByteBufAllocator.DEFAULT.buffer();
    request.write(reqBuf, RequestType.PROTOCOL_VERSION);

    request = new TopicMetadataRequestPacket();
    request.readFields(reqBuf, RequestType.PROTOCOL_VERSION);
    assertTrue(request.getTopics().isEmpty());
    reqBuf.release();

    // response with multiple TopicMetadata objects
    TopicMetadata mdA = new TopicMetadata("topicA", "handlerA", new Properties());
    TopicMetadata mdB = new TopicMetadata("topicB", "handlerB", new Properties());
    TopicMetadata mdC = new TopicMetadata("topicC", "handlerC", new Properties());
    List<TopicMetadata> allMetadata = Arrays.asList(mdA, mdB, mdC);

    TopicMetadataResponsePacket response = new TopicMetadataResponsePacket(allMetadata);
    ByteBuf respBuf = PooledByteBufAllocator.DEFAULT.buffer();
    response.write(respBuf, RequestType.PROTOCOL_VERSION);

    response = new TopicMetadataResponsePacket();
    response.readFields(respBuf, RequestType.PROTOCOL_VERSION);

    List<TopicMetadata> metadataList = response.getMetadataList();
    assertEquals(3, metadataList.size());
    assertEquals("topicA", metadataList.get(0).getTopicName());
    assertEquals("topicB", metadataList.get(1).getTopicName());
    assertEquals("topicC", metadataList.get(2).getTopicName());
    respBuf.release();
  }

  @Test
  public void testBackwardCompatSingleTopicRequestReadByNewBroker() throws IOException {
    // old client writes a single topic string
    TopicMetadataRequestPacket oldRequest = new TopicMetadataRequestPacket("myTopic");
    ByteBuf buf = PooledByteBufAllocator.DEFAULT.buffer();
    oldRequest.write(buf, RequestType.PROTOCOL_VERSION);

    // new broker reads it — should see exactly 1 topic
    TopicMetadataRequestPacket newBrokerRead = new TopicMetadataRequestPacket();
    newBrokerRead.readFields(buf, RequestType.PROTOCOL_VERSION);
    assertEquals(1, newBrokerRead.getTopics().size());
    assertEquals("myTopic", newBrokerRead.getTopic());
    buf.release();
  }

  @Test
  public void testBackwardCompatSingleResponseReadByNewClient() throws IOException {
    // old broker writes a single TopicMetadata
    TopicMetadata md = new TopicMetadata("test", "handler", new Properties());
    TopicMetadataResponsePacket oldResponse = new TopicMetadataResponsePacket(md);
    ByteBuf buf = PooledByteBufAllocator.DEFAULT.buffer();
    oldResponse.write(buf, RequestType.PROTOCOL_VERSION);

    // new client reads it — should see exactly 1 metadata via getMetadataList()
    TopicMetadataResponsePacket newClientRead = new TopicMetadataResponsePacket();
    newClientRead.readFields(buf, RequestType.PROTOCOL_VERSION);
    assertEquals(1, newClientRead.getMetadataList().size());
    assertEquals("test", newClientRead.getMetadata().getTopicName());
    buf.release();
  }

}

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
package com.pinterest.memq.commons.storage;

import com.pinterest.memq.client.commons.CommonConfigs;
import com.pinterest.memq.client.commons.ConsumerConfigs;
import com.pinterest.memq.client.commons2.DataNotFoundException;
import com.pinterest.memq.client.commons2.Endpoint;
import com.pinterest.memq.client.commons2.MemqCommonClient;
import com.pinterest.memq.client.commons2.TopicNotFoundException;
import com.pinterest.memq.client.commons2.network.NetworkClient;
import com.pinterest.memq.commons.BatchHeader;
import com.pinterest.memq.commons.protocol.BatchData;
import com.pinterest.memq.commons.protocol.ReadRequestPacket;
import com.pinterest.memq.commons.protocol.ReadResponsePacket;
import com.pinterest.memq.commons.protocol.RequestPacket;
import com.pinterest.memq.commons.protocol.RequestType;
import com.pinterest.memq.commons.protocol.ResponseCodes;
import com.pinterest.memq.commons.protocol.ResponsePacket;

import com.codahale.metrics.MetricRegistry;
import com.google.gson.JsonObject;

import java.io.IOException;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

public abstract class ReadBrokerStorageHandler implements StorageHandler {

  private Logger logger = Logger.getLogger(ReadBrokerStorageHandler.class.getName());
  private long connectTimeout = 500;
  private int maxReadAttempts = 3;
  private boolean localRead;
  private MemqCommonClient client;

  @Override
  public void initReader(Properties properties, MetricRegistry registry) throws Exception {
    StorageHandler.super.initReader(properties, registry);
    localRead = Boolean.parseBoolean(properties.getProperty("read.local.enabled", "true"));
    if (!isLocalRead()) {
      String topic = properties.getProperty(ConsumerConfigs.TOPIC_INTERNAL_PROP);
      String bootstrapServers = properties.getProperty(ConsumerConfigs.BOOTSTRAP_SERVERS);
      properties.setProperty(NetworkClient.CONFIG_CONNECT_TIMEOUT_MS, "300000");
      List<Endpoint> endpoints = MemqCommonClient
          .getEndpointsFromBootstrapServerString(bootstrapServers);
      // Attempt to use locality provided by the configuring reader, if locality is
      // missing then the underlying network client will ignore it. Using locality can
      // prevent X-AZ network costs when read brokers are used.
      MemqCommonClient client = new MemqCommonClient(properties.getProperty(CommonConfigs.CLIENT_LOCALITY, ""), null,
          properties);
      setClient(client);
      getClient().initialize(endpoints);
      // use topic metadata to find the read brokers for this topic and then reconnect
      getClient().reconnect(topic, true);
    }
  }

  protected long getConnectTimeout() {
    return connectTimeout;
  }

  protected void setConnectTimeout(long connectTimeout) {
    this.connectTimeout = connectTimeout;
  }

  protected int getMaxReadAttempts() {
    return maxReadAttempts;
  }

  protected void setMaxReadAttempts(int maxReadAttempts) {
    this.maxReadAttempts = maxReadAttempts;
  }

  protected boolean isLocalRead() {
    return localRead;
  }

  protected void setLocalRead(boolean localRead) {
    this.localRead = localRead;
  }

  protected void setClient(MemqCommonClient client) {
    this.client = client;
  }

  protected MemqCommonClient getClient() {
    return client;
  }

  protected BatchData readBatchHeader(String topic, JsonObject notification) throws Exception {
    return readBatch(topic, notification, true,
        new BatchHeader.IndexEntry(ReadRequestPacket.DISABLE_READ_AT_INDEX,
            ReadRequestPacket.DISABLE_READ_AT_INDEX),
        connectTimeout, 0);
  }

  protected BatchData readBatch(String topic, JsonObject notification) throws Exception {
    return readBatch(topic, notification, false,
        new BatchHeader.IndexEntry(ReadRequestPacket.DISABLE_READ_AT_INDEX,
            ReadRequestPacket.DISABLE_READ_AT_INDEX),
        connectTimeout, 0);
  }

  protected BatchData readBatchAtIndex(String topic,
                                    JsonObject notification,
                                    BatchHeader.IndexEntry entry) throws Exception {
    return readBatch(topic, notification, false, entry, connectTimeout, 0);
  }

  protected BatchData readBatch(String topic,
                             JsonObject notification,
                             boolean readHeaderOnly,
                             BatchHeader.IndexEntry entry,
                             long timeoutMillis,
                             int attempts) throws Exception {
    Future<ResponsePacket> response = client.sendRequestPacketAndReturnResponseFuture(
        new RequestPacket(RequestType.PROTOCOL_VERSION, ThreadLocalRandom.current().nextLong(),
            RequestType.READ, new ReadRequestPacket(topic, notification, readHeaderOnly, entry)),
        timeoutMillis);
    ResponsePacket responsePacket = response.get(timeoutMillis, TimeUnit.MILLISECONDS);
    if (responsePacket.getResponseCode() == ResponseCodes.OK) {
      return ((ReadResponsePacket) responsePacket.getPacket()).getBatchData();
    } else if (responsePacket.getResponseCode() == ResponseCodes.NO_DATA) {
      throw new DataNotFoundException("Request failed");
    } else if (responsePacket.getResponseCode() == ResponseCodes.NOT_FOUND) {
      throw new TopicNotFoundException("Topic " + topic + " not found");
    } else if (responsePacket.getResponseCode() == ResponseCodes.REDIRECT) {
      if (attempts > maxReadAttempts) {
        throw new Exception("Retries exhausted for reading: " + notification);
      }
      client.reconnect(topic, true);
      return readBatch(topic, notification, readHeaderOnly, entry, timeoutMillis, attempts + 1);
    } else {
      throw new ExecutionException("Request failed, code:" + responsePacket.getResponseCode()
          + " error:" + responsePacket.getErrorMessage(), null);
    }
  }

  @Override
  public void closeReader() {
    StorageHandler.super.closeReader();
    if (client != null) {
      try {
        client.close();
      } catch (IOException e) {
        logger.log(Level.SEVERE, "Failed to close client", e);
      }
    }
  }
}

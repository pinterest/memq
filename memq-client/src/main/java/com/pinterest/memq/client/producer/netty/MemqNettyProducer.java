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
package com.pinterest.memq.client.producer.netty;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.pinterest.memq.client.commons.Compression;
import com.pinterest.memq.client.commons2.Endpoint;
import com.pinterest.memq.client.commons2.MemqCommonClient;
import com.pinterest.memq.client.producer.MemqProducer;
import com.pinterest.memq.client.producer.TaskRequest;
import com.pinterest.memq.client.producer.http.DaemonThreadFactory;
import com.pinterest.memq.commons.config.SSLConfig;
import com.pinterest.memq.commons.protocol.Broker;
import com.pinterest.memq.commons.protocol.TopicMetadata;

public class MemqNettyProducer<H, T> extends MemqProducer<H, T> {

  private static final Logger logger = LoggerFactory.getLogger(MemqNettyProducer.class);
  private ExecutorService es;
  private boolean debug;
  private MemqCommonClient memqCommonClient;

  public MemqNettyProducer(String cluster,
                           String serversetFile,
                           String topicName,
                           int maxInflightRequest,
                           int maxPayLoadBytes,
                           Compression compression,
                           boolean disableAcks,
                           int ackCheckPollIntervalMs,
                           int requestTimeout,
                           String locality,
                           int requestAckTimeout,
                           Properties auditorConfig,
                           SSLConfig sslConfig) throws Exception {
    super(cluster, topicName, maxInflightRequest, maxPayLoadBytes, compression, disableAcks,
        ackCheckPollIntervalMs, locality, requestAckTimeout, auditorConfig, sslConfig);
    logger
        .warn("Creating MemqNetty with url:" + serversetFile + " maxPayLoadBytes:" + maxPayLoadBytes
            + " maxInflightRequests:" + maxInflightRequest + " compression:" + compression);
    this.es = Executors.newFixedThreadPool(maxInflightRequests,
        new DaemonThreadFactory("MemqRequestPool-" + topicName));

    // update topic metadata
    try (MemqCommonClient metadataClient = new MemqCommonClient(locality, sslConfig, null)) {
      metadataClient.initialize(MemqCommonClient.parseServersetFile(serversetFile));
      TopicMetadata topicMetadata = metadataClient.getTopicMetadata(topicName, 10000);
      Set<Broker> brokers = topicMetadata.getWriteBrokers();
      logger.info("Fetched topic metadata, now reconnecting to one of serving brokers:" + brokers);
      this.memqCommonClient = new MemqCommonClient(locality, sslConfig, null);
      memqCommonClient.initialize(MemqCommonClient.generateEndpointsFromBrokers(brokers));
    }
  }
  
  public MemqNettyProducer(String cluster,
                           Set<Broker> bootstrapServers,
                           String topicName,
                           int maxInflightRequest,
                           int maxPayLoadBytes,
                           Compression compression,
                           boolean disableAcks,
                           int ackCheckPollIntervalMs,
                           int requestTimeout,
                           String locality,
                           int requestAckTimeout,
                           Properties auditorConfig,
                           SSLConfig sslConfig) throws Exception {
    super(cluster, topicName, maxInflightRequest, maxPayLoadBytes, compression, disableAcks,
        ackCheckPollIntervalMs, locality, requestAckTimeout, auditorConfig, sslConfig);
    logger
        .warn("Creating MemqNetty with servers:" + bootstrapServers + " maxPayLoadBytes:" + maxPayLoadBytes
            + " maxInflightRequests:" + maxInflightRequest + " compression:" + compression);
    this.es = Executors.newFixedThreadPool(maxInflightRequests,
        new DaemonThreadFactory("MemqRequestPool-" + topicName));

    // update topic metadata
    try (MemqCommonClient metadataClient = new MemqCommonClient(locality, sslConfig, null)) {
      metadataClient.initialize(MemqCommonClient.generateEndpointsFromBrokers(bootstrapServers));
      TopicMetadata topicMetadata = metadataClient.getTopicMetadata(topicName, 10000);
      Set<Broker> brokers = topicMetadata.getWriteBrokers();
      logger.info("Fetched topic metadata, now reconnecting to one of serving brokers:" + brokers);
      this.memqCommonClient = new MemqCommonClient(locality, sslConfig, null);
      memqCommonClient.initialize(MemqCommonClient.generateEndpointsFromBrokers(brokers));
    }
  }

  public static InetSocketAddress tryAndGetAZLocalServer(String serversetFile,
                                                         String locality) throws IOException {
    List<InetSocketAddress> azLocalEndPoints = getLocalServers(serversetFile, locality);
    int randomServer = ThreadLocalRandom.current().nextInt(azLocalEndPoints.size());
    return azLocalEndPoints.get(randomServer);
  }

  public static List<InetSocketAddress> getLocalServers(String serversetFile,
                                                        String locality) throws IOException {
    List<JsonObject> servers = parseServerSetFile(serversetFile);
    if (servers.isEmpty()) {
      throw new IOException("No servers available from serverset:" + serversetFile);
    }
    List<JsonObject> azLocalEndPoints = servers.stream()
        .filter(v -> v.get("az").getAsString().equalsIgnoreCase(locality))
        .collect(Collectors.toList());
    if (azLocalEndPoints.isEmpty()) {
      logger.warn("Not using AZ awareness due to missing local memq servers for:" + serversetFile
          + " local az:" + locality);
      azLocalEndPoints = servers;
    }
    return azLocalEndPoints.stream()
        .map(ep -> InetSocketAddress.createUnresolved(ep.get("ip").getAsString(), 9092))
        .collect(Collectors.toList());
  }

  public static List<JsonObject> parseServerSetFile(String serversetFile) throws IOException {
    Gson gson = new Gson();
    List<String> lines = Files.readAllLines(new File(serversetFile).toPath());
    return lines.stream().map(line -> gson.fromJson(line, JsonObject.class))
        .filter(g -> g.entrySet().size() > 0).collect(Collectors.toList());
  }

  public MemqNettyProducer(String cluster,
                           InetSocketAddress suppliedServer,
                           String topicName,
                           int maxInflightRequest,
                           int maxPayLoadBytes,
                           Compression compression,
                           boolean disableAcks,
                           int ackCheckPollIntervalMs,
                           int requestTimeout,
                           String locality,
                           int requestAckTimeout,
                           Properties auditorConfig,
                           SSLConfig sslConfig) throws Exception {
    super(cluster, topicName, maxInflightRequest, maxPayLoadBytes, compression, disableAcks,
        ackCheckPollIntervalMs, locality, requestAckTimeout, auditorConfig, sslConfig);
    logger.warn("DEBUG Creating MemqNettyProducer with server" + suppliedServer
        + " maxPayLoadBytes:" + maxPayLoadBytes + " maxInflightRequests:" + maxInflightRequest
        + " compression:" + compression);
    this.es = Executors.newFixedThreadPool(maxInflightRequests,
        new DaemonThreadFactory("MemqRequestPool-" + topicName));
    this.memqCommonClient = new MemqCommonClient(locality, sslConfig, null);
    memqCommonClient.initialize(Collections.singletonList(new Endpoint(suppliedServer)));
  }

  @Override
  public synchronized TaskRequest warmAndGetRequestEntry(Long requestId) throws IOException {
    TaskRequest request = requestMap.get(requestId);
    if (request == null) {
      logger.debug("Making request waiting for semaphore:" + requestId);
      try {
        maxRequestLock.acquire();
      } catch (InterruptedException e) {
        throw new IOException("Failed to acquire request lock", e);
      }
      currentRequestTask = new MemqNettyRequest(topicName, requestId, compression, maxRequestLock,
          disableAcks, maxPayLoadBytes, ackCheckPollIntervalMs, requestMap, this, requestAckTimeout,
          debug);
      request = currentRequestTask;
      requestMap.put(requestId, currentRequestTask);
      currentRequest = es.submit(currentRequestTask);
    }
    return request;
  }

  public MemqCommonClient getMemqCommonClient() {
    return memqCommonClient;
  }

  public void setDebug() {
    this.debug = true;
  }

  @Override
  public boolean isClosed() {
    return memqCommonClient.isClosed();
  }

  @VisibleForTesting
  protected ExecutorService getEs() {
    return es;
  }

  @Override
  public void close() throws IOException {
    try {
      memqCommonClient.close();
      if (es != null && !es.isShutdown()) {
        es.shutdownNow();
      }
      if (auditor != null) {
        auditor.close();
      }
    } catch (Exception e) {
      throw new IOException("Interrupted closing request", e);
    }
  }

  public long getEpoch() {
    return epoch;
  }

  @Override
  public TopicMetadata getTopicMetadata(String topic, Duration timeout) throws Exception {
    return memqCommonClient.getTopicMetadata(topic, timeout.toMillis());
  }

}

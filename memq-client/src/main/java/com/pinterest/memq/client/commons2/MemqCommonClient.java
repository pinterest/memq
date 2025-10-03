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
package com.pinterest.memq.client.commons2;

import static com.pinterest.memq.client.commons2.Endpoint.DEFAULT_LOCALITY;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.pinterest.memq.client.commons2.network.NetworkClient;
import com.pinterest.memq.commons.config.SSLConfig;
import com.pinterest.memq.commons.protocol.Broker;
import com.pinterest.memq.commons.protocol.RequestPacket;
import com.pinterest.memq.commons.protocol.RequestType;
import com.pinterest.memq.commons.protocol.ResponseCodes;
import com.pinterest.memq.commons.protocol.ResponsePacket;
import com.pinterest.memq.commons.protocol.TopicMetadata;
import com.pinterest.memq.commons.protocol.TopicMetadataRequestPacket;
import com.pinterest.memq.commons.protocol.TopicMetadataResponsePacket;

public class MemqCommonClient implements Closeable {

  private static final Logger logger = LoggerFactory.getLogger(MemqCommonClient.class);
  private static final int MAX_SEND_RETRIES = 3;

  public static final String CONFIG_NUM_WRITE_ENDPOINTS = "numWriteEndpoints"; // number of endpoints for writes

  private final NetworkClient networkClient;
  private long connectTimeout = 500;
  private int numWriteEndpoints = 1;

  private String locality = DEFAULT_LOCALITY;
  private List<Endpoint> localityEndpoints;
  private List<Endpoint> writeEndpoints;
  private Map<Endpoint, Integer> failureCounts;

  protected MemqCommonClient(SSLConfig sslConfig, Properties networkProperties) {
    if (networkProperties != null) {
      if (networkProperties.containsKey(NetworkClient.CONFIG_CONNECT_TIMEOUT_MS)) {
        this.connectTimeout = Long
            .parseLong(networkProperties.getProperty(NetworkClient.CONFIG_CONNECT_TIMEOUT_MS));
      }
      if (networkProperties.containsKey(CONFIG_NUM_WRITE_ENDPOINTS)) {
        this.numWriteEndpoints = Integer.parseInt(networkProperties.getProperty(CONFIG_NUM_WRITE_ENDPOINTS));
      }
    }
    writeEndpoints = new ArrayList<>();
    failureCounts = new ConcurrentHashMap<>();
    networkClient = new NetworkClient(networkProperties, sslConfig);
  }

  public MemqCommonClient(String locality, SSLConfig sslConfig, Properties networkProperties) {
    this(sslConfig, networkProperties);
    this.locality = locality;
  }

  public void initialize(List<Endpoint> endpoints) throws Exception {
    resetEndpoints(endpoints);
  }

  public void resetEndpoints(List<Endpoint> endpoints) throws Exception {
    this.localityEndpoints = getLocalityEndpoints(endpoints);
    validateEndpoints();
  }

  private void validateEndpoints() throws Exception {
    if (localityEndpoints.isEmpty()) {
      throw new Exception("No endpoints available");
    }
  }

  /**
   * Send a request packet and return a future for the response.
   * 
   * The choice of endpoint to try is based on the logic in getEndpointsToTry(), see method javadoc for more details
   * on how the endpoints are rotated and chosen.
   * 
   * If the request fails before reaching the retry limit, the write endpoints are refreshed to deprioritize the dead endpoint.
   * If the request fails after reaching the retry limit, the exception is propagated without further refreshing.
   * 
   * For example:
   * <pre>
   * {@code
   * numEndpoints = 3
   * localityEndpoints = [A, B, C, D, E, F]
   * writeEndpoints = [A, B, C]
   * 
   * Endpoint A is dead, all other endpoints are alive.
   * 
   * getEndpointsToTry() returns [A, B, C, D, E, F]
   * sendRequestPacketAndReturnResponseFuture() --> try endpoint A --> fail
   * refreshWriteEndpoints() --> writeEndpoints = [D, C, F]
   * getEndpointsToTry() returns [D, C, F, E, B, A] (shuffled)
   * sendRequestPackeAndReturnResponseFuture() --> try endpoint D --> succeed
   * ...
   * }
   * </pre>
   * 
   * @param request
   * @param topic
   * @param timeoutMillis
   * @return the CompletableFuture for the response
   * @throws InterruptedException
   * @throws TimeoutException
   * @throws ExecutionException
   */
  public CompletableFuture<ResponsePacket> sendRequestPacketAndReturnResponseFuture(RequestPacket request,
                                                                                    String topic,
                                                                                    long timeoutMillis) throws InterruptedException,
                                                                                                        TimeoutException,
                                                                                                        ExecutionException {
    if (localityEndpoints == null || writeEndpoints == null || localityEndpoints.isEmpty()) {
      throw new IllegalStateException("Client not initialized yet");
    }
    CompletableFuture<ResponsePacket> future = null;
    List<Endpoint> endpointsToTry = getEndpointsToTry();

    long elapsed = 0;
    long start = System.currentTimeMillis();
    int retryCount = Math.min(MAX_SEND_RETRIES, endpointsToTry.size());
    for (int retry = 0; retry < retryCount; retry++) {
      if (elapsed > timeoutMillis) {
        throw new TimeoutException("Failed to send after " + timeoutMillis + " ms");
      }
      Endpoint endpoint = endpointsToTry.get(retry);
      // System.out.println("Trying endpoint " + endpoint + " for topic " + topic);
      try {
        future = networkClient.send(endpoint.getAddress(), request,
            Duration.ofMillis(timeoutMillis - elapsed));
        maybeRegisterWriteEndpoint(endpoint, topic);
        break;
      } catch (ExecutionException e) {
        if (e.getCause() instanceof ConnectException) {
          if (retry == retryCount - 1) {
            logger.error("Failed to send request packet for topic=" + topic, e);
            throw e;
          } else {
            logger.warn("Retrying send request after failure for topic=" + topic, e);
            try {
              deprioritizeDeadEndpoint(endpoint, topic);  // this endpoint is down even after retries in NetworkClient, remove it from the write endpoints and take another one from locality endpoints
            } catch (Exception ex) {
              logger.error("Failed to refresh write endpoints", e);
              throw e;
            }
          }
        } else {
          throw e;
        }
      } finally {
        elapsed = System.currentTimeMillis() - start;
      }
    }
    if (future == null) {
      future = new CompletableFuture<>();
      future.completeExceptionally(new Exception("No suitable endpoints"));
    }
    return future;
  }

  public List<Endpoint> currentWriteEndpoints() {
    return writeEndpoints;
  }

  public TopicMetadata getTopicMetadata(String topic,
                                        long timeoutMillis) throws TopicNotFoundException,
                                                            ExecutionException,
                                                            InterruptedException, TimeoutException {
    Future<ResponsePacket> response = sendRequestPacketAndReturnResponseFuture(
        new RequestPacket(RequestType.PROTOCOL_VERSION, ThreadLocalRandom.current().nextLong(),
            RequestType.TOPIC_METADATA, new TopicMetadataRequestPacket(topic)),
        topic,
        timeoutMillis);
    ResponsePacket responsePacket = response.get(timeoutMillis, TimeUnit.MILLISECONDS);
    if (responsePacket.getResponseCode() == ResponseCodes.NOT_FOUND) {
      throw new TopicNotFoundException("Topic " + topic + " not found");
    }
    TopicMetadataResponsePacket resp = ((TopicMetadataResponsePacket) responsePacket.getPacket());
    writeEndpoints.clear();
    // System.out.println("Finished getting topic metadata");
    return resp.getMetadata();
  }

  public TopicMetadata getTopicMetadata(String topic) throws TopicNotFoundException,
                                                      ExecutionException, InterruptedException,
                                                      TimeoutException {
    return getTopicMetadata(topic, connectTimeout);
  }

  public synchronized void reconnect(String topic, boolean isConsumer) throws Exception {
    logger.warn("Reconnecting topic " + topic);

    TopicMetadata md = getTopicMetadata(topic, connectTimeout);
    networkClient.reset();
    Set<Broker> brokers = null;
    if (isConsumer) {
      brokers = md.getReadBrokers();
    } else {
      brokers = md.getWriteBrokers();
    }
    localityEndpoints = getLocalityEndpoints(
        brokers.stream().map(Endpoint::fromBroker).collect(Collectors.toList()));
    validateEndpoints();
  }

  protected List<Endpoint> randomizedEndpoints(List<Endpoint> servers) {
    List<Endpoint> shuffle = new ArrayList<>(servers);
    Collections.shuffle(shuffle);
    return shuffle;
  }

  /**
   * Get the endpoints to try for a given request, ordered by priority in the following way:<br>
   * 1. N rotated write endpoints, where N = numWriteEndpoints (config) and the write endpoints are rotated by 1 after each call<br>
   * 2. Remaining locality endpoints in a random order<br>
   * 
   * A given request will attempt to be sent to the first endpoint in the list, and if that fails, the next endpoint in the list will be tried, and so on.<br>
   * 
   * <pre>
   * {@code
   * Example:
   * numEndpoints = 3
   * localityEndpoints = [A, B, C, D, E, F]
   * writeEndpoints = [A, B, C]
   * 
   * Example:
   * getEndpointsToTry() returns [A, B, C, D, E, F]
   * getEndpointsToTry() returns [C, A, B, D, F, E]
   * getEndpointsToTry() returns [B, C, A, E, F, D]
   * getEndpointsToTry() returns [A, B, C, E, D, F]
   * ...
   * }
   * </pre>
   *
   * This ensures that the write endpoints are used in a round-robin manner, and the remaining locality endpoints are used (if >N retries are needed) in a random order.
   * @return the endpoints to try
   */
  protected List<Endpoint> getEndpointsToTry() {
    List<Endpoint> endpointsToTry = new ArrayList<>();

    if (writeEndpoints.size() == numWriteEndpoints) {
      Collections.rotate(writeEndpoints, 1);
      endpointsToTry.addAll(new ArrayList<>(writeEndpoints));
      List<Endpoint> remainingEndpoints = new ArrayList<>(localityEndpoints);
      remainingEndpoints.removeAll(writeEndpoints);
      endpointsToTry.addAll(remainingEndpoints);
    } else {
      // System.out.println("Rotating locality endpoints. Before: " + localityEndpoints);
      Collections.rotate(localityEndpoints, -1);
      endpointsToTry.addAll(localityEndpoints);
      // System.out.println("Rotating locality endpoints. After: " + localityEndpoints);
    }

    return endpointsToTry;
  }

  protected void maybeRegisterWriteEndpoint(Endpoint endpoint, String topic) {
    failureCounts.remove(endpoint);
    if (writeEndpoints.size() < numWriteEndpoints && !writeEndpoints.contains(endpoint)) {
      logger.info("Registering write endpoint: " + endpoint + " for topic: " + topic);
      writeEndpoints.add(endpoint);
    }
    if (!localityEndpoints.contains(endpoint)) {
      logger.info("Registering locality endpoint: " + endpoint + " for topic: " + topic);
      localityEndpoints.add(endpoint);
    }
  }

  /**
   * Refresh the write endpoints to deprioritize the dead endpoint.
   * 
   * @param deadEndpoint
   * @param topic
   * @throws Exception
   */
  protected void deprioritizeDeadEndpoint(Endpoint deadEndpoint, String topic) throws Exception {
      // put deadEndpoint last in the priority
      failureCounts.compute(deadEndpoint, (k, v) -> v == null ? 1 : v + 1);
      if (failureCounts.get(deadEndpoint) >= 2) {
        logger.warn("Dead endpoint " + deadEndpoint + " has failed 2 times, removing from future consideration");
        localityEndpoints.remove(deadEndpoint);
        writeEndpoints.remove(deadEndpoint);
      } else {
        logger.warn("Dead endpoint " + deadEndpoint + " has failed " + failureCounts.get(deadEndpoint) + " times, deprioritizing it");
        localityEndpoints.remove(deadEndpoint);
        writeEndpoints.remove(deadEndpoint);
        localityEndpoints.add(deadEndpoint);
      }
      validateEndpoints();
  }

  protected List<Endpoint> getLocalityEndpoints(List<Endpoint> servers) {
    List<Endpoint> collect = servers.stream().filter(b -> locality.equals(b.getLocality()))
        .collect(Collectors.toList());
    if (collect.isEmpty()) {
      collect = servers;
    }
    Collections.shuffle(collect);
    logger.info("Locality endpoints: " + collect);
    return collect;
  }

  @Override
  public void close() throws IOException {
    if (!networkClient.isClosed()) {
      networkClient.close();
    }
  }

  public static List<Endpoint> generateEndpointsFromBrokers(Set<Broker> brokers) {
    return brokers.stream().map(Endpoint::fromBroker).collect(Collectors.toList());
  }

  public static List<Endpoint> parseServersetFile(String serversetFile) throws IOException {
    Gson gson = new Gson();
    List<String> lines = Files.readAllLines(new File(serversetFile).toPath());
    return lines.stream().map(l -> gson.fromJson(l, JsonObject.class)).filter(g -> g.size() > 0)
        .map(g -> new Endpoint(InetSocketAddress.createUnresolved(g.get("ip").getAsString(), 9092),
            g.get("az").getAsString()))
        .collect(Collectors.toList());
  }

  public static List<Endpoint> getEndpointsFromBootstrapServerString(String bootstrapServers) {
    return Arrays.stream(bootstrapServers.split(",")).map(e -> {
      String[] parts = e.split(":");
      return new Endpoint(InetSocketAddress.createUnresolved(parts[0], Short.parseShort(parts[1])));
    }).collect(Collectors.toList());
  }

  public boolean isClosed() {
    return networkClient.isClosed();
  }
}

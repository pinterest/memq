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
import java.util.concurrent.atomic.AtomicInteger;
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
  private volatile List<Endpoint> localityEndpoints;
  private volatile List<Endpoint> writeEndpoints;
  private Map<Endpoint, Integer> failureCounts;
  private final AtomicInteger writeRotateIdx = new AtomicInteger(0);
  private final AtomicInteger localityRotateIdx = new AtomicInteger(0);

  protected MemqCommonClient(SSLConfig sslConfig, Properties networkProperties) {
    if (networkProperties != null) {
      if (networkProperties.containsKey(NetworkClient.CONFIG_CONNECT_TIMEOUT_MS)) {
        this.connectTimeout = Long
            .parseLong(networkProperties.getProperty(NetworkClient.CONFIG_CONNECT_TIMEOUT_MS));
      }
      if (networkProperties.containsKey(CONFIG_NUM_WRITE_ENDPOINTS)) {
        this.numWriteEndpoints = Math.max(1, Integer.parseInt(networkProperties.getProperty(CONFIG_NUM_WRITE_ENDPOINTS)));
      }
    }
    writeEndpoints = Collections.emptyList();
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
    this.localityEndpoints = Collections.unmodifiableList(getLocalityEndpoints(endpoints));
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
   * The choice of endpoint to try is the first endpoint in the list returned by getEndpointsToTry().
   * The order of the list is based on the logic in getEndpointsToTry().
   * 
   * If the request succeeds and there are less than numWriteEndpoints in the set of write endpoints, the endpoint is added to the set,
   * and the next endpoint is chosen by rotating the locality endpoints.
   * 
   * If the request succeeds and there are already numWriteEndpoints in the set of write endpoints, the endpoint must already be in the set for it to be chosen,
   * so nothing is added to the set. The next endpoint is chosen by rotating the write endpoints, which will give us another endpoint in the set of write endpoints.
   * 
   * If the request fails before reaching the retry limit, the dead endpoint is removed from the set of write endpoints in rotation,
   * and the next working endpoint not already in the rotation is added to the set. The next retry will try the next endpoint in the list.
   * 
   * If an endpoint fails 2 times in a row, it is removed from both the set of write endpoints and the set of locality endpoints so it is not considered for future requests.
   * 
   * If the request fails after reaching the retry limit, the exception is propagated without further refreshing.
   * 
   * For example:
   * <pre>
   * {@code
   * numWriteEndpoints = 3
   * localityEndpoints = [A, B, C, D, E, F]
   * writeEndpoints = []
   * 
   * getEndpointsToTry() returns rotate(localityEndpoints) -->[A, B, C, D, E, F]
   * sendRequestPacketAndReturnResponseFuture() --> try endpoint A --> succeed --> writeEndpoints = [A]
   * getEndpointsToTry() returns rotate(localityEndpoints) --> [B, C, D, E, F, A]
   * sendRequestPacketAndReturnResponseFuture() --> try endpoint B --> succeed --> writeEndpoints = [A, B]
   * getEndpointsToTry() returns rotate(localityEndpoints) --> [C, D, E, F, A, B]
   * sendRequestPacketAndReturnResponseFuture() --> try endpoint C --> succeed --> writeEndpoints = [A, B, C]
   * 
   * ------- writeEndpoints is full -------
   * 
   * getEndpointsToTry() returns rotate(writeEndpoints) U localityEndpoints --> [B, C, A, D, E, F]
   * sendRequestPacketAndReturnResponseFuture() --> try endpoint B --> succeed --> writeEndpoints = [B, C, A]
   * getEndpointsToTry() returns rotate(writeEndpoints) U localityEndpoints --> [C, A, B, D, E, F]
   * sendRequestPacketAndReturnResponseFuture() --> try endpoint C --> succeed --> writeEndpoints = [C, A, B]
   * getEndpointsToTry() returns rotate(writeEndpoints) U localityEndpoints --> [A, B, C, D, E, F]
   * sendRequestPacketAndReturnResponseFuture() --> try endpoint A --> succeed --> writeEndpoints = [A, B, C]
   * 
   * ...
   * 
   * ------- now endpoint A is dead -------
   * 
   * getEndpointsToTry() returns rotate(writeEndpoints) U localityEndpoints --> [A, B, C, D, E, F]
   * sendRequestPacketAndReturnResponseFuture() --> try endpoint A --> fail --> deprioritizeDeadEndpoint(A) --> retry --> try endpoint B --> succeed --> writeEndpoints = [B, C]
   * 
   * ------- writeEndpoints is not full -------
   * 
   * getEndpointsToTry() returns rotate(localityEndpoints) --> [B, C, D, E, F, A]
   * sendRequestPacketAndReturnResponseFuture() --> try endpoint B --> succeed --> B is already in writeEndpoints, so writeEndpoints = [B, C]
   * getEndpointsToTry() returns rotate(localityEndpoints) --> [C, D, E, F, A, B]
   * sendRequestPacketAndReturnResponseFuture() --> try endpoint C --> succeed --> writeEndpoints = [C, B]
   * getEndpointsToTry() returns rotate(localityEndpoints) --> [D, E, F, A, B, C]
   * sendRequestPacketAndReturnResponseFuture() --> try endpoint D --> succeed --> writeEndpoints = [C, B, D]
   * 
   * ------- writeEndpoints is full -------
   * 
   * getEndpointsToTry() returns rotate(writeEndpoints) U localityEndpoints --> [B, C, D, E, F, A]
   * sendRequestPacketAndReturnResponseFuture() --> try endpoint B --> succeed --> writeEndpoints = [B, C, D]
   * getEndpointsToTry() returns rotate(writeEndpoints) U localityEndpoints --> [C, D, B, E, F, A]
   * sendRequestPacketAndReturnResponseFuture() --> try endpoint C --> succeed --> writeEndpoints = [C, D, B]
   * getEndpointsToTry() returns rotate(writeEndpoints) U localityEndpoints --> [D, B, C, E, F, A]
   * sendRequestPacketAndReturnResponseFuture() --> try endpoint D --> succeed --> writeEndpoints = [D, B, C]
   * 
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
              logger.error("Failed to refresh write endpoints", ex);
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
    writeEndpoints = Collections.emptyList();
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
    localityEndpoints = Collections.unmodifiableList(
        getLocalityEndpoints(brokers.stream().map(Endpoint::fromBroker).collect(Collectors.toList())));
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
   * 2. Remaining locality endpoints, the order of which was already shuffled during initialization<br>
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
   * getEndpointsToTry() returns [C, A, B, D, E, F]
   * getEndpointsToTry() returns [B, C, A, D, E, F]
   * getEndpointsToTry() returns [A, B, C, D, E, F]
   * ...
   * }
   * </pre>
   *
   * This ensures that the write endpoints are used in a round-robin manner.
   * 
   * If there are less than numWriteEndpoints in the set of writeEndpoints, the localityEndpoints are rotated by 1 and returned. The client will add working endpoints 
   * to the set of writeEndpoints until the size of writeEndpoints reaches numWriteEndpoints. An example is provided in javadoc for sendRequestPacketAndReturnResponseFuture().
   * 
   * @return the endpoints to try
   */
  protected List<Endpoint> getEndpointsToTry() {
    // Snapshot current lists to avoid races and in-place mutation
    List<Endpoint> writes = this.writeEndpoints;
    List<Endpoint> locals = this.localityEndpoints;
    List<Endpoint> endpointsToTry = new ArrayList<>(writes.size() + locals.size());

    if (writes.size() == numWriteEndpoints) {
      // If the set of writeEndpoints is full, rotate the writeEndpoints by 1 and add the localityEndpoints that are not in the set of writeEndpoints to the end of the list
      int start = Math.floorMod(writeRotateIdx.getAndIncrement(), Math.max(1, writes.size()));
      for (int i = 0; i < writes.size(); i++) {
        endpointsToTry.add(writes.get((start + i) % writes.size()));
      }
      for (Endpoint e : locals) {
        if (!writes.contains(e)) {
          endpointsToTry.add(e);
        }
      }
    } else {
      // If the set of writeEndpoints is not full, rotate the localityEndpoints by 1
      int start = Math.floorMod(localityRotateIdx.getAndIncrement(), Math.max(1, locals.size()));
      for (int i = 0; i < locals.size(); i++) {
        endpointsToTry.add(locals.get((start + i) % locals.size()));
      }
    }

    return endpointsToTry;
  }

  /**
   * The provided endpoint had just succeeded, so register the endpoint as a write endpoint if it is not already in the set of write endpoints and if the set of write endpoints is not full.
   * 
   * The endpoint's failure count is reset to 0 since it had just succeeded.
   * 
   * If the set of write endpoints is full, nothing is done.
   * 
   * @param endpoint
   * @param topic
   */
  protected void maybeRegisterWriteEndpoint(Endpoint endpoint, String topic) {
    failureCounts.remove(endpoint);
    List<Endpoint> currentWrites = this.writeEndpoints;
    if (currentWrites.size() < numWriteEndpoints && !currentWrites.contains(endpoint)) {
      logger.info("Registering write endpoint: " + endpoint + " for topic: " + topic);
      List<Endpoint> newWrites = new ArrayList<>(currentWrites);
      newWrites.add(endpoint);
      this.writeEndpoints = Collections.unmodifiableList(newWrites);
    }
    List<Endpoint> currentLocals = this.localityEndpoints;
    if (!currentLocals.contains(endpoint)) {
      logger.info("Registering locality endpoint: " + endpoint + " for topic: " + topic);
      List<Endpoint> newLocals = new ArrayList<>(currentLocals);
      newLocals.add(endpoint);
      this.localityEndpoints = Collections.unmodifiableList(newLocals);
    }
  }

  /**
   * Deprioritize the dead endpoint by removing it from the set of write endpoints and moving it to the end of locality endpoints.
   * 
   * If the endpoint has failed 2 times in a row, it is removed from both the set of write endpoints and the set of locality endpoints so it is not considered for future requests.
   * 
   * If the endpoint has failed 1 time but succeeds again in a future request, its failure count is reset to 0 in maybeRegisterWriteEndpoint().
   * 
   * An example is provided in javadoc for sendRequestPacketAndReturnResponseFuture().
   * 
   * @param deadEndpoint
   * @param topic
   * @throws Exception
   */
  protected void deprioritizeDeadEndpoint(Endpoint deadEndpoint, String topic) throws Exception {
      failureCounts.compute(deadEndpoint, (k, v) -> v == null ? 1 : v + 1);
      int failures = failureCounts.get(deadEndpoint);

      List<Endpoint> newLocals = new ArrayList<>(this.localityEndpoints);
      newLocals.remove(deadEndpoint);
      List<Endpoint> newWrites = new ArrayList<>(this.writeEndpoints);
      newWrites.remove(deadEndpoint);

      if (failures >= 2) {
        logger.warn("Dead endpoint " + deadEndpoint + " has failed 2 times, removing from future consideration");
        // Do not re-add to locals
      } else {
        logger.warn("Dead endpoint " + deadEndpoint + " has failed " + failures + " times, deprioritizing it");
        newLocals.add(deadEndpoint); // move to end
      }

      this.localityEndpoints = Collections.unmodifiableList(newLocals);
      this.writeEndpoints = Collections.unmodifiableList(newWrites);
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

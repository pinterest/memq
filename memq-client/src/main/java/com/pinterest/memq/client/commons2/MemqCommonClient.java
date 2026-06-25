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
import java.util.Collection;
import java.io.File;
import java.io.IOException;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
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
import com.pinterest.memq.commons.protocol.WriteResponsePacket;

public class MemqCommonClient implements Closeable {

  private static final Logger logger = LoggerFactory.getLogger(MemqCommonClient.class);
  private static final int MAX_SEND_RETRIES = 3;

  public static final String CONFIG_NUM_WRITE_ENDPOINTS = "numWriteEndpoints"; // number of endpoints for writes

  /**
   * Producer-side post-eviction routing boost: for this many milliseconds
   * after an eviction directive named broker {@code T} as the target, the
   * weighted endpoint map gives {@code T} an extra {@code +1} slot of
   * routing weight on top of its acknowledged {@code slotsOwned} count.
   * <p>
   * The purpose is to push {@code T}'s broker-side EMA decisively past the
   * next {@code ceil(EMA / slotSize)} boundary so its {@code SlotManager}
   * actually <i>acquires</i> the slot the eviction transferred. Without
   * the boost, the broker's EMA hovers exactly at the boundary and the
   * acquisition is metastable -- the broker may never cross threshold and
   * the producer's optimistic increment is then overwritten by the
   * broker's stale {@code numSlotsOwned} on the next response, producing
   * the eviction "flap" we see in production.
   * <p>
   * Symmetric in design with the broker-side post-eviction acquisition
   * cooldown ({@code SlotAccountingConfig.postEvictionCooldownSeconds}):
   * the broker holds the released slot empty for ~60s while the producer
   * over-routes to the eviction target for ~60s. Within that window
   * traffic genuinely shifts and the target broker acquires.
   * <p>
   * Set to {@code 0} to disable.
   */
  public static final String CONFIG_POST_EVICTION_ROUTING_BOOST_MS =
      "postEvictionRoutingBoostMs";
  public static final long DEFAULT_POST_EVICTION_ROUTING_BOOST_MS = 60_000L;

  private final NetworkClient networkClient;
  private long connectTimeout = 500;
  private int numWriteEndpoints = 1;

  private String locality = DEFAULT_LOCALITY;
  private volatile List<Endpoint> localityEndpoints;
  private volatile List<Endpoint> writeEndpoints;
  private Map<Endpoint, Integer> failureCounts;
  private final AtomicInteger writeRotateIdx = new AtomicInteger(0);
  private final AtomicInteger localityRotateIdx = new AtomicInteger(0);
  private volatile TreeMap<Double, Endpoint> weightedEndpointMap;
  private final String producerId = java.util.UUID.randomUUID().toString();
  private final ConcurrentHashMap<String, Integer> slotsOwned = new ConcurrentHashMap<>();
  /**
   * Per-broker wall-clock millis until which {@link #rebuildWeightedMap}
   * adds a post-eviction boost to the broker's routing weight. Armed when the
   * broker is the target of an eviction directive; cleared when the broker
   * becomes the source of one (it just gave a slot away -- don't keep
   * over-routing to it). Lazily evaluated against {@code System
   * .currentTimeMillis()} -- entries past their TTL behave like zero.
   * See {@link #CONFIG_POST_EVICTION_ROUTING_BOOST_MS}.
   */
  private final ConcurrentHashMap<String, Long> routingBoostUntilMs =
      new ConcurrentHashMap<>();
  /**
   * Per-broker boost <i>magnitude</i> (in slots), paired with
   * {@link #routingBoostUntilMs}. Set to the directive's {@code numSlotsToEvict}
   * when the boost is armed so the routing-weight bump is proportional to the
   * load actually moved -- a single-slot eviction nudges by {@code +1}, an
   * N-slot eviction by {@code +N}. This matches the optimistic credit added to
   * {@link #slotsOwned} so the target stays elevated even after the broker's
   * stale {@code numSlotsOwned} overwrites that credit on its next non-eviction
   * response. The TTL ({@link #postEvictionRoutingBoostMs}) is unchanged.
   */
  private final ConcurrentHashMap<String, Integer> routingBoostSlots =
      new ConcurrentHashMap<>();
  private long postEvictionRoutingBoostMs = DEFAULT_POST_EVICTION_ROUTING_BOOST_MS;
  // Sticky: flips to true the first time we observe a v4-only signal from any
  // broker (eviction directive or non-zero slot ownership). Once true, the
  // legacy numWriteEndpoints gates become no-ops and weighted selection
  // governs routing. Stays false forever when talking to v3 brokers, which
  // keeps the legacy round-robin behavior fully intact for them.
  private volatile boolean v4Active = false;

  // State-change snapshot logger: only logs when the weight distribution
  // signature actually changes. No time-based throttling — quiet on steady
  // state, immediately visible on any change.
  private volatile String lastLoggedWeightSignature = "";

  // Diagnostics: counts every WriteResponsePacket processed. Used to log the
  // first few responses verbatim (so it's obvious whether the broker is
  // sending v4 fields), and to fire a one-shot warning if responses are
  // flowing but v4Active never flipped (legacy v3 routing stuck on).
  private final AtomicLong writeResponseCount = new AtomicLong();
  private static final int LOG_FIRST_N_RESPONSES = 3;
  private volatile boolean stuckOnV3Warned = false;

  protected MemqCommonClient(SSLConfig sslConfig, Properties networkProperties) {
    if (networkProperties != null) {
      if (networkProperties.containsKey(NetworkClient.CONFIG_CONNECT_TIMEOUT_MS)) {
        this.connectTimeout = Long
            .parseLong(networkProperties.getProperty(NetworkClient.CONFIG_CONNECT_TIMEOUT_MS));
      }
      if (networkProperties.containsKey(CONFIG_NUM_WRITE_ENDPOINTS)) {
        this.numWriteEndpoints = Math.max(1, Integer.parseInt(networkProperties.getProperty(CONFIG_NUM_WRITE_ENDPOINTS)));
      }
      if (networkProperties.containsKey(CONFIG_POST_EVICTION_ROUTING_BOOST_MS)) {
        this.postEvictionRoutingBoostMs = Math.max(0L, Long.parseLong(
            networkProperties.getProperty(CONFIG_POST_EVICTION_ROUTING_BOOST_MS)));
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
    logger.info("MemqCommonClient bootstrap: producerId=" + producerId
        + " locality=" + locality
        + " numWriteEndpoints=" + numWriteEndpoints
        + " localityEndpoints=" + localityEndpoints.size()
        + " (v4Active=" + v4Active + " until first v4 broker response)");
  }

  public void resetEndpoints(List<Endpoint> endpoints) throws Exception {
    this.localityEndpoints = Collections.unmodifiableList(getLocalityEndpoints(endpoints));
    validateEndpoints();
  }

  /**
   * Producer/write-path endpoint reset that refuses to route writes cross-AZ.
   * <p>
   * Unlike {@link #resetEndpoints(List)} (used for the bootstrap serverset and
   * the cross-AZ-tolerant read path), this filters {@code writeEndpoints} to
   * the AZ-local subset with <b>no</b> cross-AZ fallback. If the topic's write
   * brokers currently contain no AZ-local broker -- which happens transiently
   * during broker deploys or governor reassignments -- we keep the previously
   * resolved AZ-local endpoint set instead of adopting cross-AZ brokers. Writes
   * then continue to AZ-local brokers (and backlog/backpressure via the
   * producer's inflight buffer) until an in-AZ write broker reappears on the
   * next metadata refresh. This preserves the slot/eviction accounting, which
   * assumes AZ-local routing.
   */
  public synchronized void resetWriteEndpoints(List<Endpoint> writeEndpoints) throws Exception {
    applyWriteLocalityEndpoints(writeEndpoints);
  }

  /**
   * Apply AZ-local write endpoints with keep-prior semantics. Must be called
   * while holding this client's monitor (callers: {@link #resetWriteEndpoints}
   * and {@link #reconnect}). See {@link #resetWriteEndpoints(List)}.
   */
  private void applyWriteLocalityEndpoints(List<Endpoint> writeEndpoints) throws Exception {
    List<Endpoint> inAz = getLocalityEndpoints(writeEndpoints, false);
    if (inAz.isEmpty()) {
      List<Endpoint> prior = this.localityEndpoints;
      if (prior != null && !prior.isEmpty()) {
        logger.warn("No AZ-local write broker available for locality=" + locality
            + "; keeping prior " + prior.size() + " AZ-local endpoint(s) and backlogging"
            + " until an in-AZ write broker reappears (no cross-AZ fallback).");
        return;
      }
      throw new Exception("No AZ-local endpoints available for locality=" + locality
          + " and no prior AZ-local endpoint set to fall back on; refusing to route writes"
          + " cross-AZ.");
    }
    this.localityEndpoints = Collections.unmodifiableList(inAz);
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
    rebuildWeightedMap();
    return resp.getMetadata();
  }

  public TopicMetadata getTopicMetadata(String topic) throws TopicNotFoundException,
                                                      ExecutionException, InterruptedException,
                                                      TimeoutException {
    return getTopicMetadata(topic, connectTimeout);
  }

  /**
   * Get all topics from the MemQ cluster.
   * @return a collection of TopicMetadata objects
   * @throws ExecutionException
   * @throws InterruptedException
   * @throws TimeoutException
   */
  public Collection<TopicMetadata> getTopics() throws ExecutionException, InterruptedException,
                                               TimeoutException {
    Future<ResponsePacket> response = sendRequestPacketAndReturnResponseFuture(
        new RequestPacket(RequestType.PROTOCOL_VERSION, ThreadLocalRandom.current().nextLong(),
            RequestType.TOPIC_METADATA, new TopicMetadataRequestPacket(Collections.emptyList())),
        "", connectTimeout);
    ResponsePacket responsePacket = response.get(connectTimeout, TimeUnit.MILLISECONDS);
    TopicMetadataResponsePacket resp = (TopicMetadataResponsePacket) responsePacket.getPacket();
    writeEndpoints = Collections.emptyList();
    return resp.getMetadataList();
  }

  public synchronized void reconnect(String topic, boolean isConsumer) throws Exception {
    logger.warn("Reconnecting topic " + topic);

    TopicMetadata md = getTopicMetadata(topic, connectTimeout);
    networkClient.reset();
    Set<Broker> brokers = isConsumer ? md.getReadBrokers() : md.getWriteBrokers();
    List<Endpoint> endpoints = brokers.stream().map(Endpoint::fromBroker)
        .collect(Collectors.toList());
    if (isConsumer) {
      // Read path: no slot/eviction accounting, so cross-AZ fallback is allowed.
      localityEndpoints = Collections.unmodifiableList(getLocalityEndpoints(endpoints, true));
      validateEndpoints();
    } else {
      // Write path: never adopt cross-AZ brokers; keep the prior AZ-local set
      // and backlog until an in-AZ write broker reappears.
      applyWriteLocalityEndpoints(endpoints);
    }
  }

  /**
   * Surgically drop a single broker from every per-broker structure on this
   * client and renormalize routing over the survivors. Intended for the
   * {@code REDIRECT} path: a broker that no longer serves the producer's
   * topic is identified by the source address of its response packet, and
   * the client snips it out without the metadata refetch /
   * {@link com.pinterest.memq.client.commons2.network.NetworkClient#reset()}
   * heaviness of {@link #reconnect(String, boolean)}.
   * <p>
   * State affected (all keyed by IP / host string of {@code addr}):
   * <ul>
   *   <li>{@link #slotsOwned} — drops the entry; survivor weights are
   *       preserved untouched.</li>
   *   <li>{@link #routingBoostUntilMs} — drops the post-eviction boost;
   *       irrelevant once the broker is gone.</li>
   *   <li>{@link #failureCounts} — drops any deprioritization record.</li>
   *   <li>{@link #writeEndpoints} — filtered.</li>
   *   <li>{@link #localityEndpoints} — filtered.</li>
   *   <li>{@link com.pinterest.memq.client.commons2.network.NetworkClient}
   *       channel pool — {@code closeChannel(addr)} releases the connection
   *       asynchronously.</li>
   *   <li>{@link #weightedEndpointMap} — rebuilt from the post-removal
   *       {@code writeEndpoints} and {@code slotsOwned}.</li>
   * </ul>
   * <p>
   * Throws <i>without mutating any state</i> if the removal would leave
   * {@link #localityEndpoints} empty. This pre-check is essential: the
   * caller's fallback path is to invoke {@link #reconnect(String, boolean)},
   * which itself needs at least one live endpoint in {@code localityEndpoints}
   * to send the metadata request. If we mutated first then threw, the
   * fallback would fail with "Client not initialized yet" because
   * {@link #sendRequestPacketAndReturnResponseFuture} short-circuits on an
   * empty {@code localityEndpoints}. By keeping state intact on the throw,
   * the fallback reconnect can still reach the dying broker for metadata
   * (which may yield a fresh broker set the dying broker still knows about).
   * <p>
   * {@code synchronized} matches {@link #reconnect(String, boolean)} so
   * mutations to the endpoint lists and the weighted map are atomic with
   * respect to other top-level routing-state mutations.
   */
  public synchronized void removeBroker(InetSocketAddress addr) throws Exception {
    if (addr == null) {
      return;
    }
    String ip = addr.getHostString();

    // Pre-check: refuse to empty localityEndpoints. State stays untouched so
    // the caller can fall back to reconnect() which still has the dying
    // broker available for the metadata round-trip.
    long localitySurvivors = localityEndpoints.stream()
        .filter(e -> !e.getAddress().getHostString().equals(ip))
        .count();
    if (localitySurvivors == 0) {
      throw new Exception("No endpoints available after removing " + ip);
    }

    slotsOwned.remove(ip);
    routingBoostUntilMs.remove(ip);
    routingBoostSlots.remove(ip);
    failureCounts.keySet().removeIf(
        e -> e.getAddress().getHostString().equals(ip));

    List<Endpoint> currentWrites = this.writeEndpoints;
    List<Endpoint> newWrites = new ArrayList<>(currentWrites.size());
    for (Endpoint e : currentWrites) {
      if (!e.getAddress().getHostString().equals(ip)) {
        newWrites.add(e);
      }
    }
    this.writeEndpoints = Collections.unmodifiableList(newWrites);

    List<Endpoint> currentLocals = this.localityEndpoints;
    List<Endpoint> newLocals = new ArrayList<>(currentLocals.size());
    for (Endpoint e : currentLocals) {
      if (!e.getAddress().getHostString().equals(ip)) {
        newLocals.add(e);
      }
    }
    this.localityEndpoints = Collections.unmodifiableList(newLocals);

    networkClient.closeChannel(addr);
    rebuildWeightedMap();
    logger.info("Surgically removed broker " + ip
        + " from routing state (writeEndpoints=" + writeEndpoints.size()
        + ", localityEndpoints=" + localityEndpoints.size()
        + ", slotsOwned=" + slotsOwned.size() + ")");
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
    // Weighted path (v4 with slot data) — entirely separate from legacy
    TreeMap<Double, Endpoint> wm = this.weightedEndpointMap;
    if (wm != null && !wm.isEmpty()) {
      return selectWeightedEndpoint(wm);
    }

    // Legacy round-robin path (v3 / no slot data)
    List<Endpoint> writes = this.writeEndpoints;
    List<Endpoint> locals = this.localityEndpoints;
    List<Endpoint> endpointsToTry = new ArrayList<>(writes.size() + locals.size());

    if (writes.size() == numWriteEndpoints) {
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
      int start = Math.floorMod(localityRotateIdx.getAndIncrement(), Math.max(1, locals.size()));
      for (int i = 0; i < locals.size(); i++) {
        endpointsToTry.add(locals.get((start + i) % locals.size()));
      }
    }

    return endpointsToTry;
  }

  /**
   * Probabilistic endpoint selection using normalized cumulative weights (0.0 to 1.0).
   * O(log n) lookup via TreeMap.higherEntry, then appends remaining endpoints as fallbacks.
   */
  private List<Endpoint> selectWeightedEndpoint(TreeMap<Double, Endpoint> wm) {
    List<Endpoint> locals = this.localityEndpoints;
    List<Endpoint> writes = this.writeEndpoints;

    double rand = ThreadLocalRandom.current().nextDouble();
    Endpoint chosen = wm.higherEntry(rand).getValue();

    List<Endpoint> result = new ArrayList<>(writes.size() + locals.size());
    result.add(chosen);
    for (Endpoint ep : wm.values()) {
      if (!ep.equals(chosen)) {
        result.add(ep);
      }
    }
    for (Endpoint ep : locals) {
      if (!writes.contains(ep)) {
        result.add(ep);
      }
    }
    return result;
  }

  /**
   * Rebuild the normalized weight map from slotsOwned + writeEndpoints.
   * Keys are cumulative weights normalized to [0.0, 1.0], so the last key is always 1.0.
   * Called whenever either data source changes. The map is set to null when
   * weighted selection is not applicable (no slot data or no write endpoints).
   * <p>
   * Note: this is not gated on {@code numWriteEndpoints}. As soon as we have
   * any slot data (i.e. v4 broker), weighted selection takes over so the
   * legacy fan-out width knob no longer matters. v3 brokers never populate
   * {@code slotsOwned}, so the legacy round-robin path keeps running for them.
   */
  void rebuildWeightedMap() {
    List<Endpoint> writes = this.writeEndpoints;
    if (slotsOwned.isEmpty() || writes.isEmpty()) {
      this.weightedEndpointMap = null;
      return;
    }
    long now = System.currentTimeMillis();
    int totalSlots = 0;
    for (Endpoint ep : writes) {
      totalSlots += effectiveWeight(ep.getAddress().getHostString(), now);
    }
    TreeMap<Double, Endpoint> map = new TreeMap<>();
    double cumulative = 0;
    for (Endpoint ep : writes) {
      cumulative += (double) effectiveWeight(ep.getAddress().getHostString(), now) / totalSlots;
      map.put(cumulative, ep);
    }
    this.weightedEndpointMap = map;
    maybeLogWeightSnapshot(writes, totalSlots, now);
  }

  /**
   * Per-broker routing weight: the broker's acknowledged slot count (with the
   * legacy floor of 1), plus the post-eviction boost while the broker is an
   * eviction target. The boost is proportional to the slots moved by the
   * directive ({@code +numSlotsToEvict}), not a flat {@code +1}.
   * <p>
   * Visible for test.
   */
  int effectiveWeight(String ip, long now) {
    int base = Math.max(slotsOwned.getOrDefault(ip, 1), 1);
    return base + routingBoost(ip, now);
  }

  /**
   * Active post-eviction boost magnitude (in slots) for {@code ip}, or 0 if no
   * boost is armed or it has expired. See {@link #routingBoostSlots}.
   */
  private int routingBoost(String ip, long now) {
    Long until = routingBoostUntilMs.get(ip);
    if (until == null || until <= now) {
      return 0;
    }
    return Math.max(0, routingBoostSlots.getOrDefault(ip, 1));
  }

  /**
   * Log the current write-endpoint weight distribution, but only when the
   * signature has changed since the last log. Cheap to call on every response;
   * stays silent on steady state.
   */
  private void maybeLogWeightSnapshot(List<Endpoint> writes, int totalSlots, long now) {
    StringBuilder sig = new StringBuilder();
    StringBuilder pretty = new StringBuilder();
    for (Endpoint ep : writes) {
      String ip = ep.getAddress().getHostString();
      int slots = Math.max(slotsOwned.getOrDefault(ip, 1), 1);
      int boost = routingBoost(ip, now);
      int weight = effectiveWeight(ip, now);
      int pct = (int) Math.round(100.0 * weight / totalSlots);
      sig.append(ip).append('=').append(weight).append(',');
      if (pretty.length() > 0) pretty.append(", ");
      pretty.append(ip).append('(').append(slots).append(" slots");
      if (boost > 0) pretty.append("+").append(boost).append(" boost");
      pretty.append(", ").append(pct).append("%)");
    }
    String signature = sig.toString();
    if (signature.equals(lastLoggedWeightSignature)) {
      return;
    }
    lastLoggedWeightSignature = signature;
    logger.info("Broker weights changed (writeEndpoints=" + writes.size()
        + ", totalSlots=" + totalSlots
        + ", trackedConnections=" + slotsOwned.size()
        + "): " + pretty);
  }

  /**
   * The provided endpoint had just succeeded, so register the endpoint as a
   * write endpoint if it is not already in the set of write endpoints and the
   * set of write endpoints is not full.
   * <p>
   * The endpoint's failure count is reset to 0 since it had just succeeded.
   * <p>
   * <b>v3 (legacy) brokers</b>: the writeEndpoints set is grown until it
   * reaches {@code numWriteEndpoints} (round-robin fan-out width).
   * <p>
   * <b>v4 brokers</b>: once {@code v4Active} flips, {@link #writeEndpoints} is a
   * pure projection of {@link #slotsOwned} (the source of truth), rebuilt by
   * {@link #reconcileWriteEndpoints()}. Per-producer connection limits are
   * enforced authoritatively by the broker's eviction strategy
   * ({@code EvictionConfig.maxConnectionsPerProducer}), so the client does not
   * apply its own cap. This method then only ensures locality discovery and
   * clears failure counts for the succeeding endpoint.
   *
   * @param endpoint
   * @param topic
   */
  protected void maybeRegisterWriteEndpoint(Endpoint endpoint, String topic) {
    failureCounts.remove(endpoint);
    List<Endpoint> currentWrites = this.writeEndpoints;
    if (!v4Active && currentWrites.size() < numWriteEndpoints
        && !currentWrites.contains(endpoint)) {
      logger.info("Registering write endpoint: " + endpoint + " for topic: " + topic);
      List<Endpoint> newWrites = new ArrayList<>(currentWrites);
      newWrites.add(endpoint);
      this.writeEndpoints = Collections.unmodifiableList(newWrites);
      rebuildWeightedMap();
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

      if (failures >= 2) {
        logger.warn("Dead endpoint " + deadEndpoint + " has failed 2 times, removing from future consideration");
        // Do not re-add to locals
      } else {
        logger.warn("Dead endpoint " + deadEndpoint + " has failed " + failures + " times, deprioritizing it");
        newLocals.add(deadEndpoint); // move to end
      }
      this.localityEndpoints = Collections.unmodifiableList(newLocals);

      if (v4Active) {
        // slotsOwned is the source of truth: drop the dead broker from it (and
        // clear its routing boost), then re-derive writeEndpoints. Removing it
        // from writeEndpoints alone would be undone by the next reconcile, which
        // rebuilds writeEndpoints from slotsOwned.
        String ip = deadEndpoint.getAddress().getHostString();
        synchronized (this) {
          slotsOwned.remove(ip);
          routingBoostUntilMs.remove(ip);
          routingBoostSlots.remove(ip);
          reconcileWriteEndpoints();
        }
      } else {
        List<Endpoint> newWrites = new ArrayList<>(this.writeEndpoints);
        newWrites.remove(deadEndpoint);
        this.writeEndpoints = Collections.unmodifiableList(newWrites);
        rebuildWeightedMap();
      }
      validateEndpoints();
  }

  protected List<Endpoint> getLocalityEndpoints(List<Endpoint> servers) {
    return getLocalityEndpoints(servers, true);
  }

  /**
   * Resolve the AZ-local subset of {@code servers}.
   * <p>
   * When no server matches this client's {@link #locality} the behaviour
   * depends on {@code allowCrossAzFallback}:
   * <ul>
   *   <li>{@code true} (read / metadata-discovery path): fall back to the full
   *       {@code servers} list so the client can still function cross-AZ.</li>
   *   <li>{@code false} (producer write path): return an empty list. Spilling
   *       writes cross-AZ silently breaks the slot/eviction accounting, which
   *       assumes AZ-local routing -- so callers must instead treat an empty
   *       result as a transient condition (keep the prior AZ-local set and let
   *       requests backlog until in-AZ write brokers reappear). See
   *       {@link #applyWriteLocalityEndpoints(List)}.</li>
   * </ul>
   */
  protected List<Endpoint> getLocalityEndpoints(List<Endpoint> servers, boolean allowCrossAzFallback) {
    List<Endpoint> collect = servers.stream().filter(b -> locality.equals(b.getLocality()))
        .collect(Collectors.toList());
    if (collect.isEmpty()) {
      if (!allowCrossAzFallback) {
        logger.warn("No AZ-local endpoints available for locality=" + locality + " among "
            + servers.size() + " broker(s); treating as transient (will retry) and NOT"
            + " falling back to cross-AZ.");
        return Collections.emptyList();
      }
      logger.warn("No AZ-local endpoints for locality=" + locality + " among " + servers.size()
          + " broker(s); falling back to cross-AZ.");
      collect = new ArrayList<>(servers);
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

  /**
   * Process a v4 WriteResponsePacket. There are two paths:
   * <p>
   * 1. Eviction path ({@code writeResp.hasEviction()}): the broker is shedding the
   *    producer to {@code targetBrokerIp}. We:
   *    <ul>
   *      <li>apply the eviction normally (bump target, update source remaining),</li>
   *      <li>arm a post-eviction routing boost on the target so the producer
   *          over-routes there long enough for the broker-side EMA to acquire
   *          the transferred slot,</li>
   *      <li>register the target as an active write endpoint <b>immediately</b> so
   *          the next dispatch can route to it (no waiting for an existing endpoint
   *          to die or a reconnect to surface the new broker), and</li>
   *      <li>rebuild the weighted endpoint map with the updated slot data.</li>
   *    </ul>
   * <p>
   * 2. Non-eviction path: the broker reports the producer's current slot
   *    ownership (numSlotsOwned). Once {@code v4Active}, the responding broker
   *    is one we wrote to, so we track it in {@code slotsOwned} (seeding at a
   *    floor of 1 when it reports 0) and
   *    {@link #reconcileWriteEndpoints() reconcile}. Before {@code v4Active}
   *    flips, only already-known brokers are refreshed so the weighted set is
   *    not derived from an empty slot map.
   * <p>
   * The per-producer connection cap is enforced authoritatively by the broker's
   * eviction strategy ({@code EvictionConfig.maxConnectionsPerProducer}): the
   * broker refuses to evict to a target that would push the producer above the
   * cap in routine mode, so the client never needs to apply its own cap and
   * the connection set converges to a steady size via the broker's decisions.
   */
  public synchronized void handleWriteResponse(WriteResponsePacket writeResp,
                                               InetSocketAddress sourceAddress) {
    if (writeResp == null) {
      return;
    }

    String sourceIp = sourceAddress != null ? sourceAddress.getHostString() : null;
    int remaining = writeResp.getNumSlotsOwned();
    short serverVersion = writeResp.getServerProtocolVersion();

    // Diagnostics: log the first few responses verbatim so it's immediately
    // visible whether the broker is actually stamping v4 capability or
    // returning a pre-feature packet. This is the canonical answer to
    // "why isn't v4 active?"
    long n = writeResponseCount.incrementAndGet();
    if (n <= LOG_FIRST_N_RESPONSES) {
      logger.info("WriteResponse #" + n + " received: source=" + sourceAddress
          + " serverProtocolVersion=" + serverVersion
          + " hasEviction=" + writeResp.hasEviction()
          + " targetBrokerIp=" + writeResp.getTargetBrokerIp()
          + " numSlotsToEvict=" + writeResp.getNumSlotsToEvict()
          + " numSlotsOwned=" + remaining
          + " producerId=" + producerId
          + " v4Active(before)=" + v4Active);
    }

    // EXPLICIT v4 detection: the broker self-declares its protocol version
    // on every WriteResponsePacket it produces. We never infer v4 from slot
    // counts (those are 0 until the producer accumulates enough traffic).
    // Once flipped, v4Active is sticky for the lifetime of the client.
    if (serverVersion >= 4) {
      if (!v4Active) {
        logger.info("v4 broker protocol detected; weighted endpoint selection now active."
            + " firstSignalSource=" + sourceAddress
            + " serverProtocolVersion=" + serverVersion
            + " firstSlotsOwned=" + remaining
            + " firstHasEviction=" + writeResp.hasEviction()
            + " currentWriteEndpoints=" + writeEndpoints);
      }
      v4Active = true;
    } else if (!v4Active && !stuckOnV3Warned && n >= LOG_FIRST_N_RESPONSES) {
      // We've seen N responses with no v4 capability declaration. Either
      // we're talking to a v3 broker fleet, or a v4 broker is on a code
      // path that returns an unstamped WriteResponsePacket. One-shot
      // warning so it can be triaged.
      stuckOnV3Warned = true;
      logger.warn("Producer is on legacy v3 routing path after " + n
          + " write responses (broker did not declare serverProtocolVersion>=4)."
          + " If you expect v4 weighted routing, verify: (a) brokers are"
          + " running the v4 build that stamps responses via WriteResponseBuilder,"
          + " (b) no proxy is stripping the trailing v4 fields, (c) producer is"
          + " sending v4 requests (this client's producerId=" + producerId + ")."
          + " Last response: source=" + sourceAddress
          + " serverProtocolVersion=" + serverVersion
          + " hasEviction=" + writeResp.hasEviction()
          + " numSlotsOwned=" + remaining);
    }

    if (writeResp.hasEviction()) {
      String targetIp = writeResp.getTargetBrokerIp();
      int slotsToEvict = writeResp.getNumSlotsToEvict();

      // Apply the eviction to the source-of-truth slot map. Only credit the
      // target when slots actually moved: a zero-slot directive (e.g. from an
      // older broker that decided the eviction before the producer decayed to
      // zero) must not insert a phantom endpoint into slotsOwned -- that would
      // inflate the connection set with a broker we never own slots on or
      // connect to. Newer brokers suppress zero-slot directives at the source.
      if (slotsToEvict > 0) {
        slotsOwned.merge(targetIp, slotsToEvict, Integer::sum);
      }
      if (sourceIp != null) {
        if (remaining > 0) {
          slotsOwned.put(sourceIp, remaining);
        } else {
          slotsOwned.remove(sourceIp);
        }
      }

      // Post-eviction routing boost.
      // Arm the +1 routing-weight boost on the target so the producer
      // over-routes there long enough for the target's broker-side EMA
      // to decisively cross the next ceil(EMA/slotSize) boundary and
      // acquire the slot for real (instead of the producer's optimistic
      // increment getting silently overwritten by the target broker's
      // stale numSlotsOwned on the next non-eviction response).
      // Symmetric with the broker-side post-eviction acquisition cooldown.
      // Clear any boost on the source -- it just gave a slot away, so we
      // must not keep over-routing to it.
      if (postEvictionRoutingBoostMs > 0 && targetIp != null && slotsToEvict > 0) {
        routingBoostUntilMs.put(targetIp,
            System.currentTimeMillis() + postEvictionRoutingBoostMs);
        // Boost magnitude is proportional to the slots moved so an N-slot
        // eviction nudges routing by +N (matching the optimistic slotsOwned
        // credit), not a flat +1. The TTL is unchanged.
        routingBoostSlots.put(targetIp, slotsToEvict);
      }
      if (sourceIp != null) {
        routingBoostUntilMs.remove(sourceIp);
        routingBoostSlots.remove(sourceIp);
      }

      // Single reconciliation: derive writeEndpoints from slotsOwned (the source
      // of truth) and rebuild the weighted map. The dropped/drained source falls
      // out of writeEndpoints automatically; the target is added immediately.
      reconcileWriteEndpoints();

      logger.info("Eviction received: source=" + sourceAddress + " target=" + targetIp
          + " slotsToEvict=" + slotsToEvict + " remaining=" + remaining
          + " slotsOwned=" + slotsOwned);
      return;
    }

    // Non-eviction response.
    if (sourceIp == null) {
      return;
    }

    if (!v4Active) {
      // Legacy / pre-activation window: routing is governed by round-robin over
      // writeEndpoints (managed by maybeRegisterWriteEndpoint). Only refresh
      // slot data for brokers we already know so we never grow the weighted set
      // -- and never derive writeEndpoints from the (still empty) slot map --
      // before a v4 broker has been observed.
      if ((slotsOwned.containsKey(sourceIp) || writeEndpointsContains(sourceIp))
          && remaining > 0) {
        slotsOwned.put(sourceIp, remaining);
        rebuildWeightedMap();
      }
      return;
    }

    // v4 non-eviction response: slotsOwned is the source of truth. The
    // responding broker is one we wrote to, so track it (seeding at a floor of
    // 1 when it reports 0) and reconcile so it participates in weighted
    // routing. The broker's eviction strategy is authoritative on connection
    // cap enforcement, so the client adds any broker it actually wrote to.
    boolean isKnown = slotsOwned.containsKey(sourceIp);
    boolean added = false;
    if (remaining > 0) {
      added = slotsOwned.put(sourceIp, remaining) == null;
    } else if (!isKnown) {
      slotsOwned.put(sourceIp, 1);
      added = true;
    }
    if (added) {
      reconcileWriteEndpoints();
    } else {
      rebuildWeightedMap();
    }
  }

  /**
   * Reconcile the derived routing state from {@link #slotsOwned}, the single
   * source of truth in v4 mode.
   * <p>
   * {@link #writeEndpoints} is rebuilt to be exactly the set of brokers tracked
   * in {@code slotsOwned} (so a broker dropped from {@code slotsOwned} -- whether
   * by an eviction draining its slots or by a connection failure -- automatically
   * falls out of the write set, and an eviction target is added immediately).
   * Each tracked broker is also ensured present in {@link #localityEndpoints} so
   * future failovers and reconnects can still see it. Finally the weighted map
   * is rebuilt. Synthesized endpoints (for an IP not previously known) reuse the
   * cluster port of an existing endpoint and the producer's configured locality.
   */
  private void reconcileWriteEndpoints() {
    List<Endpoint> currentLocals = this.localityEndpoints;
    List<Endpoint> newLocals = null;
    List<Endpoint> writes = new ArrayList<>(slotsOwned.size());
    for (String ip : slotsOwned.keySet()) {
      Endpoint ep = findOrCreateEndpoint(ip);
      writes.add(ep);
      if (!containsByIp(currentLocals, ip)
          && (newLocals == null || !containsByIp(newLocals, ip))) {
        if (newLocals == null) {
          newLocals = new ArrayList<>(currentLocals);
        }
        newLocals.add(ep);
      }
    }
    if (newLocals != null) {
      this.localityEndpoints = Collections.unmodifiableList(newLocals);
    }
    this.writeEndpoints = Collections.unmodifiableList(writes);
    rebuildWeightedMap();
  }

  private boolean writeEndpointsContains(String ip) {
    return containsByIp(this.writeEndpoints, ip);
  }

  private static boolean containsByIp(List<Endpoint> list, String ip) {
    for (Endpoint e : list) {
      if (e.getAddress().getHostString().equals(ip)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Look up an Endpoint by IP across known endpoint sets, or synthesize a new
   * one. The port is inferred from existing endpoints (MemQ brokers in a
   * cluster share the same port). The locality is set to the producer's
   * configured locality so the synthesized endpoint participates in
   * locality-aware filtering on future reconnects.
   */
  private Endpoint findOrCreateEndpoint(String ip) {
    for (Endpoint e : this.localityEndpoints) {
      if (e.getAddress().getHostString().equals(ip)) {
        return e;
      }
    }
    for (Endpoint e : this.writeEndpoints) {
      if (e.getAddress().getHostString().equals(ip)) {
        return e;
      }
    }
    int port = inferDefaultPort();
    return new Endpoint(InetSocketAddress.createUnresolved(ip, port), this.locality);
  }

  private int inferDefaultPort() {
    List<Endpoint> locals = this.localityEndpoints;
    if (locals != null && !locals.isEmpty()) {
      return locals.get(0).getAddress().getPort();
    }
    List<Endpoint> writes = this.writeEndpoints;
    if (writes != null && !writes.isEmpty()) {
      return writes.get(0).getAddress().getPort();
    }
    return 9092;
  }

  public String getProducerId() {
    return producerId;
  }

  /**
   * Snapshot of {@code slotsOwned} sent on every v4+ write request so the
   * broker can register this producer's connection set and (on v5) its
   * per-broker slot distribution.
   * <p>
   * The map's key set is the producer's connection set; the values are
   * the producer's slot ownership snapshot per broker. On v5 the broker
   * uses these counts to choose consolidation targets where the
   * producer already has substantial slot share (anti-ping-pong) and to
   * break ties in regular target selection. On v4 the values are
   * dropped on the wire and the broker reconstructs equal-weight 1s.
   *
   * @return a snapshot map of broker IP to slot count owned there.
   */
  public Map<String, Integer> getCurrentConnectionSlots() {
    if (slotsOwned.isEmpty()) {
      return Collections.emptyMap();
    }
    return new LinkedHashMap<>(slotsOwned);
  }

  public ConcurrentHashMap<String, Integer> getSlotsOwned() {
    return slotsOwned;
  }

  /**
   * Number of live TCP channels this client holds to brokers (physical
   * connection count). Counts only active pooled channels. May briefly exceed
   * the owned-endpoint count during bootstrap or right after a broker drop,
   * since a channel can be open to a broker where no slots are yet owned.
   */
  public int getActiveChannelCount() {
    return networkClient.getActiveChannelCount();
  }

  /**
   * Number of broker endpoints on which this producer currently owns at least
   * one slot -- the connection set reported to brokers and the quantity the
   * broker-side eviction caps via {@code maxConnectionsPerProducer}.
   */
  public int getOwnedEndpointCount() {
    return slotsOwned.size();
  }

  /**
   * Whether the client has observed any v4-only signal from a broker (eviction
   * directive or non-zero slot ownership). Once true, the legacy
   * {@code numWriteEndpoints} fan-out gates are no-ops and routing is governed
   * by the weighted endpoint map.
   */
  public boolean isV4Active() {
    return v4Active;
  }

  public boolean isClosed() {
    return networkClient.isClosed();
  }
}

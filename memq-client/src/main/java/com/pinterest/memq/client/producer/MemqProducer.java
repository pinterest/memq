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
package com.pinterest.memq.client.producer;

import static com.pinterest.memq.client.commons.CommonConfigs.AUDITOR_ENABLED;
import static com.pinterest.memq.client.commons.CommonConfigs.BOOTSTRAP_SERVERS;
import static com.pinterest.memq.client.commons.CommonConfigs.CLUSTER;
import static com.pinterest.memq.client.commons.CommonConfigs.SERVERSET_FILE;
import static com.pinterest.memq.client.commons.ProducerConfigs.CLIENT_LOCALITY;
import static com.pinterest.memq.client.commons.ProducerConfigs.DEFAULT_ACK_CHECKPOLLINTERVAL_MS;
import static com.pinterest.memq.client.commons.ProducerConfigs.DEFAULT_COMPRESSION_TYPE;
import static com.pinterest.memq.client.commons.ProducerConfigs.DEFAULT_DISABLE_ACKS;
import static com.pinterest.memq.client.commons.ProducerConfigs.DEFAULT_LOCALITY;
import static com.pinterest.memq.client.commons.ProducerConfigs.DEFAULT_MAX_INFLIGHT_REQUESTS;
import static com.pinterest.memq.client.commons.ProducerConfigs.DEFAULT_MAX_PAYLOADBYTES;
import static com.pinterest.memq.client.commons.ProducerConfigs.DEFAULT_REQUEST_ACKS_TIMEOUT_MS;
import static com.pinterest.memq.client.commons.ProducerConfigs.KEY_SERIALIZER;
import static com.pinterest.memq.client.commons.ProducerConfigs.REQUEST_ACKS_CHECKPOLLINTERVAL_MS;
import static com.pinterest.memq.client.commons.ProducerConfigs.REQUEST_ACKS_DISABLE;
import static com.pinterest.memq.client.commons.ProducerConfigs.REQUEST_ACKS_TIMEOUT_MS;
import static com.pinterest.memq.client.commons.ProducerConfigs.REQUEST_COMPRESSION_TYPE;
import static com.pinterest.memq.client.commons.ProducerConfigs.REQUEST_MAX_INFLIGHTREQUESTS;
import static com.pinterest.memq.client.commons.ProducerConfigs.REQUEST_MAX_PAYLOADBYTES;
import static com.pinterest.memq.client.commons.ProducerConfigs.REQUEST_TIMEOUT;
import static com.pinterest.memq.client.commons.ProducerConfigs.TOPIC_NAME;
import static com.pinterest.memq.client.commons.ProducerConfigs.VALUE_SERIALIZER;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.pinterest.memq.client.commons.AuditorUtils;
import com.pinterest.memq.client.commons.Compression;
import com.pinterest.memq.client.commons.MemqCommonClient;
import com.pinterest.memq.client.commons.audit.Auditor;
import com.pinterest.memq.client.commons.audit.KafkaBackedAuditor;
import com.pinterest.memq.client.commons.serde.Serializer;
import com.pinterest.memq.client.producer.netty.MemqNettyProducer;
import com.pinterest.memq.commons.MessageId;
import com.pinterest.memq.commons.config.SSLConfig;
import com.pinterest.memq.commons.protocol.Broker;
import com.pinterest.memq.commons.protocol.TopicMetadata;

public abstract class MemqProducer<K, V> {

  private static final Logger LOG = LoggerFactory.getLogger(MemqProducer.class);
  public static final int PAYLOADHEADER_BYTES = 8;
  @SuppressWarnings("rawtypes")
  private static Map<String, MemqProducer> clientMap = new ConcurrentHashMap<>();

  public static synchronized <K, V> MemqProducer<K, V> getInstance(Properties properties) throws Exception {
    return getInstance(properties, false);
  }

  @SuppressWarnings("unchecked")
  public static synchronized <K, V> MemqProducer<K, V> getInstance(Properties properties,
                                                                   boolean disableCache) throws Exception {
    String serversetFile = properties.getProperty(SERVERSET_FILE);
    String topicName = properties.getProperty(TOPIC_NAME);
    String cluster = properties.getProperty(CLUSTER);
    int maxInflightRequest = Integer.parseInt(
        properties.getProperty(REQUEST_MAX_INFLIGHTREQUESTS, DEFAULT_MAX_INFLIGHT_REQUESTS));
    int maxPayLoadBytes = Integer
        .parseInt(properties.getProperty(REQUEST_MAX_PAYLOADBYTES, DEFAULT_MAX_PAYLOADBYTES));
    Compression compression = Compression
        .valueOf(properties.getProperty(REQUEST_COMPRESSION_TYPE, DEFAULT_COMPRESSION_TYPE));
    boolean disableAcks = Boolean
        .parseBoolean(properties.getProperty(REQUEST_ACKS_DISABLE, DEFAULT_DISABLE_ACKS));
    int ackCheckPollInterval = Integer.parseInt(properties
        .getProperty(REQUEST_ACKS_CHECKPOLLINTERVAL_MS, DEFAULT_ACK_CHECKPOLLINTERVAL_MS));
    int requestTimeout = Integer
        .parseInt(properties.getProperty(REQUEST_TIMEOUT, DEFAULT_REQUEST_ACKS_TIMEOUT_MS));
    int requestAckTimeout = Integer
        .parseInt(properties.getProperty(REQUEST_ACKS_TIMEOUT_MS, DEFAULT_REQUEST_ACKS_TIMEOUT_MS));
    String locality = properties.getProperty(CLIENT_LOCALITY, DEFAULT_LOCALITY);
    
    String bootstrapServers = properties.getProperty(BOOTSTRAP_SERVERS);

    Serializer<K> keySerializer;
    Serializer<V> valueSerializer;
    try {
      keySerializer = Class.forName(properties.getProperty(KEY_SERIALIZER))
          .asSubclass(Serializer.class).newInstance();
      valueSerializer = Class.forName(properties.getProperty(VALUE_SERIALIZER))
          .asSubclass(Serializer.class).newInstance();
    } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
      throw new Exception("Failed to initialize serializer", e);
    }

    Properties auditConfig = null;
    boolean auditEnabled = Boolean.parseBoolean(properties.getProperty(AUDITOR_ENABLED, "false"));
    if (auditEnabled) {
      auditConfig = AuditorUtils.extractAuditorConfig(properties);
    }

    SSLConfig sslConfigs = null;

    String clientId = serversetFile + "/" + topicName;
    MemqProducer<K, V> memqClient = clientMap.get(clientId);
    if (memqClient == null || memqClient.isClosed()) {
      if (serversetFile!=null) {
      memqClient = new MemqNettyProducer<>(cluster, serversetFile, topicName, maxInflightRequest,
          maxPayLoadBytes, compression, disableAcks, ackCheckPollInterval, requestTimeout, locality,
          requestAckTimeout, auditConfig, sslConfigs);
      }else if (bootstrapServers !=null){
        Set<Broker> bootstrapBrokers = MemqCommonClient.getBootstrapBrokers(bootstrapServers);
        memqClient = new MemqNettyProducer<>(cluster, bootstrapBrokers, topicName, maxInflightRequest,
            maxPayLoadBytes, compression, disableAcks, ackCheckPollInterval, requestTimeout, locality,
            requestAckTimeout, auditConfig, sslConfigs);
      }
      memqClient.keySerializer = keySerializer;
      memqClient.valueSerializer = valueSerializer;
      if (!disableCache) {
        clientMap.put(clientId, memqClient);
      }
    }
    return memqClient;
  }

  protected abstract boolean isClosed();

  protected long epoch = System.currentTimeMillis();
  protected Serializer<K> keySerializer;
  protected Serializer<V> valueSerializer;
  protected Map<Long, TaskRequest> requestMap;
  protected int maxPayLoadBytes;
  protected AtomicLong currentRequestId;
  protected Future<MemqWriteResult> currentRequest;
  protected TaskRequest currentRequestTask;
  protected int maxInflightRequests;
  protected Semaphore maxRequestLock;
  protected Compression compression;
  protected int ackCheckPollIntervalMs;
  protected boolean disableAcks;
  protected String topicName;
  protected String locality;
  protected int requestAckTimeout;
  protected Auditor auditor;
  protected String cluster;
  protected SSLConfig sslConfig;

  public MemqProducer(String cluster,
                      String topicName,
                      int maxInflightRequest,
                      int maxPayLoadBytes,
                      Compression compression,
                      boolean disableAcks,
                      int ackCheckPollIntervalMs,
                      String locality,
                      int requestAckTimeout,
                      Properties auditorConfig,
                      SSLConfig sslConfig) throws Exception {
    this.cluster = cluster;
    this.locality = locality;
    this.requestAckTimeout = requestAckTimeout;
    this.sslConfig = sslConfig;
    this.requestMap = new ConcurrentHashMap<>();
    this.maxInflightRequests = maxInflightRequest;
    this.topicName = topicName;
    this.compression = compression;
    this.disableAcks = disableAcks;
    this.ackCheckPollIntervalMs = ackCheckPollIntervalMs;
    this.currentRequestId = new AtomicLong(0L);
    this.maxRequestLock = new Semaphore(maxInflightRequests);
    this.maxPayLoadBytes = maxPayLoadBytes;
    if (auditorConfig != null) {
      String auditorClass = auditorConfig.getProperty("class",
          KafkaBackedAuditor.class.getCanonicalName());
      this.auditor = Class.forName(auditorClass).asSubclass(Auditor.class).newInstance();
      this.auditor.init(auditorConfig);
    }
  }

  public synchronized Future<MemqWriteResult> writeToTopic(K key, V value) throws IOException {
    return writeToTopic(null, null, key, value, System.currentTimeMillis());
  }

  public synchronized Future<MemqWriteResult> writeToTopic(MessageId messageId,
                                                           K key,
                                                           V value,
                                                           long writeTimestamp) throws IOException {
    return writeToTopic(messageId, null, key, value, writeTimestamp);
  }

  public synchronized Future<MemqWriteResult> writeToTopic(K key,
                                                           V value,
                                                           long writeTimestamp) throws IOException {
    return writeToTopic(null, null, key, value, writeTimestamp);
  }

  public synchronized Future<MemqWriteResult> writeToTopic(MessageId messageId,
                                                           K key,
                                                           V value) throws IOException {
    return writeToTopic(messageId, null, key, value, System.currentTimeMillis());
  }

  public synchronized Future<MemqWriteResult> writeToTopic(Map<String, byte[]> headers,
                                                           K key,
                                                           V value) throws IOException {
    return writeToTopic(null, headers, key, value, System.currentTimeMillis());
  }

  public synchronized Future<MemqWriteResult> writeToTopic(MessageId messageId,
                                                           Map<String, byte[]> headers,
                                                           K key,
                                                           V value,
                                                           long writeTimestamp) throws IOException {
    byte[] keyBytes = keySerializer.serialize(key);
    byte[] valueBytes = valueSerializer.serialize(value);
    byte[] headerBytes = serializeHeadersToByteArray(headers);
    int totalPayloadLength = calculateTotalPayloadLength(messageId, keyBytes, valueBytes,
        headerBytes);
    if (totalPayloadLength > getMaxMessageSize()) {
      // drop data
      return null;
    }
    TaskRequest request = warmAndGetRequestEntry(getCurrentRequestId().get());
    if (getCurrentRequestTask().remaining() > totalPayloadLength) {
      // note compression estimation isn't used here so we may be leaving unused bytes
      writeMemqLogMessage(messageId, headerBytes, keyBytes, valueBytes, request, writeTimestamp);
      return getCurrentRequest();
    } else {
      finalizeRequest();
      request = warmAndGetRequestEntry(getCurrentRequestId().get());
      writeMemqLogMessage(messageId, headerBytes, keyBytes, valueBytes, request, writeTimestamp);
      return getCurrentRequest();
    }
  }

  private int calculateTotalPayloadLength(MessageId messageId,
                                          byte[] keyBytes,
                                          byte[] valueBytes,
                                          byte[] headerBytes) {
    return valueBytes.length + (keyBytes != null ? keyBytes.length : 0) + 11// additional field
                                                                            // 8+1+2
        + (messageId != null ? messageId.toByteArray().length : 0)
        + (headerBytes != null ? headerBytes.length : 0);
  }

  public static void writeMemqLogMessage(MessageId messageId,
                                         byte[] headerBytes,
                                         byte[] keyBytes,
                                         byte[] valueBytes,
                                         TaskRequest request,
                                         long writeTimestamp) throws IOException {
    OutputStream os = request.getOutputStream();

    // #######################################
    // write additional fields here in future
    // #######################################
    // 8 bytes for write ts
    // 1 byte for messageId length
    // 2 bytes for header length
    ByteArrayOutputStream out = new ByteArrayOutputStream(11);
    DataOutputStream str = new DataOutputStream(out);
    // write timestamp
    str.writeLong(writeTimestamp);
    // message id
    if (messageId != null) {
      byte[] ary = messageId.toByteArray();
      str.write((byte) ary.length);
      str.write(ary);
    } else {
      str.write((byte) 0);
    }
    // encode and write user defined headers
    if (headerBytes != null) {
      str.writeShort(headerBytes.length);
      str.write(headerBytes);
    } else {
      str.writeShort(0);
    }
    // #######################################
    // write additional fields here in future
    // #######################################
    str.close();
    byte[] byteArray = out.toByteArray();
    os.write(ByteBuffer.allocate(2).putShort((short) byteArray.length).array());
    os.write(byteArray);

    ByteBuffer keyLength = ByteBuffer.allocate(4);
    if (keyBytes != null) {
      // mark keys present
      keyLength.putInt(keyBytes.length);
      os.write(keyLength.array());
      os.write(keyBytes);
    } else {
      keyLength.putInt(0);
      os.write(keyLength.array());
    }
    os.write(ByteBuffer.allocate(4).putInt(valueBytes.length).array());
    os.write(valueBytes);

    // record the messageId
    request.addMessageId(messageId);

    os.flush();
    request.incrementLogMessageCount();
  }

  public static byte[] serializeHeadersToByteArray(Map<String, byte[]> headers) throws IOException {
    if (headers != null) {
      ByteArrayOutputStream out = new ByteArrayOutputStream(11);
      DataOutputStream str = new DataOutputStream(out);
      for (Entry<String, byte[]> entry : headers.entrySet()) {
        byte[] k = entry.getKey().getBytes();
        byte[] v = entry.getValue();
        str.writeShort(k.length);
        str.write(k);
        str.writeShort(v.length);
        str.write(v);
      }
      str.close();
      return out.toByteArray();
    } else {
      return null;
    }
  }

  protected abstract TaskRequest warmAndGetRequestEntry(Long requestId) throws IOException;

  public synchronized TaskRequest finalizeRequest() throws IOException {
    if (currentRequestTask == null) {
      LOG.warn("Attempt to finalize a request when request is not initialized");
      return null;
    }
    LOG.debug("Buffer filled:" + (double) currentRequestTask.size() / 1024 / 1024);
    currentRequestTask.markReady();
    currentRequestId.incrementAndGet();
    return currentRequestTask;
  }

  public Map<Long, TaskRequest> getRequestMap() {
    return requestMap;
  }

  public abstract void close() throws IOException;

  public enum ClientType {
                          HTTP,
                          TCP
  }

  public TaskRequest getCurrentRequestTask() {
    return currentRequestTask;
  }

  public Future<MemqWriteResult> getCurrentRequest() {
    return currentRequest;
  }

  public AtomicLong getCurrentRequestId() {
    return currentRequestId;
  }

  public Serializer<K> getKeySerializer() {
    return keySerializer;
  }

  public void setKeySerializer(Serializer<K> keySerializer) {
    this.keySerializer = keySerializer;
  }

  public Serializer<V> getValueSerializer() {
    return valueSerializer;
  }

  public void setValueSerializer(Serializer<V> valueSerializer) {
    this.valueSerializer = valueSerializer;
  }

  protected int getMaxMessageSize() {
    return maxPayLoadBytes - TaskRequest.COMPRESSION_WINDOW;
  }

  public Auditor getAuditor() {
    return auditor;
  }

  public String getCluster() {
    return cluster;
  }

  public void reinitialize() throws IOException {
  }

  public void cancelAll() {
  }

  public void awaitConnect(int i, TimeUnit seconds) throws InterruptedException {
  }

  public TopicMetadata getTopicMetadata(String topic, Duration timeout) throws Exception {
    // NOT implemented in abstract class
    return null;
  }

}

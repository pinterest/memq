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

import com.pinterest.memq.client.commons.Compression;
import com.pinterest.memq.client.commons.MemqMessageHeader;
import com.pinterest.memq.client.commons.audit.Auditor;
import com.pinterest.memq.client.commons.audit.KafkaBackedAuditor;
import com.pinterest.memq.client.commons.serde.Serializer;
import com.pinterest.memq.client.commons2.Endpoint;
import com.pinterest.memq.client.commons2.MemqCommonClient;
import com.pinterest.memq.client.commons2.retry.FullJitterRetryStrategy;
import com.pinterest.memq.client.commons2.retry.RetryStrategy;
import com.pinterest.memq.client.producer.MemqWriteResult;
import com.pinterest.memq.commons.MessageId;
import com.pinterest.memq.commons.config.SSLConfig;
import com.pinterest.memq.commons.protocol.Broker;
import com.pinterest.memq.commons.protocol.TopicMetadata;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.NoopMetricRegistry;
import com.codahale.metrics.Timer;
import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Future;

public class MemqProducer<K, V> implements Closeable {
  protected static final Map<String, MemqProducer<?, ?>> clientMap = new HashMap<>();
  private static final Logger logger = LoggerFactory.getLogger(MemqProducer.class);

  private final String cluster;
  private final int maxPayloadBytes;

  private final Serializer<K> keySerializer;
  private final Serializer<V> valueSerializer;

  private final long epoch = System.currentTimeMillis();
  private final MemqCommonClient client;
  private final RequestManager requestManager;
  private final Auditor auditor;
  private final MetricRegistry metricRegistry;
  private Counter writeTooLargeMessagesCounter;
  private Histogram writeMessageSizeHistogram;
  private Counter writeMessageCounter;
  private Timer writeTimer;

  protected MemqProducer(String cluster,
                         String topic,
                         String locality,
                         List<Endpoint> bootstrapEndpoints,
                         Serializer<K> keySerializer,
                         Serializer<V> valueSerializer,
                         Properties auditProperties,
                         Properties networkProperties,
                         SSLConfig sslConfig,
                         long sendRequestTimeout,
                         RetryStrategy retryStrategy,
                         int maxPayloadBytes,
                         int lingerMs,
                         int maxInflightRequests,
                         Compression compression,
                         boolean disableAcks,
                         MetricRegistry metricRegistry,
                         MemqCommonClient memqCommonClient) throws Exception {
    this.cluster = cluster;
    this.maxPayloadBytes = maxPayloadBytes;
    this.keySerializer = keySerializer;
    this.valueSerializer = valueSerializer;
    this.metricRegistry = metricRegistry;
    try {
      if (memqCommonClient != null) {
        this.client = memqCommonClient;
      } else {
        this.client = new MemqCommonClient(locality, sslConfig, networkProperties);
      }
      this.requestManager =
          new RequestManager(client, topic, this, sendRequestTimeout, retryStrategy, maxPayloadBytes, lingerMs, maxInflightRequests, compression, disableAcks, metricRegistry);
      if (auditProperties != null) {
        String
            auditorClass =
            auditProperties.getProperty("class", KafkaBackedAuditor.class.getCanonicalName());
        this.auditor = Class.forName(auditorClass).asSubclass(Auditor.class).newInstance();
        this.auditor.init(auditProperties);
      } else {
        this.auditor = null;
      }
      initializeMetrics();
      initializeTopicConnection(bootstrapEndpoints, topic);
    } catch (Exception e) {
      close();
      throw e;
    }
  }

  private void initializeMetrics() {
    writeTooLargeMessagesCounter = metricRegistry.counter("producer.write.too_large_messages");
    writeMessageCounter = metricRegistry.counter("producer.write.message");
    writeMessageSizeHistogram = metricRegistry.histogram("producer.write.message.size");
    writeTimer = metricRegistry.timer("producer.write.time");
  }

  private void initializeTopicConnection(List<Endpoint> bootstrapEndpoints, String topic) throws Exception {
    client.initialize(bootstrapEndpoints);
    TopicMetadata topicMetadata = client.getTopicMetadata(topic);
    Set<Broker> brokers = topicMetadata.getWriteBrokers();
    logger.debug("Fetched topic metadata, now reconnecting to one of the serving brokers:" + brokers);
    client.resetEndpoints(MemqCommonClient.generateEndpointsFromBrokers(brokers));
  }

  /**
   * @param key
   * @param value
   * @return
   * @throws IOException
   */
  public Future<MemqWriteResult> write(K key,
                                       V value) throws IOException {
    return write(null, null, key, value, System.currentTimeMillis());
  }

  public Future<MemqWriteResult> write(K key,
                                       V value,
                                       long writeTimestamp) throws IOException {
    return write(null, null, key, value, writeTimestamp);
  }

  public Future<MemqWriteResult> write(MessageId messageId,
                                       K key,
                                       V value,
                                       long writeTimestamp) throws IOException {
    return write(messageId, null, key, value, writeTimestamp);
  }


  public Future<MemqWriteResult> write(MessageId messageId,
                                       K key,
                                       V value) throws IOException {
    return write(messageId, null, key, value, System.currentTimeMillis());
  }

  public Future<MemqWriteResult> write(Map<String, byte[]> headers,
                                       K key,
                                       V value) throws IOException {
    return write(null, headers, key, value, System.currentTimeMillis());
  }

  // returns null if the record is dropped
  public Future<MemqWriteResult> write(MessageId messageId,
                                       Map<String, byte[]> headers,
                                       K key,
                                       V value,
                                       long writeTimestamp) throws IOException {
    byte[] keyBytes = keySerializer.serialize(key);
    byte[] valueBytes = valueSerializer.serialize(value);
    RawRecord record = RawRecord.newInstance(messageId, headers, keyBytes, valueBytes, writeTimestamp);
    int encodedMessageLength = record.calculateEncodedLogMessageLength();
    if (encodedMessageLength > maxPayloadBytes - MemqMessageHeader.getHeaderLength()) {
      writeTooLargeMessagesCounter.inc();
      return null;
    }
    try(Timer.Context ctx = writeTimer.time()) {

      writeMessageCounter.inc();
      writeMessageSizeHistogram.update(encodedMessageLength);

      return requestManager.write(record);
    }
  }

  public MetricRegistry getMetricRegistry() {
    return metricRegistry;
  }

  public void flush() {
    requestManager.flush();
  }

  @Override
  public void close() throws IOException {
    if (requestManager != null) {
      requestManager.close();
    }

    if (client != null) {
      client.close();
    }

    if (auditor != null) {
      auditor.close();
    }
  }

  @VisibleForTesting
  protected int getAvailablePermits() {
    return requestManager.getAvailablePermits();
  }

  public String getCluster() {
    return cluster;
  }

  public long getEpoch() {
    return epoch;
  }

  public Auditor getAuditor() {
    return auditor;
  }

  public static class Builder<K, V> {

    private String cluster;
    private String topic;
    private String locality = Endpoint.DEFAULT_LOCALITY;
    private String serversetFile;
    private String bootstrapServers;
    private Serializer<K> keySerializer;
    private Serializer<V> valueSerializer;
    private Properties auditProperties = null;
    private Properties networkProperties = null;
    private SSLConfig sslConfig = null;
    private long sendRequestTimeout = 5000;
    private RetryStrategy retryStrategy = null;
    private int maxPayloadBytes = 1024 * 1024; // 1 MB
    private int lingerMs = 10;
    private int maxInflightRequests = 30;
    private Compression compression = Compression.ZSTD;
    private boolean disableAcks = false;
    private MetricRegistry metricRegistry = null;
    private MemqCommonClient client = null;
    private boolean memoize = false;

    public Builder() {

    }

    public Builder(Builder<K, V> builder) {
      cluster = builder.cluster;
      topic = builder.topic;
      locality = builder.locality;
      serversetFile = builder.serversetFile;
      bootstrapServers = builder.bootstrapServers;
      keySerializer = builder.keySerializer;
      valueSerializer = builder.valueSerializer;
      auditProperties = builder.auditProperties;
      networkProperties = builder.networkProperties;
      sslConfig = builder.sslConfig;
      sendRequestTimeout = builder.sendRequestTimeout;
      retryStrategy = builder.retryStrategy;
      maxPayloadBytes = builder.maxPayloadBytes;
      lingerMs = builder.lingerMs;
      maxInflightRequests = builder.maxPayloadBytes;
      compression = builder.compression;
      disableAcks = builder.disableAcks;
      metricRegistry = builder.metricRegistry;
      client = builder.client;
      memoize = builder.memoize;
    }

    public Builder<K, V> cluster(String cluster) {
      this.cluster = cluster;
      return this;
    }

    public Builder<K, V> topic(String topic) {
      this.topic = topic;
      return this;
    }

    public Builder<K, V> locality(String locality) {
      this.locality = locality;
      return this;
    }

    public Builder<K, V> keySerializer(Serializer<K> keySerializer) {
      this.keySerializer = keySerializer;
      return this;
    }

    public Builder<K, V> valueSerializer(Serializer<V> valueSerializer) {
      this.valueSerializer = valueSerializer;
      return this;
    }

    public Builder<K, V> serversetFile(String serversetFile) {
      this.serversetFile = serversetFile;
      return this;
    }

    public Builder<K, V> bootstrapServers(String bootstrapServers) {
      this.bootstrapServers = bootstrapServers;
      return this;
    }

    public Builder<K, V> auditProperties(Properties auditProperties) {
      this.auditProperties = auditProperties;
      return this;
    }

    public Builder<K, V> networkProperties(Properties networkProperties) {
      this.networkProperties = networkProperties;
      return this;
    }

    public Builder<K, V> sslConfig(SSLConfig sslConfig) {
      this.sslConfig = sslConfig;
      return this;
    }

    public Builder<K, V> sendRequestTimeout(long sendRequestTimeout) {
      this.sendRequestTimeout = sendRequestTimeout;
      return this;
    }

    public Builder<K, V> retryStrategy(RetryStrategy retryStrategy) {
      this.retryStrategy = retryStrategy;
      return this;
    }

    public Builder<K, V> maxPayloadBytes(int maxPayloadBytes) {
      this.maxPayloadBytes = maxPayloadBytes;
      return this;
    }

    public Builder<K, V> lingerMs(int lingerMs) {
      this.lingerMs = lingerMs;
      return this;
    }

    public Builder<K, V> maxInflightRequests(int maxInflightRequests) {
      this.maxInflightRequests = maxInflightRequests;
      return this;
    }

    public Builder<K, V> compression(Compression compression) {
      this.compression = compression;
      return this;
    }

    public Builder<K, V> disableAcks(boolean disableAcks) {
      this.disableAcks = disableAcks;
      return this;
    }

    public Builder<K, V> metricRegistry(MetricRegistry metricRegistry) {
      this.metricRegistry = metricRegistry;
      return this;
    }

    @VisibleForTesting
    protected Builder<K, V> injectClient(MemqCommonClient client) {
      this.client = client;
      return this;
    }

    @SuppressWarnings("unchecked")
    public MemqProducer<K, V> build() throws Exception {
      if (memoize) {
        MemqProducer<K, V> ret;
        synchronized (clientMap) {
          String key = cluster + "/" + topic;
          ret = (MemqProducer<K, V>) clientMap.get(key);
          if (ret == null || ret.client.isClosed()) {
            ret = create();
            clientMap.put(key, ret);
          }
        }
        return ret;
      }
      return create();
    }

    private MemqProducer<K, V> create() throws Exception {
      validate();

      List<Endpoint> bootstrapEndpoints = generateEndpoints();

      if (retryStrategy == null) {
        retryStrategy = new FullJitterRetryStrategy();
      }

      if (metricRegistry == null) {
        metricRegistry = new NoopMetricRegistry();
      }

      return new MemqProducer<>(
          cluster,
          topic,
          locality,
          bootstrapEndpoints,
          keySerializer,
          valueSerializer,
          auditProperties,
          networkProperties,
          sslConfig,
          sendRequestTimeout,
          retryStrategy,
          maxPayloadBytes,
          lingerMs,
          maxInflightRequests,
          compression,
          disableAcks,
          metricRegistry,
          client);
    }

    private void validate() throws Exception {
      if (cluster == null) {
        throw new Exception("cluster is null");
      }
      if (topic == null) {
        throw new Exception("topic is null");
      }
      if (serversetFile == null && bootstrapServers == null) {
        throw new Exception("serversetFile and bootstrapServers cannot be both null");
      }
      if (keySerializer == null) {
        throw new Exception("keySerializer is null");
      }
      if (valueSerializer == null) {
        throw new Exception("valueSerializer is null");
      }
    }

    private List<Endpoint> generateEndpoints() throws Exception {
      List<Endpoint> endpoints;
      if (serversetFile != null) {
        return MemqCommonClient.parseServersetFile(serversetFile);
      } else {
        endpoints = MemqCommonClient.getEndpointsFromBootstrapServerString(bootstrapServers);
      }
      return endpoints;
    }

    public Builder<K, V> memoize() {
      this.memoize = true;
      return this;
    }
  }
}

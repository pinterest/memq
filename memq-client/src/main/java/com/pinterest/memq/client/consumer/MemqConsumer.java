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
package com.pinterest.memq.client.consumer;

import static com.pinterest.memq.client.commons.CommonConfigs.CLUSTER;
import static com.pinterest.memq.client.commons.ConsumerConfigs.BUFFER_FILES_DIRECTORY_KEY;
import static com.pinterest.memq.client.commons.ConsumerConfigs.BUFFER_TO_FILE_CONFIG_KEY;
import static com.pinterest.memq.client.commons.ConsumerConfigs.CLIENT_ID;
import static com.pinterest.memq.client.commons.ConsumerConfigs.DIRECT_CONSUMER;
import static com.pinterest.memq.client.commons.ConsumerConfigs.DRY_RUN_KEY;
import static com.pinterest.memq.client.commons.ConsumerConfigs.KEY_DESERIALIZER_CLASS_CONFIGS_KEY;
import static com.pinterest.memq.client.commons.ConsumerConfigs.KEY_DESERIALIZER_CLASS_KEY;
import static com.pinterest.memq.client.commons.ConsumerConfigs.NOTIFICATION_SOURCE_PROPS_KEY;
import static com.pinterest.memq.client.commons.ConsumerConfigs.NOTIFICATION_SOURCE_PROPS_PREFIX_KEY;
import static com.pinterest.memq.client.commons.ConsumerConfigs.STORAGE_PROPS_PREFIX_KEY;
import static com.pinterest.memq.client.commons.ConsumerConfigs.USE_DIRECT_BUFFER_KEY;
import static com.pinterest.memq.client.commons.ConsumerConfigs.VALUE_DESERIALIZER_CLASS_CONFIGS_KEY;
import static com.pinterest.memq.client.commons.ConsumerConfigs.VALUE_DESERIALIZER_CLASS_KEY;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Properties;
import java.util.Queue;
import java.util.Set;
import java.util.SortedMap;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.ImmutableMap;
import com.google.gson.JsonObject;
import com.pinterest.memq.client.commons.AuditorUtils;
import com.pinterest.memq.client.commons.ConsumerConfigs;
import com.pinterest.memq.client.commons.Deserializer;
import com.pinterest.memq.client.commons.MemqLogMessageIterator;
import com.pinterest.memq.client.commons.audit.Auditor;
import com.pinterest.memq.client.commons.audit.KafkaBackedAuditor;
import com.pinterest.memq.client.commons.serde.ByteArrayDeserializer;
import com.pinterest.memq.client.commons2.ClosedException;
import com.pinterest.memq.client.commons2.DataNotFoundException;
import com.pinterest.memq.client.commons2.Endpoint;
import com.pinterest.memq.client.commons2.MemqCommonClient;
import com.pinterest.memq.commons.BatchHeader;
import com.pinterest.memq.commons.BatchHeader.IndexEntry;
import com.pinterest.memq.commons.CloseableIterator;
import com.pinterest.memq.commons.MemqLogMessage;
import com.pinterest.memq.commons.protocol.TopicMetadata;
import com.pinterest.memq.commons.storage.StorageHandler;
import com.pinterest.memq.commons.storage.StorageHandlerTable;
import com.pinterest.memq.core.utils.MiscUtils;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.buffer.PooledByteBufAllocator;

/**
 * Consumer class for MemQ data. This consumer follows conventions to other
 * PubSub consumers like Kafka and Pulsar.
 * 
 * The consumer has 2 mode direct and indirect. Direct consumer allows custom
 * notification configurations allowing users to override what data is being
 * read and how reads are configured. Indirect consumer performs metadata query
 * on the broker to get topic metadata before initializing storage
 * configurations, this allows cluster to provide updated metadata rather than
 * static configuration.
 */
public final class MemqConsumer<K, V> implements Closeable {

  private static final Logger logger = Logger.getLogger(MemqConsumer.class.getCanonicalName());

  public static final String METRICS_PREFIX = "memq.consumer";
  private final MemqLogMessage<K, V> EMPTY_MESSAGE = new MemqLogMessage<>(null, null, null, null, true);

  public MemqLogMessageIterator<K, V> EMPTY_ITERATOR = new MemqLogMessageIterator<K, V>() {

    @Override
    public boolean hasNext() {
      return false;
    }

    @Override
    public MemqLogMessage<K, V> next() {
      return null;
    }

    @Override
    public void close() {
    }
  };
  private Deserializer<K> keyDeserializer;
  private Deserializer<V> valueDeserializer;
  private Properties properties;
  private ConcurrentLinkedQueue<JsonObject> notificationQueue;
  private String topicName;
  private KafkaNotificationSource notificationSource;
  private boolean isBufferToFile;
  private boolean isDirectBuffer;
  private String bufferFilesDirectory;
  private boolean dryRun;
  private volatile boolean closed = false;

  // metrics
  private MetricRegistry metricRegistry;
  private File bufferFile;
  private String cluster;
  private Auditor auditor;
  private Counter iteratorExceptionCounter;
  private Counter loadBatchExceptionCounter;
  private boolean directConsumer;
  private MemqCommonClient client;
  private Properties notificationSourceProps;
  private Properties additionalStorageProps;
  private int metadataTimeout = 10000;
  private String groupId;

  private StorageHandler storageHandler;

  @SuppressWarnings("unchecked")
  public MemqConsumer(Properties props) throws Exception {
    this.properties = props;
    this.cluster = props.getProperty(CLUSTER);
    this.metricRegistry = new MetricRegistry();
    this.isBufferToFile = Boolean
        .parseBoolean(props.getProperty(BUFFER_TO_FILE_CONFIG_KEY, "false"));
    this.isDirectBuffer = Boolean.parseBoolean(props.getProperty(USE_DIRECT_BUFFER_KEY, "false"));

    String clientId = props.getProperty(CLIENT_ID, UUID.randomUUID().toString());
    checkAndConfigureTmpFileBuffering(props, clientId);
    this.notificationQueue = new ConcurrentLinkedQueue<>();
    this.dryRun = Boolean.parseBoolean(props.getProperty(DRY_RUN_KEY, "false"));

    this.directConsumer = Boolean.parseBoolean(props.getProperty(DIRECT_CONSUMER, "true"));
    try {
      String valueDeserializerClassName = props.getProperty(VALUE_DESERIALIZER_CLASS_KEY,
          ByteArrayDeserializer.class.getName());
      this.valueDeserializer = (Deserializer<V>) Class.forName(valueDeserializerClassName)
          .newInstance();
      this.valueDeserializer.init((Properties) props.get(VALUE_DESERIALIZER_CLASS_CONFIGS_KEY));
    } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
      throw e;
    }
    try {
      String keyDeserializerClassName = props.getProperty(KEY_DESERIALIZER_CLASS_KEY,
          ByteArrayDeserializer.class.getName());
      this.keyDeserializer = (Deserializer<K>) Class.forName(keyDeserializerClassName)
          .newInstance();
      this.keyDeserializer.init((Properties) props.get(KEY_DESERIALIZER_CLASS_CONFIGS_KEY));
    } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
      throw e;
    }

    // initialize auditor
    if (Boolean.parseBoolean(props.getProperty(ConsumerConfigs.AUDITOR_ENABLED, "false"))) {
      this.auditor = Class.forName(props.getProperty(ConsumerConfigs.AUDITOR_CLASS,
          KafkaBackedAuditor.class.getCanonicalName())).asSubclass(Auditor.class).newInstance();
      Properties auditorConfig = AuditorUtils.extractAuditorConfig(props);
      this.auditor.init(auditorConfig);
    }

    initializeMetrics();

    // fetch consumer metadata
    if (directConsumer) {
      if (!dryRun) {
        String storageType = props.getProperty("storageHandler", "customs3aync2");
        storageHandler = StorageHandlerTable.getClass(storageType).newInstance();
        storageHandler.initReader(props, metricRegistry);
        initializeNotificationSource();
      }
    } else {
      this.groupId = props.getProperty(ConsumerConfigs.GROUP_ID);
      if (groupId == null) {
        throw new Exception("Missing consumer group id");
      }
      additionalStorageProps = new Properties();
      additionalStorageProps.setProperty(ConsumerConfigs.BOOTSTRAP_SERVERS,
          props.getProperty(ConsumerConfigs.BOOTSTRAP_SERVERS));
      notificationSourceProps = (Properties) properties.get(NOTIFICATION_SOURCE_PROPS_KEY);
      if (notificationSourceProps == null) {
        notificationSourceProps = new Properties();
      }
      for (Entry<Object, Object> prop : properties.entrySet()) {
        String key = prop.getKey().toString();
        if (key.startsWith(NOTIFICATION_SOURCE_PROPS_PREFIX_KEY)) {
          notificationSourceProps.put(key.replace(NOTIFICATION_SOURCE_PROPS_PREFIX_KEY, ""),
              prop.getValue());
        } else if (key.startsWith(STORAGE_PROPS_PREFIX_KEY)) {
          additionalStorageProps.put(key.replace(STORAGE_PROPS_PREFIX_KEY, ""), prop.getValue());
        }
      }

      String bootstrapServers = props.getProperty(ConsumerConfigs.BOOTSTRAP_SERVERS);
      List<Endpoint> endpoints = MemqCommonClient
          .getEndpointsFromBootstrapServerString(bootstrapServers);
      client = new MemqCommonClient("", null, null);
      client.initialize(endpoints);
    }
  }

  public MemqConsumer(Properties props, StorageHandler storageHandler) throws Exception {
    this(props);
    this.storageHandler = storageHandler;
  }

  protected void initializeMetrics() {
    if (metricRegistry != null) {
      iteratorExceptionCounter = metricRegistry.counter("iterator.exception");
      loadBatchExceptionCounter = metricRegistry.counter("loading.exception");

      if (isDirectBuffer) {
        metricRegistry.gauge("netty.direct.memory.used",
            () -> () -> PooledByteBufAllocator.DEFAULT.metric().usedDirectMemory());
        metricRegistry.gauge("netty.heap.memory.used",
            () -> () -> PooledByteBufAllocator.DEFAULT.metric().usedHeapMemory());
      }
    }
  }

  private void checkAndConfigureTmpFileBuffering(Properties props, String clientId) {
    if (isBufferToFile) {
      bufferFilesDirectory = props.getProperty(BUFFER_FILES_DIRECTORY_KEY, "/tmp");
      bufferFilesDirectory += "/" + clientId;
      new File(bufferFilesDirectory).mkdirs();
      bufferFile = new File(
          bufferFilesDirectory + "/objectBufferFile_" + Thread.currentThread().getId());
      bufferFile.deleteOnExit();
    }
  }

  // for testing
  public MemqConsumer(ConcurrentLinkedQueue<JsonObject> notificationQueue,
                      Deserializer<K> keyDeserializer,
                      Deserializer<V> valueDeserializer) throws Exception {
    this.notificationQueue = notificationQueue;
    this.dryRun = true;
    this.keyDeserializer = keyDeserializer;
    this.valueDeserializer = valueDeserializer;
    this.metricRegistry = new MetricRegistry();
  }

  /**
   * Set fields based on nextNotificationToProcess
   *
   * @param nextNotificationToProcess a JsonObject of the next notification in the
   *                                  queue from compaction topic
   * @throws IOException
   * @throws DataNotFoundException
   */
  protected InputStream fetchBatchStreamForNotification(JsonObject nextNotificationToProcess)
      throws IOException, DataNotFoundException, ClosedException {
    if (closed) {
      throw new ClosedException();
    }
    return storageHandler.fetchBatchStreamForNotification(nextNotificationToProcess);
  }

  /**
   * Fetch a Memq data object and returns a DataInputStream of the object
   *
   * @return a DataInputStream of the Memq data object
   * @throws DataNotFoundException
   */
  public DataInputStream fetchObjectToInputStream(JsonObject nextNotificationToProcess)
      throws IOException, DataNotFoundException, ClosedException {

    if (closed) {
      throw new ClosedException();
    }
    InputStream stream = fetchBatchStreamForNotification(nextNotificationToProcess);
    InputStream newStream;
    try {
      if (isBufferToFile) {
        IOUtils.copy(stream, new FileOutputStream(bufferFile));
        newStream = new FileInputStream(bufferFile);
      } else if (isDirectBuffer) {
        ByteBuf byteBuf = PooledByteBufAllocator.DEFAULT
            .directBuffer(storageHandler.getBatchSizeFromNotification(nextNotificationToProcess));
        ByteBufOutputStream out = new ByteBufOutputStream(byteBuf);
        IOUtils.copy(stream, out);
        newStream = new ByteBufInputStream(byteBuf, true);
        out.close();
      } else {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        IOUtils.copy(stream, out);
        newStream = new ByteArrayInputStream(out.toByteArray());
        out.close();
      }
      return new DataInputStream(newStream);
    } finally {
      try {
        // attempt to close stream if not already closed to avoid memory leaks
        stream.close();
      } catch (Exception e) {
      }
    }
  }

  /**
   * Subscribe to topicNames
   *
   * @param topicNames the Memq topicNames to subscribe to
   * @throws Exception
   */
  public void subscribe(Collection<String> topicNames) throws Exception {
    if (topicNames.size() > 1) {
      // only support single topic subscription for now
      throw new UnsupportedOperationException(
          "MemqConsumer only supports subscription to a single topic (singleton collection)");
    }
    subscribe(topicNames.iterator().next());
  }

  public void subscribe(String topicName) throws Exception {
    if (!topicName.isEmpty() && this.topicName != null && !this.topicName.equals(topicName)) {
      // cannot subscribe to another topic
      throw new UnsupportedOperationException(
          "MemqConsumer doesn't currently support multi-topic subscription");
    }
    this.topicName = topicName;
    if (!directConsumer) {
      initializeNotificationSource(topicName);
    }
  }

  public List<Integer> getPartition() {
    List<Integer> partitions = new ArrayList<>();
    for (PartitionInfo partitionInfo : notificationSource.getPartitions()) {
      partitions.add(partitionInfo.partition());
    }
    Collections.sort(partitions);
    return partitions;
  }

  public TopicMetadata getTopicMetadata(String topic, int timeoutMillis) throws Exception {
    return client.getTopicMetadata(topic, timeoutMillis);
  }

  /**
   * Unsubscribe from currently subscribed topics
   */
  public void unsubscribe() {
    topicName = null;
  }

  /**
   * Initialize notification source
   * 
   * @throws Exception
   */
  private void initializeNotificationSource() throws Exception {
    try {
      Properties notificationSourceProperties = (Properties) properties
          .get(NOTIFICATION_SOURCE_PROPS_KEY);
      this.notificationSource = new KafkaNotificationSource(notificationSourceProperties);
      notificationSource.setParentConsumer(this);
    } catch (Exception e) {
      logger.log(Level.SEVERE, "Failed to initialize notification source", e);
      throw e;
    }
  }

  private void initializeNotificationSource(String topic) throws Exception {
    if (!directConsumer) {
      if (notificationSource != null) {
        notificationSource.close();
        notificationSource = null;
      }
      TopicMetadata topicMetadata = client.getTopicMetadata(topic, metadataTimeout);
      Properties storageProperties = topicMetadata.getStorageHandlerConfig();
      storageProperties.putAll(additionalStorageProps);
      storageProperties.setProperty(ConsumerConfigs.TOPIC_INTERNAL_PROP, topic);
      notificationSourceProps.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
      notificationSourceProps.putAll(storageProperties);
      storageHandler = StorageHandlerTable.getClass(topicMetadata.getStorageHandlerName())
          .newInstance();
      storageHandler.initReader(storageProperties, metricRegistry);
    }
    if (notificationSource == null) {
      notificationSource = new KafkaNotificationSource(notificationSourceProps);
      notificationSource.setParentConsumer(this);
    }
  }

  public void commitOffset() {
    if (!dryRun) {
      notificationSource.commit();
    }
  }

  public void commitOffset(Map<Integer, Long> offsetMap) {
    if (!dryRun) {
      notificationSource.commit(offsetMap);
    }
  }

  public void commitOffsetAsync() {
    if (!dryRun) {
      notificationSource.commitAsync();
    }
  }

  public void commitOffsetAsync(OffsetCommitCallback callback) {
    if (!dryRun) {
      notificationSource.commitAsync(callback);
    }
  }

  public void commitOffsetAsync(Map<Integer, Long> offsetMap, OffsetCommitCallback callback) {
    if (!dryRun) {
      notificationSource.commitAsync(offsetMap, callback);
    }
  }

  public Map<Integer, Long> offsetsOfTimestamps(Map<Integer, Long> partitionTimestamps) {
    return notificationSource.offsetsForTimestamps(partitionTimestamps);
  }

  public Map<Integer, Long> getEarliestOffsets(Collection<Integer> partitions) {
    return notificationSource.getEarliestOffsets(partitions);
  }

  public Map<Integer, Long> getLatestOffsets(Collection<Integer> partitions) {
    return notificationSource.getLatestOffsets(partitions);
  }

  public long position(int partition) {
    return notificationSource.position(partition);
  }

  public long committed(int partition) {
    return notificationSource.committed(partition);
  }

  /**
   * Close memq consumer
   */
  @Override
  public void close() throws IOException {
    closed = true;
    notificationQueue.clear();
    if (notificationSource != null) {
      notificationSource.close();
    }
    if (storageHandler != null) {
      storageHandler.closeReader();
    }
    if (client != null) {
      client.close();
    }
  }

  public void seek(Map<Integer, Long> map) {
    notificationQueue.clear();
    getNotificationSource().seek(map);
  }

  public void seekToBeginning(Collection<Integer> partitions) {
    notificationQueue.clear();
    getNotificationSource().seekToBeginning(partitions);
  }

  public void seekToEnd(Collection<Integer> partitions) {
    notificationQueue.clear();
    getNotificationSource().seekToEnd(partitions);
  }

  protected BatchHeader fetchHeaderForBatch(JsonObject nextNotificationToProcess) throws IOException,
                                                                                  DataNotFoundException {
    return storageHandler.fetchHeaderForBatch(nextNotificationToProcess);
  }

  public Set<Integer> assignment() {
    Set<TopicPartition> assignments = getNotificationSource().getAssignments();
    return assignments.stream().map(p -> p.partition()).collect(Collectors.toSet());
  }

  public MemqLogMessage<K, V> getLogMessageAtCurrentOffset(Duration timeout,
                                                           int partition,
                                                           long batchOffset,
                                                           int messageIndex,
                                                           int logMessageIndex) throws IOException,
                                                                                DataNotFoundException {
    // first index
    notificationQueue.clear();
    getNotificationSource().assign(Arrays.asList(partition));
    getNotificationSource().seek(ImmutableMap.of(partition, batchOffset));
    if (notificationQueue.isEmpty()
        && notificationSource.lookForNewObjects(timeout, notificationQueue) <= 0
        && notificationQueue.isEmpty()) {
//      throw new IOException("No new object");
      return null;
    }

    // second index
    JsonObject objectNotification = notificationQueue.poll();

    return getMessageFromNotification(messageIndex, logMessageIndex, objectNotification);
  }

  private MemqLogMessage<K, V> getMessageFromNotification(int messageIndex,
                                                          int logMessageIndex,
                                                          JsonObject objectNotification) throws IOException,
                                                                                         DataNotFoundException {
    BatchHeader header = fetchHeaderForBatch(objectNotification);
    SortedMap<Integer, IndexEntry> idx = header.getMessageIndex();
    if (messageIndex == -1) {
      messageIndex = idx.lastKey();
    }
    IndexEntry indexEntry = idx.get(messageIndex);
    if (indexEntry == null) {
      // TODO throw no such message
      return null;
    }
    DataInputStream stream = fetchMessageAtIndex(objectNotification, indexEntry);

    // third index
    try {
      if (logMessageIndex == -1) {
        logMessageIndex = Integer.MAX_VALUE;
      }
      try (MemqLogMessageIterator<K, V> iterator = new MemqLogMessageIterator<>(cluster, groupId,
          stream, objectNotification, getKeyDeserializer(), getValueDeserializer(),
          getMetricRegistry(), true, auditor)) {
        int i = 0;
        MemqLogMessage<K, V> msg = null;
        // TODO make this code more robust
        while (i <= logMessageIndex && iterator.hasNext()) {
          msg = iterator.next();
          i++;
        }
        return msg;
      }
    } finally {
      stream.close();
    }
  }

  public List<MemqLogMessage<K, V>> getLogMessagesAtOffsets(Duration timeout,
                                                            int[] partitions,
                                                            long[] batchOffsets,
                                                            int[] messageIndexes,
                                                            int[] logMessageIndexes,
                                                            ExecutorService downloadPool) throws IllegalArgumentException,
                                                                                          TimeoutException {
    if (partitions.length != batchOffsets.length || batchOffsets.length != messageIndexes.length
        || messageIndexes.length != logMessageIndexes.length) {
      throw new IllegalArgumentException("Mismatching argument array lengths");
    }
    Map<Integer, Long> partitionOffsets = new HashMap<>();
    for (int i = 0; i < partitions.length; i++) {
      int partition = partitions[i];
      long batchOffset = batchOffsets[i];
      partitionOffsets.put(partition, batchOffset);
    }

    Map<Integer, JsonObject> notifications = getNotificationSource()
        .getNotificationsAtOffsets(timeout, partitionOffsets);
    List<Future<MemqLogMessage<K, V>>> futures = new ArrayList<>();
    for (int i = 0; i < partitions.length; i++) {
      final int p = i;
      futures.add(downloadPool.submit(() -> {
        JsonObject notification = notifications.get(partitions[p]);
        int retries = 3;
        while (true) {
          try {
            return getMessageFromNotification(messageIndexes[p], logMessageIndexes[p],
                notification);
          } catch (Exception e) {
            if (--retries == 0) {
              throw new ExecutionException(e);
            }
          }
        }
      }));
    }
    List<MemqLogMessage<K, V>> messages = new ArrayList<>();
    for (int i = 0; i < futures.size(); i++) {
      try {
        messages.add(futures.get(i).get());
      } catch (Exception e) {
        logger.log(Level.SEVERE,
            "Failed to retrieve message of partition " + i + " at offset " + batchOffsets[i], e);
        messages.add(null);
      }
    }
    return messages;
  }

  protected DataInputStream fetchMessageAtIndex(JsonObject objectNotification,
                                                IndexEntry index) throws IOException,
                                                                  DataNotFoundException {
    return storageHandler.fetchMessageAtIndex(objectNotification, index);
  }

  public CloseableIterator<MemqLogMessage<K, V>> poll(Duration timeout) throws NoTopicsSubscribedException,
                                                                        IOException {
    return poll(timeout, new MutableInt(0));
  }

  public void wakeup() {
    getNotificationSource().wakeup();
  }

  /**
   * Poll the notification source for new notifications and return an iterator of
   * the polled data objects
   *
   * @param timeout timeout for notification source poll
   * @return a MemqLogMessageIterator
   * @throws IOException
   */
  public CloseableIterator<MemqLogMessage<K, V>> poll(Duration timeout,
                                                      MutableInt numMessages) throws NoTopicsSubscribedException,
                                                                              IOException {
    if (topicName == null) {
      throw new NoTopicsSubscribedException("Currently not subscribed to any topic");
    }

    if (notificationQueue.isEmpty()) {
      int count = notificationSource.lookForNewObjects(timeout, notificationQueue);
      numMessages.add(count);
      if (notificationQueue.isEmpty()) {
        logger.fine("Empty iterator due to no new objects");
        return MiscUtils.emptyCloseableIterator();
      }
    } else {
      numMessages.setValue(notificationQueue.size());
    }
    NotificationBatchIterator tmp = new NotificationBatchIterator(notificationQueue);
    notificationQueue.clear();
    return tmp;
  }

  public Set<Integer> waitForAssignment() throws NoTopicsSubscribedException {
    if (getNotificationSource() == null) {
      throw new NoTopicsSubscribedException(
          "Notification source is null, must subscribe before waiting for assignments");
    }
    Set<TopicPartition> waitForAssignment = getNotificationSource().waitForAssignment();
    return waitForAssignment.stream().map(tp -> tp.partition()).collect(Collectors.toSet());
  }

  public Deserializer<K> getKeyDeserializer() {
    return keyDeserializer;
  }

  public Deserializer<V> getValueDeserializer() {
    return valueDeserializer;
  }

  public Properties getProperties() {
    return properties;
  }

  public ConcurrentLinkedQueue<JsonObject> getNotificationQueue() {
    return notificationQueue;
  }

  public KafkaNotificationSource getNotificationSource() {
    return notificationSource;
  }

  public void setNotificationSource(KafkaNotificationSource notificationSource) {
    this.notificationSource = notificationSource;
  }

  public MetricRegistry getMetricRegistry() {
    return metricRegistry;
  }

  public String getTopicName() {
    return topicName;
  }

  public void setValueDeserializer(Deserializer<V> valueDeserializer) {
    this.valueDeserializer = valueDeserializer;
  }

  public void assign(Collection<Integer> asList) throws Exception {
    if (topicName == null) {
      throw new Exception("Must subscribe to topic before calling assign");
    }
    if (!directConsumer) {
      initializeNotificationSource(topicName);
    }
    notificationSource.assign(asList);
  }

  public StorageHandler getStorageHandler() {
    return storageHandler;
  }

  public final class NotificationBatchIterator implements CloseableIterator<MemqLogMessage<K, V>> {

    private static final int MAX_RETRIES_FOR_BATCH_LOAD_FAILURE = 2;
    private Deque<JsonObject> queue;
    private JsonObject currNotificationObj;
    private MemqLogMessageIterator<K, V> itr;

    public NotificationBatchIterator(Queue<JsonObject> notificationQueue) throws IOException {
      this.queue = new LinkedList<>(notificationQueue);
    }

    /**
     * Loads next notification and returns whether there are bytes to read in the
     * next object
     *
     * @return true if there are object bytes to read, false otherwise
     * @throws IOException
     * @throws DataNotFoundException
     */
    protected MemqLogMessageIterator<K, V> loadNewNotification(JsonObject nextNotification) throws IOException, DataNotFoundException, ClosedException {
      currNotificationObj = nextNotification;
      if (itr != null) {
        closeAndResetIterator();
      }
      DataInputStream objectStream = fetchObjectToInputStream(currNotificationObj);
      // parse batch header
      itr = new MemqLogMessageIterator<>(cluster, groupId, objectStream, currNotificationObj,
          keyDeserializer, valueDeserializer, metricRegistry, false, auditor);
      return itr;
    }

    @Override
    public boolean hasNext() {
      return !queue.isEmpty() || (itr != null && itr.hasNext());
    }

    @Override
    public MemqLogMessage<K, V> next() {
      if (itr == null) {
        JsonObject poll = queue.poll();
        if (poll == null) {
          // if there are no messages in the poll due to DataNotFound exception the return
          // empty message
          return EMPTY_MESSAGE;
        }
        int retryCount = MAX_RETRIES_FOR_BATCH_LOAD_FAILURE;
        while (retryCount > 0) {
          try {
            loadNewNotification(poll);
            break;
          } catch (IOException e) {
            if (retryCount == 1) {
              throw new RuntimeException(e);
            } else {
              loadBatchExceptionCounter.inc();
              logger.log(Level.SEVERE, "Loading batch for object:" + poll + " failed retrying", e);
            }
          } catch (DataNotFoundException e) {
            e.printStackTrace();
            // if data not found then skip this notification, set to empty iterator
            itr = EMPTY_ITERATOR;
            break;
          } catch (ClosedException ce) {
            logger.log(Level.SEVERE, "Consumer has been closed", ce);
            throw new NoSuchElementException("Consumer has been closed");
          }
          retryCount--;
        }
      }
      try {
        if (!itr.hasNext()) {
          closeAndResetIterator();
          return next();
        } else {
          return itr.next();
        }
      } catch (Exception e) {
        logger.log(Level.SEVERE, "Iterator failed for:" + currNotificationObj + " skipping to next",
            e);
        if (iteratorExceptionCounter != null) {
          iteratorExceptionCounter.inc();
        }
        closeAndResetIterator();
        return next();
      }
    }

    private void closeAndResetIterator() {
      try {
        if (itr != null) {
          itr.close();
        }
      } catch (Exception e1) {
        logger.log(Level.WARNING, "Iterator failed to best-effort close: ", e1);
      }
      itr = null;
    }

    public void skipToLastLogMessage() throws IOException, DataNotFoundException, ClosedException {
      JsonObject last = queue.getLast();
      MemqLogMessageIterator<K, V> itr = loadNewNotification(last);
      itr.skipToLastLogMessage();
    }

    @Override
    public void close() throws IOException {
      closeAndResetIterator();
    }
  }

}

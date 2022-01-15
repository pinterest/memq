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

import java.io.File;
import java.nio.file.Files;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

import com.google.common.collect.ImmutableList;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.pinterest.memq.client.producer.http.DaemonThreadFactory;
import com.pinterest.memq.commons.MemqLogMessage;

public class KafkaNotificationSource {

  private static final Logger logger = Logger
      .getLogger(KafkaNotificationSource.class.getCanonicalName());
  public static final String NOTIFICATION_SERVERSET = "notificationServerset";
  public static final String NOTIFICATION_TOPIC_NAME_KEY = "notificationTopic";
  private static final Gson gson = new Gson();

  private final Consumer<String, String> kc;
  private String notificationTopicName;
  private Properties props;
  private MemqConsumer<?, ?> parentConsumer;

  public KafkaNotificationSource(Properties props) throws Exception {
    this.notificationTopicName = props.getProperty(NOTIFICATION_TOPIC_NAME_KEY);
    if (props.containsKey(NOTIFICATION_SERVERSET)) {
      String serverset = props.getProperty(NOTIFICATION_SERVERSET);
      List<String> servers = Files.readAllLines(new File(serverset).toPath());
      props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, servers.get(0));
    }
    validateConsumerProps(props);
    props.putIfAbsent(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
        org.apache.kafka.common.serialization.StringDeserializer.class.getName());
    props.putIfAbsent(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
        org.apache.kafka.common.serialization.StringDeserializer.class.getName());
    props.putIfAbsent(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    props.putIfAbsent(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "100");
    props.putIfAbsent(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
    props.remove(NOTIFICATION_TOPIC_NAME_KEY);
    props.remove(NOTIFICATION_SERVERSET);
    this.props = props;
    kc = new KafkaConsumer<>(props);
    kc.subscribe(Collections.singleton(notificationTopicName));
    logger.info("Initialized notification source with:" + props + " and subscribed to:"
        + notificationTopicName);
  }

  public String getNotificationTopicName() {
    return notificationTopicName;
  }

  protected KafkaNotificationSource(Consumer<String, String> kc) {
    this.kc = kc;
  }

  private static void validateConsumerProps(Properties props) throws Exception {
    if (!props.containsKey(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG)) {
      throw new Exception("bootstrapServers config missing:" + props);
    }
    if (!props.containsKey((NOTIFICATION_TOPIC_NAME_KEY))) {
      throw new Exception("notification topic name missing:" + props);
    }
    if (!props.containsKey(ConsumerConfig.GROUP_ID_CONFIG)) {
      throw new Exception("consumer groupId config missing:" + props);
    }
  }

  public List<PartitionInfo> getPartitions() {
    return kc.partitionsFor(notificationTopicName);
  }

  public long[] getOffsetsForAllPartitions(boolean isBeginning) {
    List<PartitionInfo> partitionInfo = kc.partitionsFor(notificationTopicName);
    List<TopicPartition> partitions = partitionInfo.stream()
        .map(p -> new TopicPartition(notificationTopicName, p.partition()))
        .sorted(Comparator.comparingInt(TopicPartition::partition)).collect(Collectors.toList());
    long[] offsets = new long[partitions.size()];
    Arrays.fill(offsets, -1);
    // scan partition until we find the first offset for the given topic;
    kc.unsubscribe();
    Map<TopicPartition, Long> offsetMap;
    if (isBeginning) {
      offsetMap = kc.beginningOffsets(partitions);
    } else {
      offsetMap = kc.endOffsets(partitions);
    }
    for (Entry<TopicPartition, Long> entry : offsetMap.entrySet()) {
      offsets[entry.getKey().partition()] = entry.getValue();
    }
    return offsets;
  }

  // @Override
  public int lookForNewObjects(Duration timeout, Queue<JsonObject> notificationQueue) {
    if (parentConsumer.getTopicName() == null || parentConsumer.getTopicName().isEmpty()) {
      throw new RuntimeException("No topics subscribed");
    }
    logger.fine("Looking for new objects");
    int c = 0;
    ConsumerRecords<String, String> records = kc.poll(timeout);
    for (ConsumerRecord<String, String> record : records) {
      try {
        JsonObject notificationObject = parseAndGetNotificationObject(record);
        if (notificationObject != null) {
          notificationQueue.add(notificationObject);
        }
        c++;
      } catch (Exception e) {
        e.printStackTrace();
        logger.log(Level.SEVERE, "Unable to process notification topic record", e);
      }
    }
    if (!notificationQueue.isEmpty()) {
      logger.fine(() -> "Size of queue: " + notificationQueue.size());
    }
    return c;
  }

  public List<JsonObject> getNotificationObjectsForAllPartitionsTillCurrent() throws InterruptedException {
    List<JsonObject> list = Collections.synchronizedList(new ArrayList<>());

    ExecutorService es = Executors.newFixedThreadPool(4, new DaemonThreadFactory("objectfetcher"));
    for (PartitionInfo partitionInfo : getPartitions()) {
      final int partitionId = partitionInfo.partition();
      es.submit(() -> {
        try {
          KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
          TopicPartition tp = new TopicPartition(notificationTopicName, partitionId);
          ImmutableList<TopicPartition> set = ImmutableList.of(tp);
          consumer.assign(set);
          Map<TopicPartition, Long> beginningOffsets = consumer.beginningOffsets(set);
          Long beginningOffset = beginningOffsets.get(tp);
          consumer.seek(tp, beginningOffset);
          Map<TopicPartition, Long> endOffsets = consumer.endOffsets(set);
          Long endOffset = endOffsets.get(tp);
          long lastOffset = 0L;
          while (lastOffset < endOffset) {
            for (ConsumerRecord<String, String> consumerRecord : consumer
                .poll(Duration.ofSeconds(10))) {
              lastOffset = consumerRecord.offset();
              JsonObject obj = parseAndGetNotificationObject(consumerRecord);
              if (obj != null) {
                list.add(obj);
              }
            }
          }
          consumer.close();
        } catch (Exception e) {
          e.printStackTrace();
        }
      });
    }
    es.shutdown();
    es.awaitTermination(100, TimeUnit.SECONDS);
    return list;
  }

  protected JsonObject parseAndGetNotificationObject(ConsumerRecord<String, String> record) {
    JsonObject json = gson.fromJson(record.value(), JsonObject.class);
    json.addProperty(MemqLogMessage.INTERNAL_FIELD_NOTIFICATION_PARTITION_ID, record.partition());
    json.addProperty(MemqLogMessage.INTERNAL_FIELD_NOTIFICATION_PARTITION_OFFSET,
        record.offset());
    json.addProperty(MemqLogMessage.INTERNAL_FIELD_NOTIFICATION_READ_TIMESTAMP,
        System.currentTimeMillis());
    return json;
  }

  public void commit(Map<Integer, Long> offsetMap) {
    Map<TopicPartition, OffsetAndMetadata> toCommit = new HashMap<>();
    for (Entry<Integer, Long> entry : offsetMap.entrySet()) {
      TopicPartition tp = new TopicPartition(notificationTopicName, entry.getKey());
      OffsetAndMetadata om = new OffsetAndMetadata(entry.getValue());
      toCommit.put(tp, om);
    }
    kc.commitSync(toCommit);
  }

  // @Override
  public void commit() {
    kc.commitSync();
  }

  public void commitAsync() {
    kc.commitAsync();
  }

  public void commitAsync(OffsetCommitCallback callback) {
    kc.commitAsync((offsets, exception) -> {
      Map<Integer, Long> transformedOffsetMap = new HashMap<>();
      if (offsets != null) {
        for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : offsets.entrySet()) {
          transformedOffsetMap.put(entry.getKey().partition(), entry.getValue().offset());
        }
      }
      callback.onCompletion(transformedOffsetMap, exception);
    });
  }

  public void commitAsync(Map<Integer, Long> offsetMap, OffsetCommitCallback callback) {
    Map<TopicPartition, OffsetAndMetadata> toCommit = new HashMap<>();
    for (Entry<Integer, Long> entry : offsetMap.entrySet()) {
      TopicPartition tp = new TopicPartition(notificationTopicName, entry.getKey());
      OffsetAndMetadata om = new OffsetAndMetadata(entry.getValue());
      toCommit.put(tp, om);
    }
    kc.commitAsync(toCommit, (offsets, exception) -> {
      Map<Integer, Long> transformedOffsetMap = new HashMap<>();
      if (offsets != null) {
        for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : offsets.entrySet()) {
          transformedOffsetMap.put(entry.getKey().partition(), entry.getValue().offset());
        }
      }
      callback.onCompletion(transformedOffsetMap, exception);
    });
  }

  public long committed(int partition) {
    return kc.committed(new TopicPartition(notificationTopicName, partition)).offset();
  }

  public long position(int partition) {
    return kc.position(new TopicPartition(notificationTopicName, partition));
  }

  // @Override
  public void seek(Map<Integer, Long> notificationOffset) {
    for (Entry<Integer, Long> entry : notificationOffset.entrySet()) {
      long offset = entry.getValue();
      kc.seek(new TopicPartition(notificationTopicName, entry.getKey()), offset);
    }
  }

  public void seekToBeginning(Collection<Integer> partitions) {
    kc.seekToBeginning(partitions.stream().map(p -> new TopicPartition(notificationTopicName, p)).collect(Collectors.toSet()));
  }

  public void seekToEnd(Collection<Integer> partitions) {
    kc.seekToEnd(partitions.stream().map(p -> new TopicPartition(notificationTopicName, p)).collect(Collectors.toSet()));
  }

  public Map<Integer, Long> offsetsForTimestamps(Map<Integer, Long> partitionTimestamps) {
    Map<TopicPartition, Long> topicPartitionTimestamps = partitionTimestamps
        .entrySet()
        .stream()
        .collect(
            Collectors.toMap(
                e -> new TopicPartition(notificationTopicName, e.getKey()),
                Entry::getValue
            )
        );
    return kc.offsetsForTimes(topicPartitionTimestamps).entrySet().stream().collect(Collectors.toMap(e -> e.getKey().partition(), e -> e.getValue().offset()));
  }

  // @Override
  public void assign(Collection<Integer> partitions) {
    kc.unsubscribe();
    List<TopicPartition> collect = partitions.stream()
        .map(partition -> new TopicPartition(notificationTopicName, partition))
        .collect(Collectors.toList());
    kc.assign(collect);
  }

  public Map<Integer, Long> getEarliestOffsets(Collection<Integer> partitions) {
    List<TopicPartition> collect = partitions.stream()
        .map(partition -> new TopicPartition(notificationTopicName, partition))
        .collect(Collectors.toList());
    Map<TopicPartition, Long> beginningOffsets = kc.beginningOffsets(collect);
    Map<Integer, Long> result = new HashMap<>();
    for (Entry<TopicPartition, Long> entry : beginningOffsets.entrySet()) {
      result.put(entry.getKey().partition(), entry.getValue());
    }
    logger.info("Earliest offsets:" + result);
    return result;
  }

  public Map<Integer, Long> getLatestOffsets(Collection<Integer> partitions) {
    List<TopicPartition> collect = partitions.stream()
        .map(partition -> new TopicPartition(notificationTopicName, partition))
        .collect(Collectors.toList());
    Map<TopicPartition, Long> beginningOffsets = kc.endOffsets(collect);
    Map<Integer, Long> result = new HashMap<>();
    for (Entry<TopicPartition, Long> entry : beginningOffsets.entrySet()) {
      result.put(entry.getKey().partition(), entry.getValue());
    }
    logger.info("Latest offsets:" + result);
    return result;
  }

  public Map<Integer, JsonObject> getNotificationsAtOffsets(Duration timeout,
                                                            Map<Integer, Long> partitionOffsets) throws TimeoutException {
    Set<TopicPartition> tpSet = new HashSet<>();
    Map<Integer, JsonObject> notificationMap = new HashMap<>();
    for (Map.Entry<Integer, Long> entry : partitionOffsets.entrySet()) {
      TopicPartition tp = new TopicPartition(notificationTopicName, entry.getKey());
      tpSet.add(tp);
    }

    long deadline = timeout.toMillis();
    kc.unsubscribe();
    while (!tpSet.isEmpty() && deadline >= 0) {
      long start = System.currentTimeMillis();
      kc.assign(tpSet);
      for (TopicPartition tp : tpSet) {
        kc.seek(tp, partitionOffsets.get(tp.partition()));
      }
      ConsumerRecords<String, String> records = kc.poll(timeout);
      for (Iterator<TopicPartition> itr = tpSet.iterator(); itr.hasNext();) {
        TopicPartition tp = itr.next();
        for (ConsumerRecord<String, String> record : records.records(tp)) {
          if (record.offset() == partitionOffsets.get(tp.partition())) {
            itr.remove();
            notificationMap.put(tp.partition(), parseAndGetNotificationObject(record));
          }
        }
      }
      deadline -= (System.currentTimeMillis() - start);
    }
    if (deadline < 0 && !tpSet.isEmpty()) {
      throw new TimeoutException(
          "Failed to retrieve all notifications within " + timeout.toMillis() + " seconds");
    }
    return notificationMap;
  }

  // @Override
  public Object getRawObject() {
    return kc;
  }

  // @Override
  public void unsubscribe() {
    kc.unsubscribe();
  }

  // @Override
  public void close() {
    kc.close();
  }

  public void setParentConsumer(MemqConsumer<?, ?> parentConsumer) {
    this.parentConsumer = parentConsumer;
  }

  public Set<TopicPartition> getAssignments() {
    return kc.assignment();
  }

  public void wakeup() {
    kc.wakeup();
  }

  public Set<TopicPartition> waitForAssignment() {
    Set<TopicPartition> assignment = kc.assignment();
    while (assignment.isEmpty()) {
      kc.poll(Duration.ofMillis(500));
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
        logger.warning("Thread interrupted");
        break;
      }
      assignment = kc.assignment();
    }
    return assignment;
  }
}
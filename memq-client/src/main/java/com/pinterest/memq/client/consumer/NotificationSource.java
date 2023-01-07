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

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.TimeoutException;

import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

import com.google.gson.JsonObject;

interface NotificationSource {

  int lookForNewObjects(Duration timeout, Queue<JsonObject> notificationQueue);

  void assign(Collection<Integer> asList);

  void seek(Map<Integer, Long> notificationOffset);

  long position(int partition);

  long committed(int partition);

  public void commit(Map<Integer, Long> offsetMap);

  void commit();

  void commitAsync();

  void commitAsync(OffsetCommitCallback callback);

  void commitAsync(Map<Integer, Long> offsets, OffsetCommitCallback callback);

  Object getRawObject();

  void unsubscribe();

  void close();

  String getNotificationTopicName();

  List<PartitionInfo> getPartitions();

  void setParentConsumer(MemqConsumer<?, ?> memqConsumer);

  Map<Integer, Long> offsetsForTimestamps(Map<Integer, Long> partitionTimestamps);

  Map<Integer, Long> getEarliestOffsets(Collection<Integer> partitions);

  Map<Integer, Long> getLatestOffsets(Collection<Integer> partitions);

  Set<TopicPartition> waitForAssignment();

  void wakeup();

  Map<Integer, JsonObject> getNotificationsAtOffsets(Duration timeout,
                                                     Map<Integer, Long> partitionOffsets) throws TimeoutException;

  Set<TopicPartition> getAssignments();

  void seekToEnd(Collection<Integer> partitions);

  void seekToBeginning(Collection<Integer> partitions);

}

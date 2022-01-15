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
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

import com.google.gson.JsonObject;

interface NotificationSource {

  void init(Set<String> subscribedTopics);
  
  boolean lookForNewObjects(Duration timeout,
                            Queue<JsonObject> notificationQueue);

  void assign(List<String> items);

  void seek(Map<String, byte[]> notificationOffset);

  long position(int partition);

  long committed(int partition);

  Map<Integer, Long> offsetsOfTimestamps(Map<Integer, Long> partitionTimestamps);

  void commit(Map<String, byte[]> notificationOffset);

  void commit();

  void commitAsync();

  void commitAsync(OffsetCommitCallback callback);

  void commitAsync(Map<Integer, Long> offsets, OffsetCommitCallback callback);

  Object getRawObject();

  void unsubscribe();

  void close();

  String getNotificationTopicName();

}

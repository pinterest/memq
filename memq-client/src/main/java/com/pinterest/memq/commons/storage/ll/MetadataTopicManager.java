/**
 * Copyright 2023 Pinterest, Inc.
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
package com.pinterest.memq.commons.storage.ll;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.google.gson.JsonObject;

public class MetadataTopicManager {

  private static MetadataTopicManager INSTANCE = null;

  public static MetadataTopicManager getInstance() {
    return INSTANCE;
  }

  private Map<String, MetadataTopic> topicMap = new ConcurrentHashMap<>();

  public long write(String topic, JsonObject payload) {
    MetadataTopic metadataTopic = topicMap.get(topic);
    if (metadataTopic==null) {
      // TODO throw exception 
      return -1;
    }
    return metadataTopic.write(payload);
  }

}

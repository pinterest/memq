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
package com.pinterest.memq.client.commons;

public class ConsumerConfigs extends CommonConfigs {

  public static final String CLIENT_ID = "clientId";
  public static final String USE_STREAMING_ITERATOR = "useStreamingIterator";
  public static final String NOTIFICATION_SOURCE_TYPE_KEY = "notificationSourceType";
  public static final String NOTIFICATION_SOURCE_PROPS_KEY = "notificationSourceProps";
  public static final String NOTIFICATION_SOURCE_PROPS_PREFIX_KEY = "notification.";
  public static final String STORAGE_PROPS_PREFIX_KEY = "storage.";
  public static final String BUFFER_TO_FILE_CONFIG_KEY = "bufferToFile";
  public static final String USE_DIRECT_BUFFER_KEY = "directBuffer";
  public static final String BUFFER_FILES_DIRECTORY_KEY = "bufferFilename";
  public static final String KEY_DESERIALIZER_CLASS_KEY = "key.deserializerclass";
  public static final String VALUE_DESERIALIZER_CLASS_KEY = "value.deserializerclass";
  public static final String KEY_DESERIALIZER_CLASS_CONFIGS_KEY = "key.deserializerclass.configs";
  public static final String VALUE_DESERIALIZER_CLASS_CONFIGS_KEY = "value.deserializerclass.configs";
  public static final String AUTO_COMMIT_PER_POLL_KEY = "autoCommitPerPoll";
  public static final String DRY_RUN_KEY = "dryRun";
  public static final String DIRECT_CONSUMER = "directConsumer";
  public static final String GROUP_ID = "group.id";
  public static final String TOPIC_INTERNAL_PROP = "topic";

}

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

public class ProducerConfigs extends CommonConfigs {

  public static final String TOPIC_NAME = "topic.name";
  public static final String VALUE_SERIALIZER = "value.serializer";
  public static final String KEY_SERIALIZER = "key.serializer";
  public static final String CLIENT_TYPE = "client.type";
  public static final String REQUEST_ACKS_TIMEOUT_MS = "request.acks.timeout.ms";
  public static final String REQUEST_ACKS_CHECKPOLLINTERVAL_MS = "request.acks.checkpollinterval.ms";
  public static final String REQUEST_ACKS_DISABLE = "request.acks.disable";
  public static final String REQUEST_COMPRESSION_TYPE = "request.compression.type";
  public static final String REQUEST_MAX_PAYLOADBYTES = "request.max.payloadbytes";
  public static final String REQUEST_MAX_INFLIGHTREQUESTS = "request.max.inflightrequests";
  public static final String REQUEST_TIMEOUT = "request.timeout";
  
  // Defaults
  public static final String DEFAULT_REQUEST_ACKS_TIMEOUT_MS = "60000";
  public static final String DEFAULT_ACK_CHECKPOLLINTERVAL_MS = "100";
  public static final String DEFAULT_DISABLE_ACKS = "false";
  public static final String DEFAULT_COMPRESSION_TYPE = Compression.ZSTD.name();
  public static final String DEFAULT_MAX_PAYLOADBYTES = String.valueOf(1024 * 1024);
  public static final String DEFAULT_MAX_INFLIGHT_REQUESTS = "30";
  public static final String DEFAULT_LOCALITY = "none";

}

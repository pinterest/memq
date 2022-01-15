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
package com.pinterest.memq.client.consumer.utils.properties;

import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;

import com.pinterest.memq.client.commons.ConsumerConfigs;
import com.pinterest.memq.client.consumer.KafkaNotificationSource;
import com.pinterest.memq.client.consumer.MemqConsumer;

public class MemqConsumerBuilder<H, T> {

  protected Properties properties;
  private Properties notificationSourceProperties;

  public MemqConsumerBuilder() {
    this.properties = new Properties();
    this.notificationSourceProperties = new Properties();
  }

  public MemqConsumerBuilder<H, T> setConsumerGroupId(String consumerGroupId) {
    notificationSourceProperties.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroupId);
    return this;
  }

  public MemqConsumerBuilder<H, T> setHeaderDeserializerClassname(String headerDeserializerClassname) {
    properties.put(ConsumerConfigs.KEY_DESERIALIZER_CLASS_KEY, headerDeserializerClassname);
    return this;
  }

  public MemqConsumerBuilder<H, T> setHeaderDeserializerClassConfigs(Properties headerDeserializerClassConfigs) {
    properties.put(ConsumerConfigs.KEY_DESERIALIZER_CLASS_CONFIGS_KEY,
        headerDeserializerClassConfigs);
    return this;
  }

  public MemqConsumerBuilder<H, T> setValueDeserializerClassname(String valueDeserializerClassname) {
    properties.put(ConsumerConfigs.VALUE_DESERIALIZER_CLASS_KEY, valueDeserializerClassname);
    return this;
  }

  public MemqConsumerBuilder<H, T> setValueDeserializerClassConfigs(Properties valueDeserializerClassConfigs) {
    properties.put(ConsumerConfigs.VALUE_DESERIALIZER_CLASS_CONFIGS_KEY,
        valueDeserializerClassConfigs);
    return this;
  }

  public MemqConsumerBuilder<H, T> bufferObjectFetchToFile() {
    properties.put(ConsumerConfigs.BUFFER_TO_FILE_CONFIG_KEY, true);
    return this;
  }

  public MemqConsumerBuilder<H, T> setBufferFilesDirectory(String directory) {
    properties.put(ConsumerConfigs.BUFFER_FILES_DIRECTORY_KEY, directory);
    return this;
  }

  public MemqConsumerBuilder<H, T> dryRun() {
    properties.put(ConsumerConfigs.DRY_RUN_KEY, true);
    return this;
  }

  public MemqConsumerBuilder<H, T> useStreamingIterator() {
    properties.put(ConsumerConfigs.USE_STREAMING_ITERATOR, true);
    return this;
  }

  public Properties getProperties() {
    return properties;
  }

  private void validateProperties() throws PropertiesInitializationException {
    if (!properties.containsKey(ConsumerConfigs.KEY_DESERIALIZER_CLASS_KEY)) {
      throw new PropertiesInitializationException(
          "Missing header deserializer config for MemqConsumer");
    }
    if (!properties.containsKey(ConsumerConfigs.VALUE_DESERIALIZER_CLASS_KEY)) {
      throw new PropertiesInitializationException(
          "Missing value deserializer config for MemqConsumer");
    }
  }

  private void loadDefaultNotificationSourceProperties() {
    notificationSourceProperties.put(ConsumerConfigs.NOTIFICATION_SOURCE_TYPE_KEY, "kafka");
    notificationSourceProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
    notificationSourceProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
        StringDeserializer.class.getName());
    notificationSourceProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
        StringDeserializer.class.getName());
    notificationSourceProperties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 10);
  }

  public MemqConsumer<H, T> getMemqS3Consumer() throws Exception {
    loadDefaultNotificationSourceProperties();
    properties.put(ConsumerConfigs.NOTIFICATION_SOURCE_PROPS_KEY, notificationSourceProperties);
    validateProperties();
    return new MemqConsumer<>(properties);
  }

  public MemqConsumerBuilder<H, T> setNotificationBootstrapServer(String bootstrapServer) {
    notificationSourceProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
    return this;
  }
  
  public MemqConsumerBuilder<H, T> setNotificationTopic(String notificationTopic) {
    notificationSourceProperties.put(KafkaNotificationSource.NOTIFICATION_TOPIC_NAME_KEY,
        notificationTopic);
    return this;
  }
}

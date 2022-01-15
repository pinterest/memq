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
package com.pinterest.memq.commons.storage.s3;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.List;
import java.util.Properties;
import java.util.logging.Logger;

import javax.naming.ConfigurationException;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import com.google.gson.Gson;
import com.google.gson.JsonObject;

public class KafkaNotificationSink {

  private static final Logger logger = Logger.getLogger(KafkaNotificationSink.class.getCanonicalName());
  public static final String NOTIFICATION_SERVERSET = "notificationServerset";
  public static final String NOTIFICATION_TOPIC = "notificationTopic";
  
  private KafkaProducer<String, String> producer;
  private Gson gson = new Gson();
  private String notificationTopic;
  private Properties props;
  private String bootstrapServers;

  public synchronized KafkaNotificationSink init(Properties props) throws Exception {
    this.props = props;
    bootstrapServers = getBootstrapServers(props, 10);
    Properties producerProps = new Properties(props);
    producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    producerProps.put(ProducerConfig.ACKS_CONFIG, "-1");
    producerProps.put(ProducerConfig.RETRIES_CONFIG, "3");
    if (!props.containsKey(NOTIFICATION_TOPIC)) {
      throw new ConfigurationException("Missing notification topic name");
    }
    notificationTopic = props.get(NOTIFICATION_TOPIC).toString();
    props.get(NOTIFICATION_TOPIC).toString();
    if (producer == null) {
      producer = new KafkaProducer<>(producerProps);
      logger.info("Initialized notification sink:" + notificationTopic + " on:" + bootstrapServers);
    }
    return this;
  }

  public void reinitializeSink() throws Exception {
    logger.warning("Notification sink reset triggered");
    producer.close();
    producer = null;
    init(props);
  }

  protected String getBootstrapServers(Properties props, int limit) throws ConfigurationException,
                                                                    IOException {
    if (!props.containsKey(NOTIFICATION_SERVERSET)) {
      throw new ConfigurationException("Missing serverset configuration for notification sink");
    }
    String notificationServerset = props.get(NOTIFICATION_SERVERSET).toString();
    List<String> lines = Files.readAllLines(new File(notificationServerset).toPath());
    lines = lines.subList(0, lines.size() > limit ? limit : lines.size());
    return String.join(",", lines);
  }

  public synchronized void notify(JsonObject payload,
                                  int retryCount
                                  ) throws Exception {
    try {
      producer
          .send(new ProducerRecord<String, String>(notificationTopic, null, gson.toJson(payload)))
          .get();
    } catch (Exception e) {
      reinitializeSink();
      if (retryCount < 2) {
        notify(payload, retryCount++);
      } else {
        throw e;
      }
    }
  }

  public String getReadUrl() {
    return bootstrapServers;
  }

  public void close() {
    producer.close();
  }
}

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
package com.pinterest.memq.client.commons.audit;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;

public class KafkaBackedAuditor extends Auditor {
  private static final Map<String, KafkaProducer<byte[], byte[]>> producers = new ConcurrentHashMap<>();

  private String auditTopic;
  private KafkaProducer<byte[], byte[]> producer;

  public void init(Properties props) throws IOException {
    this.auditTopic = props.getProperty("topic");
    String serverset = props.getProperty("serverset");
    this.producer = getProducer(serverset, props);
  }

  protected static KafkaProducer<byte[], byte[]> getProducer(String serverset, Properties props) throws IOException {
    if (!props.containsKey(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG)) {
      props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
          Files.readAllLines(new File(serverset).toPath()).get(0));
    } else {
      serverset = props.getProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG);
    }
    if (!props.containsKey(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG)) {
      throw new IOException("Missing bootstrap server");
    }
    props.put(ProducerConfig.ACKS_CONFIG, "-1");
    props.put(ProducerConfig.LINGER_MS_CONFIG, "10");
    props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, String.valueOf(100 * 1024));
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
        ByteArraySerializer.class.getCanonicalName());
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
        ByteArraySerializer.class.getCanonicalName());
    return producers.computeIfAbsent(serverset, s -> new KafkaProducer<>(props));
  }

  public void auditMessage(byte[] cluster,
                           byte[] topic,
                           byte[] hostAddress,
                           long epoch,
                           long id,
                           byte[] hash,
                           int count,
                           boolean isProducer,
                           String clientId) throws IOException {
    ByteArrayOutputStream os = new ByteArrayOutputStream();
    ByteArrayOutputStream keyOs = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(os);
    DataOutputStream keyDos = new DataOutputStream(keyOs);
    dos.writeShort(cluster.length);
    dos.write(cluster);
    dos.writeShort(topic.length);
    dos.write(topic);
    dos.writeShort(hostAddress.length);
    dos.write(hostAddress);
    dos.writeLong(epoch);
    dos.writeLong(id);
    keyDos.write(hostAddress);
    keyDos.writeLong(epoch);
    keyDos.writeLong(id);
    dos.writeShort(hash.length);
    dos.write(hash);
    dos.writeInt(count);
    dos.writeBoolean(isProducer);
    producer.send(new ProducerRecord<>(auditTopic, keyOs.toByteArray(), os.toByteArray()));
  }

  public void close() {

  }

}

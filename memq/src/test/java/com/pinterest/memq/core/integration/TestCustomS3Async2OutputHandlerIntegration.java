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
package com.pinterest.memq.core.integration;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.function.BiFunction;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import com.codahale.metrics.MetricRegistry;
import com.google.common.io.Files;
import com.google.gson.JsonObject;
import com.pinterest.memq.client.commons.Compression;
import com.pinterest.memq.client.commons.ConsumerConfigs;
import com.pinterest.memq.client.commons.TestUtils;
import com.pinterest.memq.client.commons.serde.ByteArrayDeserializer;
import com.pinterest.memq.client.consumer.MemqConsumer;
import com.pinterest.memq.commons.MemqLogMessage;
import com.pinterest.memq.commons.storage.s3.CustomS3Async2StorageHandler;
import com.pinterest.memq.core.commons.Message;
import com.pinterest.memq.core.utils.CoreUtils;
import com.salesforce.kafka.test.junit4.SharedKafkaTestResource;
import com.salesforce.kafka.test.listeners.PlainListener;

import io.netty.util.internal.ThreadLocalRandom;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.S3Object;

public class TestCustomS3Async2OutputHandlerIntegration {

  public static final String INTEGRATION_TEST_BUCKET = System.getProperty("integration.test.bucket");
  public static final String BASEPATH = "testing/integration/memq";

  @ClassRule
  public static final SharedKafkaTestResource sharedKafkaTestResource = new SharedKafkaTestResource()
      .withBrokers(1).registerListener(new PlainListener().onPorts(9092));

  @BeforeClass
  public static void beforeClass() {
    S3Client s3 = S3Client.builder().region(Region.US_EAST_1).build();
    List<S3Object> listObjects = s3.listObjectsV2(ListObjectsV2Request.builder()
        .bucket(INTEGRATION_TEST_BUCKET).prefix(BASEPATH).build()).contents();
    for (S3Object s3ObjectSummary : listObjects) {
      s3.deleteObject(
          DeleteObjectRequest.builder().bucket(INTEGRATION_TEST_BUCKET).key(s3ObjectSummary.key()).build());
      System.out.println("Deleting old test data s3://" + INTEGRATION_TEST_BUCKET + "/"
          + s3ObjectSummary.key());
    }
    s3.close();
  }

  @Test
  public void testSimpleS3Uploads() throws Exception {
    String notificationTopic = "testnotifications";
    sharedKafkaTestResource.getKafkaTestUtils().createTopic(notificationTopic, 1, (short) 1);
    CustomS3Async2StorageHandler handler = new CustomS3Async2StorageHandler();
    Properties outputHandlerConfig = new Properties();
    outputHandlerConfig.put("bucket", INTEGRATION_TEST_BUCKET);
    outputHandlerConfig.put("path", BASEPATH + "/test-simpleuploads");
    outputHandlerConfig.put("disableNotifications", "false");
    outputHandlerConfig.put("enableHashing", "false");
    outputHandlerConfig.put("retryTimeoutMillis", "30000");

    Files.write("localhost:9092".getBytes(), new File("target/tests3outputhandlerserverset"));
    outputHandlerConfig.put("notificationServerset", "target/tests3outputhandlerserverset");
    outputHandlerConfig.put("notificationTopic", notificationTopic);
    String topic = "test";
    handler.initWriter(outputHandlerConfig, topic, new MetricRegistry());

    List<Message> messages = new ArrayList<Message>();
    int totalMessages = publishMessages(messages);
    int checksum = CoreUtils.batchChecksum(messages);
    int objectSize = CoreUtils.batchSizeInBytes(messages);
    handler.writeOutput(objectSize, checksum, messages);
    handler.closeWriter();
    
    Properties config = new Properties();
    Properties props = new Properties();
    props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
        sharedKafkaTestResource.getKafkaConnectString());
    config.setProperty(ConsumerConfigs.KEY_DESERIALIZER_CLASS_KEY,
        ByteArrayDeserializer.class.getCanonicalName());
    config.setProperty(ConsumerConfigs.VALUE_DESERIALIZER_CLASS_KEY,
        ByteArrayDeserializer.class.getCanonicalName());
    props.setProperty(ConsumerConfigs.GROUP_ID, "testSimpleS3Uploads1");
    props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    props.setProperty("notificationTopic", notificationTopic);
    config.put(ConsumerConfigs.NOTIFICATION_SOURCE_PROPS_KEY, props);
    config.setProperty(ConsumerConfigs.CLUSTER, topic);
    config.setProperty(ConsumerConfigs.CLIENT_ID, topic);
    

    MemqConsumer<byte[], byte[]> consumer = new MemqConsumer<>(config);
    consumer.subscribe(topic);
    
    Iterator<MemqLogMessage<byte[], byte[]>> iterator = consumer.poll(Duration.ofSeconds(10));
    int counts = 0;
    while (iterator.hasNext()) {
      iterator.next();
      counts++;
    }
    assertEquals(totalMessages, counts);
    consumer.close();
  }

  private int publishMessages(List<Message> messages) throws IOException {
    BiFunction<String, Integer, byte[]> getLogMessageBytes = (base, k) -> (base + k).getBytes();
    long baseRequestId = ThreadLocalRandom.current().nextLong(0, Long.MAX_VALUE);
    int totalMessages = 0;
    // 02/16/2021: increased number of test messages to 100+ to test an edge case that causes corrupted message for
    // batches with more than 64 messages (potentially caused by reactive-stream client)
    for (int i = 0; i < 100 + ThreadLocalRandom.current().nextInt(50); i++) {
      Message m = new Message(1024 * 100, true);
      m.setClientRequestId(baseRequestId + i);
      m.setServerRequestId(baseRequestId + 1000 + i);
      byte[] message = TestUtils.createMessage(UUID.randomUUID().toString(), getLogMessageBytes,
          100, true, Compression.NONE, null, false);
      m.getBuf().writeBytes(message);
      messages.add(m);
      totalMessages += 100;
    }
    return totalMessages;
  }

  public static class ConsumerResponse {

    private byte[] byteArray;
    private JsonObject notification;
    private String bucket;
    private String key;
    private String eTag;

    public ConsumerResponse(byte[] byteArray,
                            JsonObject notification,
                            String bucket,
                            String key,
                            String eTag) {
      this.byteArray = byteArray;
      this.notification = notification;
      this.bucket = bucket;
      this.key = key;
      this.eTag = eTag;
    }

    public byte[] getByteArray() {
      return byteArray;
    }

    public JsonObject getNotification() {
      return notification;
    }

    public String getBucket() {
      return bucket;
    }

    public String getKey() {
      return key;
    }

    public String geteTag() {
      return eTag;
    }

  }

}

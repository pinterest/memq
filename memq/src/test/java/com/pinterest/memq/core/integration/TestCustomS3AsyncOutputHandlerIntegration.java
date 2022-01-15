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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.function.BiFunction;

import org.apache.commons.compress.utils.IOUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import com.codahale.metrics.MetricRegistry;
import com.google.common.io.Files;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.pinterest.memq.client.commons.Compression;
import com.pinterest.memq.client.commons.MemqLogMessageIterator;
import com.pinterest.memq.client.commons.TestUtils;
import com.pinterest.memq.client.commons.serde.ByteArrayDeserializer;
import com.pinterest.memq.commons.storage.s3.AbstractS3StorageHandler;
import com.pinterest.memq.commons.storage.s3.CustomS3AsyncStorageHandler;
import com.pinterest.memq.core.commons.Message;
import com.pinterest.memq.core.utils.CoreUtils;
import com.pinterest.memq.core.utils.MemqUtils;
import com.salesforce.kafka.test.junit4.SharedKafkaTestResource;
import com.salesforce.kafka.test.listeners.PlainListener;

import io.netty.util.internal.ThreadLocalRandom;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.S3Object;

public class TestCustomS3AsyncOutputHandlerIntegration {

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
      s3.deleteObject(DeleteObjectRequest.builder().bucket(INTEGRATION_TEST_BUCKET)
          .key(s3ObjectSummary.key()).build());
      System.out.println(
          "Deleting old test data s3://" + INTEGRATION_TEST_BUCKET + "/" + s3ObjectSummary.key());
    }
    s3.close();
  }

  @Test
  public void testSimpleS3Uploads() throws Exception {
    String notificationTopic = "testnotifications";
    sharedKafkaTestResource.getKafkaTestUtils().createTopic(notificationTopic, 1, (short) 1);
    CustomS3AsyncStorageHandler handler = new CustomS3AsyncStorageHandler();
    Properties outputHandlerConfig = new Properties();
    outputHandlerConfig.put("bucket", INTEGRATION_TEST_BUCKET);
    outputHandlerConfig.put("path", BASEPATH + "/test-simpleuploads");
    outputHandlerConfig.put("disableNotifications", "false");
    outputHandlerConfig.put("enableHashing", "false");

    Files.write("localhost:9092".getBytes(), new File("target/tests3outputhandlerserverset"));
    outputHandlerConfig.put("notificationServerset", "target/tests3outputhandlerserverset");
    outputHandlerConfig.put("notificationTopic", notificationTopic);
    handler.initWriter(outputHandlerConfig, "test", new MetricRegistry());

    List<Message> messages = new ArrayList<Message>();
    int totalMessages = publishMessages(messages);
    int checksum = CoreUtils.batchChecksum(messages);
    int objectSize = CoreUtils.batchSizeInBytes(messages);
    handler.writeOutput(objectSize, checksum, messages);

    ConsumerResponse consumerResponse = extractConsumerObject(notificationTopic,
        "testSimpleS3Uploads");

    String md5 = md5ToBase64(consumerResponse);
    System.out.println("Result:" + md5 + " " + consumerResponse.getBucket() + "/"
        + consumerResponse.getKey() + " etag:" + consumerResponse.geteTag());

    String etagToBase64 = MemqUtils.etagToBase64(consumerResponse.geteTag());

    assertEquals(md5, etagToBase64);

    MemqLogMessageIterator<byte[], byte[]> iterator = new MemqLogMessageIterator<>("test", "test",
        new DataInputStream(new ByteArrayInputStream(consumerResponse.getByteArray())),
        consumerResponse.getNotification(), new ByteArrayDeserializer(),
        new ByteArrayDeserializer(), new MetricRegistry(), false, null);
    int counts = 0;
    while (iterator.hasNext()) {
      iterator.next();
      counts++;
    }
    assertEquals(totalMessages, counts);
    handler.closeWriter();
  }

  private int publishMessages(List<Message> messages) throws IOException {
    BiFunction<String, Integer, byte[]> getLogMessageBytes = (base, k) -> (base + k).getBytes();
    long baseRequestId = ThreadLocalRandom.current().nextLong(0, Long.MAX_VALUE);
    int totalMessages = 0;
    for (int i = 0; i < ThreadLocalRandom.current().nextInt(50); i++) {
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

  public static ConsumerResponse extractConsumerObject(String notificationTopic,
                                                       String groupId) throws Exception {
    Properties config = new Properties();
    config.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
        sharedKafkaTestResource.getKafkaConnectString());
    config.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
    config.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
        StringDeserializer.class.getCanonicalName());
    config.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
        StringDeserializer.class.getCanonicalName());
    config.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    KafkaConsumer<String, String> notificationConsumer = new KafkaConsumer<>(config);
    notificationConsumer.subscribe(Arrays.asList(notificationTopic));
    ConsumerRecords<String, String> poll = notificationConsumer.poll(Duration.ofSeconds(5));
    Gson gson = new Gson();
    JsonObject notification = null;
    for (ConsumerRecord<String, String> consumerRecord : poll) {
      notification = gson.fromJson(consumerRecord.value(), JsonObject.class);
    }
    notificationConsumer.close();

    S3Client s3 = S3Client.builder().region(Region.US_EAST_1).build();
    String bucket = notification.get(AbstractS3StorageHandler.BUCKET).getAsString();
    String key = notification.get(AbstractS3StorageHandler.KEY).getAsString();
    ResponseInputStream<GetObjectResponse> resp = s3
        .getObject(GetObjectRequest.builder().bucket(bucket).key(key).build());
    GetObjectResponse object = resp.response();
    String eTag = object.eTag().replace("\"", "");
    ByteArrayOutputStream os = new ByteArrayOutputStream();
    IOUtils.copy(resp, os);
    os.close();
    byte[] byteArray = os.toByteArray();
    s3.close();

    return new ConsumerResponse(byteArray, notification, bucket, key, eTag);
  }

  private String md5ToBase64(ConsumerResponse consumerResponse) throws NoSuchAlgorithmException {
    MessageDigest md = MessageDigest.getInstance("MD5");
    byte[] digest = md.digest(consumerResponse.getByteArray());
    String md5 = Base64.getEncoder().encodeToString(digest);
    return md5;
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

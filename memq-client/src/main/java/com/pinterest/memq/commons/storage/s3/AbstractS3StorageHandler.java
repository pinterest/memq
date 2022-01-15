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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.logging.Logger;

import org.apache.commons.compress.utils.IOUtils;

import com.codahale.metrics.MetricRegistry;
import com.google.gson.JsonObject;
import com.pinterest.memq.commons.BatchHeader;
import com.pinterest.memq.commons.BatchHeader.IndexEntry;
import com.pinterest.memq.commons.MemqLogMessage;
import com.pinterest.memq.commons.protocol.BatchData;
import com.pinterest.memq.commons.storage.StorageHandler;
import com.pinterest.memq.commons.storage.s3.S3Exception.ForbiddenException;
import com.pinterest.memq.commons.storage.s3.S3Exception.InternalServerErrorException;
import com.pinterest.memq.commons.storage.s3.S3Exception.NotFoundException;
import com.pinterest.memq.commons.storage.s3.S3Exception.ServiceUnavailableException;
import com.pinterest.memq.commons.storage.s3.reader.client.ApacheRequestClient;
import com.pinterest.memq.commons.storage.s3.reader.client.ReactorNettyRequestClient;
import com.pinterest.memq.commons.storage.s3.reader.client.RequestClient;

import software.amazon.awssdk.services.s3.model.GetObjectRequest;

public abstract class AbstractS3StorageHandler implements StorageHandler {

  public static final NotFoundException NOT_FOUND_EXCEPTION = new NotFoundException();
  public static final InternalServerErrorException ISE_EXCEPTION = new InternalServerErrorException();
  public static final ServiceUnavailableException UNAVAILABLE_EXCEPTION = new ServiceUnavailableException();
  public static final ForbiddenException FORBIDDEN_EXCEPTION = new ForbiddenException();

  public static final String OBJECT_FETCH_LATENCY_MS_HISTOGRAM_KEY = "objectFetchLatencyMs";
  public static final String OBJECT_FETCH_ERROR_KEY = "objectFetchErrorKey";
  public static final String TOPIC = "topic";
  public static final String KEY = "key";
  public static final String BUCKET = "bucket";
  public static final String HEADER_SIZE = "headerSize";
  public static final String NUMBER_OF_MESSAGES_IN_BATCH = "numBatchMessages";
  public static final String CONTENT_MD5 = "contentMD5";
  public static final String NUM_ATTEMPTS = "numAttempts";
  public static final String USE_APACHE_HTTP_CLIENT = "useApacheHttpClient";

  private MetricRegistry registry;
  private RequestClient httpClient;

  @Override
  public void initReader(Properties properties, MetricRegistry registry) throws Exception {
    this.registry = registry;
    if (properties.containsKey(USE_APACHE_HTTP_CLIENT)
        && Boolean.parseBoolean(properties.get(USE_APACHE_HTTP_CLIENT).toString())) {
      this.httpClient = new ApacheRequestClient(registry);
    } else {
      this.httpClient = new ReactorNettyRequestClient(registry);
    }
    httpClient.initialize(properties);
  }

  @Override
  public InputStream fetchBatchStreamForNotification(JsonObject nextNotificationToProcess) throws IOException {
    String currentBucket = nextNotificationToProcess.get(BUCKET).getAsString();
    String currentKey = nextNotificationToProcess.get(KEY).getAsString();
    int currentObjectSize = nextNotificationToProcess.get(SIZE).getAsInt();
    getLogger().fine("Updating bucket and key: " + currentBucket + "/" + currentKey + " {"
        + nextNotificationToProcess.get(MemqLogMessage.INTERNAL_FIELD_NOTIFICATION_PARTITION_ID)
            .getAsNumber()
        + ", " + nextNotificationToProcess
            .get(MemqLogMessage.INTERNAL_FIELD_NOTIFICATION_PARTITION_OFFSET).getAsNumber()
        + "}");
    getLogger().finest("Object size: " + currentObjectSize);
    long fetchStartTime = System.currentTimeMillis();
    try {
      return httpClient
          .tryObjectGet(GetObjectRequest.builder().bucket(currentBucket).key(currentKey).build());
    } finally {
      long fetchTime = System.currentTimeMillis() - fetchStartTime;
      getLogger().fine("Fetch Time:" + fetchTime);
      registry.histogram(OBJECT_FETCH_LATENCY_MS_HISTOGRAM_KEY).update(fetchTime);
    }
  }

  @Override
  public BatchData fetchBatchStreamForNotificationBuf(JsonObject nextNotificationToProcess) throws IOException {
    String currentBucket = nextNotificationToProcess.get(BUCKET).getAsString();
    String currentKey = nextNotificationToProcess.get(KEY).getAsString();
    int currentObjectSize = nextNotificationToProcess.get(SIZE).getAsInt();
    getLogger().fine("Updating bucket and key: " + currentBucket + "/" + currentKey + " {"
        + nextNotificationToProcess.get(MemqLogMessage.INTERNAL_FIELD_NOTIFICATION_PARTITION_ID)
            .getAsNumber()
        + ", " + nextNotificationToProcess
            .get(MemqLogMessage.INTERNAL_FIELD_NOTIFICATION_PARTITION_OFFSET).getAsNumber()
        + "}");
    getLogger().finest("Object size: " + currentObjectSize);
    long fetchStartTime = System.currentTimeMillis();
    try {
      return new BatchData(currentObjectSize, httpClient.tryObjectGetAsBuffer(
          GetObjectRequest.builder().bucket(currentBucket).key(currentKey).build()));
    } finally {
      long fetchTime = System.currentTimeMillis() - fetchStartTime;
      getLogger().fine("Fetch Time:" + fetchTime);
      registry.histogram(OBJECT_FETCH_LATENCY_MS_HISTOGRAM_KEY).update(fetchTime);
    }
  }

  @Override
  public BatchHeader fetchHeaderForBatch(JsonObject nextNotificationToProcess) throws IOException {
    String bucketName = nextNotificationToProcess.get(BUCKET).getAsString();
    String key = nextNotificationToProcess.get(KEY).getAsString();
    int headerSize = nextNotificationToProcess.get("headerSize").getAsInt();

    GetObjectRequest getObjectRequest = GetObjectRequest.builder().bucket(bucketName).key(key)
        .range("bytes=0-" + headerSize).build();
    InputStream is = httpClient.tryObjectGet(getObjectRequest);
    DataInputStream dis = new DataInputStream(is);
    BatchHeader batchHeader = new BatchHeader(dis);
    dis.close();
    return batchHeader;
  }

  @Override
  public DataInputStream fetchMessageAtIndex(JsonObject objectNotification,
                                             IndexEntry index) throws IOException {
    String bucketName = objectNotification.get(BUCKET).getAsString();
    String key = objectNotification.get(KEY).getAsString();

    GetObjectRequest getObjectRequest = GetObjectRequest.builder().bucket(bucketName).key(key)
        .range("bytes=" + index.getOffset() + "-" + (index.getOffset() + index.getSize())).build();
    InputStream is = httpClient.tryObjectGet(getObjectRequest);
    return new DataInputStream(is);
  }

  public static DataInputStream convertS3StreamToInMemory(InputStream objectContentStream) throws IOException {
    ByteArrayOutputStream str = new ByteArrayOutputStream();
    IOUtils.copy(objectContentStream, str);
    str.close();
    objectContentStream.close();
    return new DataInputStream(new ByteArrayInputStream(str.toByteArray()));
  }

  public static JsonObject buildPayload(String topic,
                                        String bucket,
                                        int objectSize,
                                        int numberOfMessages,
                                        int batchHeaderLength,
                                        String key,
                                        int attempt) {
    JsonObject payload = new JsonObject();
    payload.addProperty(BUCKET, bucket);
    payload.addProperty(KEY, key);
    payload.addProperty(SIZE, objectSize);
    payload.addProperty(TOPIC, topic);
    payload.addProperty(HEADER_SIZE, batchHeaderLength);
    payload.addProperty(NUMBER_OF_MESSAGES_IN_BATCH, numberOfMessages);
    payload.addProperty(NUM_ATTEMPTS, attempt);
    return payload;
  }

  @Override
  public void closeReader() {
    try {
      httpClient.close();
    } catch (IOException ioe) {
      getLogger().warning("Failed to close http client when closing S3 storage handler");
    }
  }

  public abstract Logger getLogger();

}

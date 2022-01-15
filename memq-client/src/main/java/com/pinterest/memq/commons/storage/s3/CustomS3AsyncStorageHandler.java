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

import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.ProtocolException;
import java.net.URL;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.naming.ConfigurationException;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.compress.utils.IOUtils;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.codahale.metrics.Timer.Context;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.pinterest.memq.commons.storage.StorageHandler;
import com.pinterest.memq.commons.storage.StorageHandlerName;
import com.pinterest.memq.commons.storage.WriteFailedException;
import com.pinterest.memq.core.commons.Message;
import com.pinterest.memq.core.commons.MessageBufferInputStream;
import com.pinterest.memq.core.utils.DaemonThreadFactory;
import com.pinterest.memq.core.utils.MemqUtils;
import com.pinterest.memq.core.utils.MiscUtils;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import software.amazon.awssdk.auth.credentials.InstanceProfileCredentialsProvider;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest.Builder;
import software.amazon.awssdk.services.s3.presigner.S3Presigner;
import software.amazon.awssdk.services.s3.presigner.model.PresignedPutObjectRequest;
import software.amazon.awssdk.services.s3.presigner.model.PutObjectPresignRequest;

@StorageHandlerName(name = "customs3aync")
public class CustomS3AsyncStorageHandler extends AbstractS3StorageHandler {

  private static final int HIGH_LATENCY_THRESHOLD = 5;
  private static final int NANOSECONDS_TO_SECONDS = 1000_000_000;
  private static final int ERROR_CODE = 500;
  private static final int SUCCESS_CODE = 200;
  private static final int FIRST_INDEX = 0;
  private static final String SLASH = "/";
  private static final String CONTENT_LENGTH = "Content-Length";
  private static final String APPLICATION_OCTET_STREAM = "application/octet-stream";
  private static final String CONTENT_MD5 = "Content-MD5";
  private static final String CONTENT_TYPE = "Content-Type";
  private static final String E_TAG = "ETag";
  private static final String S3_REQUEST_ID = "x-amz-request-id";
  private static final String S3_EXTENDED_REQUEST_ID = "x-amz-id-2";
  private static final String SEPARATOR = "_";
  private static final int LAST_ATTEMPT_TIMEOUT = 60_000;

  static {
    java.security.Security.setProperty("networkaddress.cache.ttl", "1");
  }

  private static final String HOSTNAME = MiscUtils.getHostname();
  private static final Gson GSON = new Gson();
  private Logger logger = Logger.getLogger(CustomS3AsyncStorageHandler.class.getName());
  private String path;
  private String bucket;
  private Counter streamResetCounter;
  private KafkaNotificationSink notificationSink;
  private String topic;
  @SuppressWarnings("unused")
  private boolean dryrun;
  private boolean disableNotifications;
  private Timer s3PutLatencyTimer;
  private boolean enableHashing;
  private ExecutorService requestExecutor;
  private int maxAttempts;
  private int retryTimeoutMillis;
  private Counter s3RetryCounters;
  private Timer s3PutInternalLatencyTimer;
  private Timer streamCopyTimer;
  private S3Presigner signer;
  private MetricRegistry registry;
  private Counter notificationFailureCounter;
  private boolean enableMD5;
  private Counter s3RequestCounter;
  private ExecutorService asyncSlowUploadHandlingExecutor;

  public CustomS3AsyncStorageHandler() {
  }

  @Override
  public void initWriter(Properties outputHandlerConfig,
                         String topic,
                         MetricRegistry registry) throws Exception {
    this.logger = Logger.getLogger(CustomS3AsyncStorageHandler.class.getName() + "-" + topic);
    this.topic = topic;
    this.registry = registry;
    this.dryrun = Boolean.parseBoolean(outputHandlerConfig.getProperty("dryrun", "false"));
    this.disableNotifications = Boolean
        .parseBoolean(outputHandlerConfig.getProperty("disableNotifications", "true"));
    if (!disableNotifications) {
      this.notificationSink = new KafkaNotificationSink();
      this.notificationSink.init(outputHandlerConfig);
    }
    this.s3RequestCounter = registry.counter("output.s3.requests");
    this.streamResetCounter = registry.counter("output.s3.streamReset");
    this.notificationFailureCounter = registry.counter("output.notification.fail");
    this.s3PutLatencyTimer = MiscUtils.oneMinuteWindowTimer(registry, "output.s3.putobjectlatency");
    this.s3PutInternalLatencyTimer = MiscUtils.oneMinuteWindowTimer(registry,
        "output.s3.internalPutobjectlatency");
    this.streamCopyTimer = MiscUtils.oneMinuteWindowTimer(registry, "output.s3.streamCopyTime");
    this.bucket = outputHandlerConfig.getProperty("bucket");
    if (bucket == null) {
      throw new ConfigurationException("Missing S3 bucket name");
    }

    this.enableMD5 = Boolean.parseBoolean(outputHandlerConfig.getProperty("enableMD5", "true"));
    if (!enableMD5) {
      logger.warning("MD5 hashes for uploads have been disabled");
    }

    this.enableHashing = Boolean
        .parseBoolean(outputHandlerConfig.getProperty("enableHashing", "true"));
    if (!enableHashing) {
      logger.warning("Hashing has been disabled for object uploads");
    }

    this.path = outputHandlerConfig.getProperty("path", topic);
    this.requestExecutor = Executors.newCachedThreadPool(new DaemonThreadFactory());
    this.asyncSlowUploadHandlingExecutor = Executors.newCachedThreadPool(new DaemonThreadFactory());
    this.s3RetryCounters = registry.counter("output.s3.retries");
    this.retryTimeoutMillis = Integer
        .parseInt(outputHandlerConfig.getProperty("retryTimeoutMillis", "5000"));
    this.maxAttempts = Integer.parseInt(outputHandlerConfig.getProperty("retryCount", "2")) + 1;
    signer = S3Presigner.builder()
        .credentialsProvider(InstanceProfileCredentialsProvider.builder()
            .asyncCredentialUpdateEnabled(true).asyncThreadName("IamCredentialUpdater").build())
        .build();
  }

  @Override
  public void writeOutput(int objectSize,
                          int checksum,
                          final List<Message> messages) throws WriteFailedException {
    Context timer = s3PutLatencyTimer.time();
    ByteBuf ref = null;
    try {
      String key = null;
      ByteBuf header = StorageHandler.getBatchHeadersAsByteArray(messages);
      ref = header;
      String contentMD5 = null;
      UploadResult result = null;
      boolean hasSucceeded = false;
      int i = 0;

      // for s3 tracking
      List<Future<UploadResult>> taskFutures = new ArrayList<>();
      while (!hasSucceeded && i < maxAttempts) {
        final int k = i;
        key = createKey(messages, i).toString();
        String tmpKey = key;
        Future<UploadResult> taskFuture = requestExecutor.submit(() -> {
          return attemptUpload(header.duplicate(), objectSize, checksum, contentMD5, messages,
              tmpKey, k);
        });
        taskFutures.add(taskFuture);
        try {
          if (i < maxAttempts - 1) {
            result = taskFuture.get(retryTimeoutMillis + i * 2000, TimeUnit.MILLISECONDS);
          } else {
            // if this is the last attempt then don't timeout
            result = taskFuture.get(LAST_ATTEMPT_TIMEOUT, TimeUnit.MILLISECONDS);
          }
          // start tracking response codes from s3
          registry.counter("output.s3.responseCode." + result.getResponseCode()).inc();
          if (result.getResponseCode() == SUCCESS_CODE) {
            hasSucceeded = true;
          } else {
            logger.severe("Request failed reason:" + result + " key:" + key);
            if (result.getResponseCode() == ERROR_CODE) {
              continue;
            }
          }
          break;
        } catch (Exception e) {
          e.printStackTrace();
          // TODO: commented out for latency tracking
//          resultFuture.cancel(true);
          s3RetryCounters.inc();
        }
        i++;
      }
      if (!hasSucceeded) {
        throw new WriteFailedException("Upload failed due to error out: " + key);
      }
      if (!disableNotifications) {
        JsonObject payload = buildPayload(topic, bucket, objectSize, messages.size(),
            header.capacity(), key, i);
        try {
          notificationSink.notify(payload, 0);
        } catch (Exception e) {
          notificationFailureCounter.inc();
          throw e;
        }
      }
      long latency = timer.stop() / NANOSECONDS_TO_SECONDS;
      if (latency > HIGH_LATENCY_THRESHOLD) {
        final String s3path = "s3://" + bucket + SLASH + key;
        logger.info("Uploaded " + s3path + " latency(" + latency + ")s");

        asyncSlowUploadHandlingExecutor.submit(() -> {
          Map<String, Object> message = new HashMap<>();
          message.put("s3path", s3path);
          message.put("latencySeconds", latency);
          List<Map<String, Object>> attempts = new ArrayList<>();

          for (int taskId = 0; taskId < taskFutures.size(); taskId++) {
            Map<String, Object> attempt = new HashMap<>();
            try {
              UploadResult res = taskFutures.get(taskId).get();
              if (res.getResponseCode() == 200) {
                attempt.put("requestId", res.getHttpResponseHeaders().get(S3_REQUEST_ID));
                attempt.put("extendedRequestId",
                    res.getHttpResponseHeaders().get(S3_EXTENDED_REQUEST_ID));
              } else {
                attempt.put("responseCode", res.getResponseCode());
              }
              attempt.put("timeMillis", res.getTime() / 1_000_000);
            } catch (Exception e) {
              attempt.put("exception", e.getMessage());
            }
            attempts.add(attempt);
          }
          message.put("attempts", attempts);
          logger.fine(() -> GSON.toJson(message));
        });
      }
    } catch (Exception e) {
      timer.stop();
      throw new WriteFailedException(e);
    } finally {
      if (ref != null) {
        ref.release();
      }
    }
  }

  private UploadResult attemptUpload(ByteBuf header,
                                     int sizeInBytes,
                                     int checksum,
                                     String contentMD5,
                                     final List<Message> messages,
                                     final String key,
                                     int count) throws IOException, ProtocolException {
    header.resetReaderIndex();
    Context internalLatency = s3PutInternalLatencyTimer.time();
    try {
      Builder putRequestBuilder = PutObjectRequest.builder().bucket(bucket).key(key);
      if (contentMD5 != null) {
        putRequestBuilder.contentMD5(contentMD5);
      }
      int length = header.capacity() + sizeInBytes;
      putRequestBuilder.contentLength((long) length);
      MessageBufferInputStream input = new MessageBufferInputStream(messages, streamResetCounter);
      PresignedPutObjectRequest presignPutObject = signer.presignPutObject(
          PutObjectPresignRequest.builder().putObjectRequest(putRequestBuilder.build())
              .signatureDuration(Duration.ofSeconds(2000)).build());
      URL presignedUrl = presignPutObject.url();
      HttpURLConnection connection = (HttpURLConnection) presignedUrl.openConnection();
      connection.setDoOutput(true);
      connection.setRequestProperty(CONTENT_TYPE, APPLICATION_OCTET_STREAM);
      if (contentMD5 != null) {
        connection.setRequestProperty(CONTENT_MD5, contentMD5);
      }
      connection.setRequestProperty(CONTENT_LENGTH, String.valueOf(length));
      connection.setRequestMethod("PUT");
      Context streamCopyLatency = streamCopyTimer.time();
      OutputStream outputStream = connection.getOutputStream();
      IOUtils.copy(new ByteBufInputStream(header), outputStream);
      IOUtils.copy(input, outputStream);
      outputStream.flush();
      outputStream.close();
      streamCopyLatency.stop();
      s3RequestCounter.inc();
      int responseCode = connection.getResponseCode();
      if (responseCode != SUCCESS_CODE) {
        logger.severe(responseCode + " reason:" + connection.getResponseMessage() + "\t"
            + connection.getHeaderFields() + " index:" + count + " url:" + presignedUrl);
      }

      if (contentMD5 != null && responseCode == SUCCESS_CODE) {
        try {
          String eTagHex = connection.getHeaderFields().get(E_TAG).get(FIRST_INDEX);
          String etagToBase64 = MemqUtils.etagToBase64(eTagHex.replace("\"", ""));
          if (!contentMD5.equals(etagToBase64)) {
            logger.severe("Request failed due to etag mismatch url:" + presignedUrl);
            responseCode = ERROR_CODE;
          }
        } catch (Exception e) {
          logger.log(Level.SEVERE, "Unable to parse the returnedetag", e);
        }
      }
      connection.disconnect();
      return new UploadResult(responseCode, header.capacity(), connection.getHeaderFields(),
          internalLatency.stop());
    } finally {
      internalLatency.stop();
    }
  }

  public static class UploadResult {

    private int responseCode;
    private int memqBatchHeaderSize;
    private Map<String, List<String>> httpResponseHeaders;
    private long time;

    public UploadResult(int responseCode,
                        int memqBatchHeaderSize,
                        Map<String, List<String>> httpResponseHeaders,
                        long time) {
      this.responseCode = responseCode;
      this.memqBatchHeaderSize = memqBatchHeaderSize;
      this.httpResponseHeaders = httpResponseHeaders;
      this.time = time;
    }

    public int getResponseCode() {
      return responseCode;
    }

    public void setResponseCode(int responseCode) {
      this.responseCode = responseCode;
    }

    public int getMemqBatchHeaderSize() {
      return memqBatchHeaderSize;
    }

    public void setMemqBatchHeaderSize(int memqBatchHeaderSize) {
      this.memqBatchHeaderSize = memqBatchHeaderSize;
    }

    public Map<String, List<String>> getHttpResponseHeaders() {
      return httpResponseHeaders;
    }

    public void setHttpResponseHeaders(Map<String, List<String>> httpResponseHeaders) {
      this.httpResponseHeaders = httpResponseHeaders;
    }

    public long getTime() {
      return time;
    }

    public void setTime(long time) {
      this.time = time;
    }
  }

  private StringBuilder createKey(List<Message> messages, int attempt) {
    Message firstMessage = messages.get(0);
    StringBuilder keyBuilder = new StringBuilder();
    if (enableHashing) {
      String hash = DigestUtils.md2Hex(String.valueOf(firstMessage.getClientRequestId()));
      keyBuilder.append(hash.substring(0, 2));
      keyBuilder.append(SLASH);
    }
    keyBuilder.append(path);
    keyBuilder.append(SLASH);
    keyBuilder.append(firstMessage.getClientRequestId());
    keyBuilder.append(SEPARATOR);
    keyBuilder.append(firstMessage.getServerRequestId());
    keyBuilder.append(SEPARATOR);
    keyBuilder.append(System.currentTimeMillis());
    keyBuilder.append(SEPARATOR);
    keyBuilder.append(attempt);
    keyBuilder.append(SEPARATOR);
    keyBuilder.append(HOSTNAME);
    return keyBuilder;
  }

  public void closeWriter() {
    notificationSink.close();
  }

  public KafkaNotificationSink getNotificationSink() {
    return notificationSink;
  }

  @Override
  public String getReadUrl() {
    return notificationSink.getReadUrl();
  }

  @Override
  public Logger getLogger() {
    return logger;
  }

}
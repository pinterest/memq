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

import java.net.URISyntaxException;
import java.net.URL;
import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import javax.naming.ConfigurationException;

import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.CompositeByteBuf;
import org.apache.commons.codec.digest.DigestUtils;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.codahale.metrics.Timer.Context;
import com.google.gson.JsonObject;
import com.pinterest.memq.commons.storage.StorageHandler;
import com.pinterest.memq.commons.storage.StorageHandlerName;
import com.pinterest.memq.commons.storage.WriteFailedException;
import com.pinterest.memq.core.commons.Message;
import com.pinterest.memq.core.utils.DaemonThreadFactory;
import com.pinterest.memq.core.utils.MemqUtils;
import com.pinterest.memq.core.utils.MiscUtils;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelOption;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.util.ReferenceCounted;
import reactor.core.publisher.Mono;
import reactor.netty.http.client.HttpClient;
import reactor.netty.http.client.HttpClientResponse;
import software.amazon.awssdk.auth.credentials.InstanceProfileCredentialsProvider;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest.Builder;
import software.amazon.awssdk.services.s3.presigner.S3Presigner;
import software.amazon.awssdk.services.s3.presigner.model.PresignedPutObjectRequest;
import software.amazon.awssdk.services.s3.presigner.model.PutObjectPresignRequest;

@StorageHandlerName(name = "s3v2", previousName = "customs3aync2")
public class CustomS3Async2StorageHandler extends AbstractS3StorageHandler {

  private static final int HIGH_LATENCY_THRESHOLD = 5;
  private static final int NANOSECONDS_TO_SECONDS = 1000_000_000;
  private static final int ERROR_CODE = 500;
  private static final int SUCCESS_CODE = 200;
  private static final String SLASH = "/";
  private static final String CONTENT_LENGTH = "Content-Length";
  private static final String APPLICATION_OCTET_STREAM = "application/octet-stream";
  private static final String CONTENT_MD5 = "Content-MD5";
  private static final String CONTENT_TYPE = "Content-Type";
  private static final String E_TAG = "ETag";
  private static final String SEPARATOR = "_";
  private static final int LAST_ATTEMPT_TIMEOUT = 60_000;

  static {
    java.security.Security.setProperty("networkaddress.cache.ttl", "1");
  }

  private static final String HOSTNAME = MiscUtils.getHostname();
  private Logger logger = Logger.getLogger(CustomS3Async2StorageHandler.class.getName());
  private String path;
  private String bucket;
  private KafkaNotificationSink notificationSink;
  private String topic;
  @SuppressWarnings("unused")
  private boolean dryrun;
  private boolean disableNotifications;
  private boolean enableHashing;
  private boolean enableMD5;
  private volatile int maxAttempts;
  private volatile int retryTimeoutMillis;
  private S3Presigner signer;
  private HttpClient secureClient;
  private MetricRegistry registry;
  private ExecutorService requestExecutor;
  private ScheduledExecutorService executionTimer;

  private Timer s3PutLatencyTimer;
  private Timer s3PutInternalLatencyTimer;
  private Timer notificationPublishingTimer;
  private Counter s3RetryCounters;
  private Counter s3RequestCounter;
  private Counter notificationFailureCounter;
  private Counter timeoutExceptionCounter;

  public CustomS3Async2StorageHandler() {
  }

  @Override
  public void initWriter(Properties outputHandlerConfig,
                         String topic,
                         MetricRegistry registry) throws Exception {
    this.logger = Logger.getLogger(CustomS3Async2StorageHandler.class.getName() + "-" + topic);
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
    this.timeoutExceptionCounter = registry.counter("output.timeout.exceptions");
    this.notificationFailureCounter = registry.counter("output.notification.fail");
    this.notificationPublishingTimer = MiscUtils.oneMinuteWindowTimer(registry,
        "output.notification.publish.latency");
    this.s3PutLatencyTimer = MiscUtils.oneMinuteWindowTimer(registry, "output.s3.putobjectlatency");
    this.s3PutInternalLatencyTimer = MiscUtils.oneMinuteWindowTimer(registry,
        "output.s3.internalPutobjectlatency");
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
    this.executionTimer = Executors.newSingleThreadScheduledExecutor(new DaemonThreadFactory());
    this.s3RetryCounters = registry.counter("output.s3.retries");
    this.retryTimeoutMillis = Integer
        .parseInt(outputHandlerConfig.getProperty("retryTimeoutMillis", "5000"));
    this.maxAttempts = Integer.parseInt(outputHandlerConfig.getProperty("retryCount", "2")) + 1;
    this.secureClient = HttpClient.create().option(ChannelOption.SO_SNDBUF, 4 * 1024 * 1024)
        .option(ChannelOption.SO_LINGER, 0).secure();
    signer = S3Presigner.builder()
        .credentialsProvider(InstanceProfileCredentialsProvider.builder()
            .asyncCredentialUpdateEnabled(true).asyncThreadName("IamCredentialUpdater").build())
        .build();
  }

  @Override
  public boolean reconfigure(Properties outputHandlerConfig) {
    int newRetryTimeoutMillis = Integer
        .parseInt(outputHandlerConfig.getProperty("retryTimeoutMillis", "5000"));
    if (newRetryTimeoutMillis != retryTimeoutMillis) {
      retryTimeoutMillis = newRetryTimeoutMillis;
    }

    int newMaxAttempts = Integer.parseInt(outputHandlerConfig.getProperty("retryCount", "2")) + 1;
    if (newMaxAttempts != maxAttempts) {
      maxAttempts = newMaxAttempts;
    }
    return true;
  }

  @Override
  public void writeOutput(int objectSize,
                          int checksum,
                          final List<Message> messages) throws WriteFailedException {
    Context timer = s3PutLatencyTimer.time();
    ByteBuf batchHeader = StorageHandler.getBatchHeadersAsByteArray(messages);
    final List<ByteBuf> messageBuffers = messageToBufferList(messages);

    try {
      final int currentMaxAttempts = maxAttempts;
      final int currentRetryTimeoutMs = retryTimeoutMillis;

      int contentLength = batchHeader.writerIndex() + objectSize;
      String contentMD5 = null;
      UploadResult result = null;
      boolean hasSucceeded = false;
      int attempt = 0;
      Message firstMessage = messages.get(0);
      // map used for cancellation
      Map<String, Future<UploadResult>> futureMap = new HashMap<>();
      Map<String, CompletableFuture<UploadResult>> taskMap = new HashMap<>();
      final Publisher<ByteBuf> bodyPublisher = getBodyPublisher(messageBuffers, batchHeader);
      while (attempt < currentMaxAttempts) {
        final int timeout = attempt == currentMaxAttempts - 1 ? LAST_ATTEMPT_TIMEOUT
            : currentRetryTimeoutMs;

        final int k = attempt;
        final String key = createKey(firstMessage.getClientRequestId(),
            firstMessage.getServerRequestId(), k).toString();
        CompletableFuture<UploadResult> task = new CompletableFuture<>();
        Callable<UploadResult> uploadAttempt = () -> {
          try {
            UploadResult ur = attemptUpload(bodyPublisher, objectSize, checksum, contentLength,
                contentMD5, key, k, 0);
            task.complete(ur);
            return ur;
          } catch (Exception e) {
            task.completeExceptionally(e);
            throw e;
          }
        };
        Future<UploadResult> future = requestExecutor.submit(uploadAttempt);
        futureMap.put(key, future);
        taskMap.put(key, task);

        CompletableFuture<UploadResult> resultFuture = anyUploadResultOrTimeout(taskMap.values(),
            Duration.ofMillis(timeout));
        try {
          result = resultFuture.get();
          // start tracking response codes from s3
          registry.counter("output.s3.responseCode." + result.getResponseCode()).inc();
          if (result.getResponseCode() == SUCCESS_CODE) {
            hasSucceeded = true;
            break;
          } else {
            // remove the task so that it doesn't short circuit the next iteration
            taskMap.remove(result.getKey());
            logger.severe("Request failed reason:" + result + " attempt:" + result.getAttempt());
            if (result.getResponseCode() >= 500 && result.getResponseCode() < 600) {
              // retry 500s without increasing attempts
              s3RetryCounters.inc();
              // TODO: add circuit breaker for too many S3 failures
              continue;
            }
          }
        } catch (ExecutionException ee) {
          if (ee.getCause() instanceof TimeoutException) {
            timeoutExceptionCounter.inc();
          } else {
            logger.log(Level.SEVERE, "Request failed", ee);
          }
        } catch (Exception e) {
          logger.log(Level.SEVERE, "Request failed", e);
        }
        attempt++;
        s3RetryCounters.inc();
      }

      // best effort cancel all outstanding uploads, no matter what the result is
      for (Map.Entry<String, Future<UploadResult>> entry : futureMap.entrySet()) {
        if (result != null && entry.getKey().equals(result.getKey())) {
          continue;
        }
        entry.getValue().cancel(true);
      }

      if (result == null) {
        throw new WriteFailedException("All upload attempts failed");
      } else if (!hasSucceeded) {
        throw new WriteFailedException(
            "Upload failed due to error out: s3://" + bucket + "/" + result.getKey());
      }
      if (!disableNotifications) {
        Context publishTime = notificationPublishingTimer.time();
        JsonObject payload = buildPayload(topic, bucket, objectSize, messages.size(),
            batchHeader.capacity(), result.getKey(), result.getAttempt());
        if (contentMD5 != null) {
          payload.addProperty(CONTENT_MD5, contentMD5);
        }
        try {
          notificationSink.notify(payload, 0);
        } catch (Exception e) {
          notificationFailureCounter.inc();
          throw e;
        } finally {
          publishTime.stop();
        }
      }
      long latency = timer.stop() / NANOSECONDS_TO_SECONDS;
      if (latency > HIGH_LATENCY_THRESHOLD) {
        final String s3path = "s3://" + bucket + SLASH + result.getKey();
        logger.info("Uploaded " + s3path + " latency(" + latency + ")s, successful on attempt "
            + result.getAttempt() + ", total tasks: " + futureMap.size());
      }
    } catch (Exception e) {
      timer.stop();
      throw new WriteFailedException(e);
    } finally {
      messageBuffers.forEach(ReferenceCounted::release);
      batchHeader.release();
    }
  }

  private UploadResult attemptUpload(final Publisher<ByteBuf> bodyPublisher,
                                     int sizeInBytes,
                                     int checksum,
                                     int contentLength,
                                     String contentMD5,
                                     final String key,
                                     final int count,
                                     int timeout) throws URISyntaxException {
    Context internalLatency = s3PutInternalLatencyTimer.time();
    try {
      Builder putRequestBuilder = PutObjectRequest.builder().bucket(bucket).key(key);
      if (contentMD5 != null) {
        putRequestBuilder.contentMD5(contentMD5);
      }
      putRequestBuilder.contentLength((long) contentLength);
      PresignedPutObjectRequest presignPutObject = signer.presignPutObject(
          PutObjectPresignRequest.builder().putObjectRequest(putRequestBuilder.build())
              .signatureDuration(Duration.ofSeconds(2000)).build());

      URL url = presignPutObject.url();
      s3RequestCounter.inc();
      Mono<HttpClientResponse> responseFuture = secureClient.headers(headers -> {
        headers.set(CONTENT_TYPE, APPLICATION_OCTET_STREAM);
        if (contentMD5 != null) {
          headers.set(CONTENT_MD5, contentMD5);
        }
        headers.set(CONTENT_LENGTH, String.valueOf(contentLength));
      }).put().uri(url.toURI()).send(bodyPublisher).response();
      HttpClientResponse response = responseFuture.block();

      HttpResponseStatus status = response.status();
      int responseCode = status.code();
      HttpHeaders responseHeaders = response.responseHeaders();

      if (responseCode != SUCCESS_CODE) {
        logger.severe(responseCode + " reason:" + status.reasonPhrase() + "\t" + responseHeaders
            + " index:" + count + " url:" + url);
      }

      if (contentMD5 != null && responseCode == SUCCESS_CODE) {
        try {
          String eTagHex = responseHeaders.get(E_TAG);
          String etagToBase64 = MemqUtils.etagToBase64(eTagHex.replace("\"", ""));
          if (!contentMD5.equals(etagToBase64)) {
            logger.severe("Request failed due to etag mismatch url:" + url);
            responseCode = ERROR_CODE;
          }
        } catch (Exception e) {
          logger.log(Level.SEVERE, "Unable to parse the returnedetag", e);
        }
      }
      return new UploadResult(key, responseCode, responseHeaders, internalLatency.stop(), count);
    } finally {
      internalLatency.stop();
    }
  }

  public static class UploadResult {

    private final String key;
    private final int responseCode;
    private final HttpHeaders httpResponseHeaders;
    private final long time;
    private final int attempt;

    public UploadResult(String key,
                        int responseCode,
                        HttpHeaders responseHeaders,
                        long time,
                        int attempt) {
      this.key = key;
      this.responseCode = responseCode;
      this.httpResponseHeaders = responseHeaders;
      this.time = time;
      this.attempt = attempt;
    }

    public int getResponseCode() {
      return responseCode;
    }

    public HttpHeaders getHttpResponseHeaders() {
      return httpResponseHeaders;
    }

    public String getKey() {
      return key;
    }

    public long getTime() {
      return time;
    }

    public int getAttempt() {
      return attempt;
    }

    @Override
    public String toString() {
      return "UploadResult [key=" + key + ", responseCode=" + responseCode
          + ", httpResponseHeaders=" + httpResponseHeaders + "]";
    }
  }

  private StringBuilder createKey(long firstMessageClientRequestId,
                                  long firstMessageServerRequestId,
                                  int attempt) {
    StringBuilder keyBuilder = new StringBuilder();
    if (enableHashing) {
      String hash = DigestUtils.md2Hex(String.valueOf(firstMessageClientRequestId));
      keyBuilder.append(hash, 0, 2);
      keyBuilder.append(SLASH);
    }
    keyBuilder.append(path);
    keyBuilder.append(SLASH);
    keyBuilder.append(firstMessageClientRequestId);
    keyBuilder.append(SEPARATOR);
    keyBuilder.append(firstMessageServerRequestId);
    keyBuilder.append(SEPARATOR);
    keyBuilder.append(System.currentTimeMillis());
    keyBuilder.append(SEPARATOR);
    keyBuilder.append(attempt);
    keyBuilder.append(SEPARATOR);
    keyBuilder.append(HOSTNAME);
    return keyBuilder;
  }

  public static List<ByteBuf> messageToBufferList(List<Message> messages) {
    return messages.stream().map(m -> m.getBuf().retainedDuplicate()).collect(Collectors.toList());
  }

  public static CompositeByteBuf messageAndHeaderToCompositeBuffer(final List<ByteBuf> messageByteBufs,
                                                                   ByteBuf batchHeaders) {
    CompositeByteBuf byteBuf = ByteBufAllocator.DEFAULT.compositeBuffer();
    byteBuf.addComponent(true, batchHeaders.retainedDuplicate());
    byteBuf.addComponents(true,
        messageByteBufs.stream().map(ByteBuf::retainedDuplicate).collect(Collectors.toList()));
    return byteBuf;
  }

  public static Publisher<ByteBuf> getBodyPublisher(final List<ByteBuf> messageByteBufs,
                                                    ByteBuf batchHeaders) {
    return s -> s.onSubscribe(new Subscription() {
      @Override
      public void request(long n) {
        CompositeByteBuf byteBuf = messageAndHeaderToCompositeBuffer(messageByteBufs, batchHeaders);
        s.onNext(byteBuf);
        s.onComplete();
      }

      @Override
      public void cancel() {
      }
    });
  }

  public CompletableFuture<UploadResult> anyUploadResultOrTimeout(Collection<CompletableFuture<UploadResult>> tasks,
                                                                  Duration duration) {
    final CompletableFuture<UploadResult> promise = new CompletableFuture<>();
    executionTimer.schedule(() -> {
      final TimeoutException ex = new TimeoutException(
          "Timeout after " + duration.toMillis() + " milliseconds");
      return promise.completeExceptionally(ex);
    }, duration.toMillis(), TimeUnit.MILLISECONDS);
    CompletableFuture<UploadResult> anyUploadResultFuture = CompletableFuture
        .anyOf(tasks.toArray(new CompletableFuture[0])).thenApply(o -> (UploadResult) o);
    return anyUploadResultFuture.applyToEither(promise, Function.identity());
  }

  public void closeWriter() {
    notificationSink.close();
  }

  protected KafkaNotificationSink getNotificationSink() {
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
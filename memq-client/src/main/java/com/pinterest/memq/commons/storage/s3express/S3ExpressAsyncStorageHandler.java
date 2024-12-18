/**
 * Copyright 2024 Pinterest, Inc.
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
package com.pinterest.memq.commons.storage.s3express;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
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

import com.pinterest.memq.commons.storage.s3express.keygenerator.DateHourKeyGenerator;
import com.pinterest.memq.commons.storage.s3express.keygenerator.S3ExpressObjectKeyGenerator;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.codahale.metrics.Timer.Context;
import com.google.gson.JsonObject;
import com.pinterest.memq.commons.MemqLogMessage;
import com.pinterest.memq.commons.protocol.BatchData;
import com.pinterest.memq.commons.storage.StorageHandler;
import com.pinterest.memq.commons.storage.StorageHandlerName;
import com.pinterest.memq.commons.storage.WriteFailedException;
import com.pinterest.memq.commons.storage.s3.AbstractS3StorageHandler;
import com.pinterest.memq.commons.storage.s3.KafkaNotificationSink;
import com.pinterest.memq.core.commons.Message;
import com.pinterest.memq.core.utils.DaemonThreadFactory;
import com.pinterest.memq.core.utils.MemqUtils;
import com.pinterest.memq.core.utils.MiscUtils;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.ChannelOption;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.util.ReferenceCounted;
import reactor.core.publisher.Mono;
import reactor.netty.http.client.HttpClient;
import reactor.netty.http.client.HttpClientResponse;
import reactor.netty.resources.ConnectionProvider;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.signer.AwsS3V4Signer;
import software.amazon.awssdk.auth.signer.params.AwsS3V4SignerParams;
import software.amazon.awssdk.http.SdkHttpFullRequest;
import software.amazon.awssdk.http.SdkHttpMethod;
import software.amazon.awssdk.regions.Region;

/**
 * S3 Express storage handler for MemQ, manages both writes and reads
 */
@StorageHandlerName(name = "s3express")
public class S3ExpressAsyncStorageHandler extends AbstractS3StorageHandler {

  private static final int HIGH_LATENCY_THRESHOLD = 5;
  private static final int ERROR_CODE = 500;
  private static final int SUCCESS_CODE = 200;
  private static final String SLASH = "/";
  private static final String CONTENT_LENGTH = "Content-Length";
  private static final String CONTENT_MD5 = "Content-MD5";
  private static final String E_TAG = "ETag";
  private static final String BUCKET = "bucket";
  private static final String REGION = "region";
  private static final String DEFAULT_REGION = "us-east-1";
  private static final String DEFAULT_RETRY_TIMEOUT_MILLIS = "5000";
  private static final String DEFAULT_RETRY_COUNT = "2";
  private static final String DEFAULT_RETRY_COUNT_500S = "3";
  private static final int LAST_ATTEMPT_TIMEOUT = 60_000;
  private Logger logger = Logger.getLogger(S3ExpressAsyncStorageHandler.class.getName());
  private String path;
  private String bucket;
  private KafkaNotificationSink notificationSink;
  private String topic;
  @SuppressWarnings("unused")
  private boolean dryrun;
  private boolean disableNotifications;
  private boolean enableMD5;
  private volatile int maxAttempts;
  private volatile int maxS3Attempts;
  private volatile int retryTimeoutMillis;
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
  private String baseConnStr = null;
  private S3ExpressObjectKeyGenerator keyGenerator;

  static {
    // Set the DNS cache TTL to 1 second to avoid stale DNS entries
    java.security.Security.setProperty("networkaddress.cache.ttl", "1");
  }

  public S3ExpressAsyncStorageHandler() {
  }

  protected void initializeWriterRegistry(MetricRegistry registry) {
    this.registry = registry;
    this.s3RequestCounter = registry.counter(
        "output.s3express.requests");
    this.s3RetryCounters = registry.counter(
        "output.s3express.retries");
    this.timeoutExceptionCounter = registry.counter(
        "output.timeout.exceptions");
    this.notificationFailureCounter = registry.counter(
        "output.notification.fail");
    this.notificationPublishingTimer = MiscUtils.oneMinuteWindowTimer(registry,
        "output.notification.publish.latency");
    this.s3PutLatencyTimer = MiscUtils.oneMinuteWindowTimer(registry,
        "output.s3express.putobjectlatency");
    this.s3PutInternalLatencyTimer = MiscUtils.oneMinuteWindowTimer(registry,
        "output.s3express.internalPutobjectlatency");
  }

  protected void loadOutputHandlerConfigs(Properties outputHandlerConfig, String topic) throws Exception {
    this.dryrun = Boolean.parseBoolean(outputHandlerConfig.getProperty("dryrun", "false"));
    this.disableNotifications = Boolean
        .parseBoolean(outputHandlerConfig.getProperty("disableNotifications", "false"));
    if (!disableNotifications) {
      this.notificationSink = new KafkaNotificationSink();
      this.notificationSink.init(outputHandlerConfig);
    }
    this.region = Region.of(outputHandlerConfig.getProperty(REGION, DEFAULT_REGION).toLowerCase());
    this.bucket = outputHandlerConfig.getProperty(BUCKET);
    if (bucket == null) {
      throw new ConfigurationException("Missing S3 bucket name");
    }
    this.enableMD5 = Boolean.parseBoolean(outputHandlerConfig.getProperty("enableMD5", "false"));
    if (!enableMD5) {
      logger.warning("MD5 hashes for uploads have been disabled");
    }
    this.retryTimeoutMillis = Integer
        .parseInt(outputHandlerConfig.getProperty("retryTimeoutMillis", DEFAULT_RETRY_TIMEOUT_MILLIS));
    this.maxAttempts = Integer.parseInt(outputHandlerConfig.getProperty("retryCount", DEFAULT_RETRY_COUNT)) + 1;
    this.maxS3Attempts = Integer.parseInt(outputHandlerConfig.getProperty("retryCount500s", DEFAULT_RETRY_COUNT_500S)) + 1;
    this.path = outputHandlerConfig.getProperty("path", topic);
    this.keyGenerator = new DateHourKeyGenerator(path);
  }

  @Override
  public void initWriter(Properties outputHandlerConfig,
                         String topic,
                         MetricRegistry registry) throws Exception {
    this.topic = topic;
    initializeWriterRegistry(registry);
    loadOutputHandlerConfigs(outputHandlerConfig, topic);
    this.logger = Logger.getLogger(S3ExpressAsyncStorageHandler.class.getName() + "-" + topic);

    this.requestExecutor = Executors.newCachedThreadPool(new DaemonThreadFactory());
    this.executionTimer = Executors.newSingleThreadScheduledExecutor(new DaemonThreadFactory());
    ConnectionProvider provider = ConnectionProvider.builder("s3express")
        .maxConnections(10)
        .maxIdleTime(Duration.ofSeconds(20))
        .maxLifeTime(Duration.ofSeconds(60))
        .pendingAcquireTimeout(Duration.ofSeconds(60))
        .evictInBackground(Duration.ofSeconds(120))
        .build();
    this.secureClient = HttpClient.create(provider).option(ChannelOption.SO_SNDBUF, 4 * 1024 * 1024)
        .option(ChannelOption.SO_LINGER, 0).secure();
    logger.fine("Session Credentials: " + SessionTokenManager.getInstance().fetchCredentials(bucket));
    baseConnStr = S3ExpressHelper.generateBucketUrl(bucket);
  }
  
  @Override
  public boolean reconfigure(Properties outputHandlerConfig) {
    int newRetryTimeoutMillis = Integer.parseInt(
        outputHandlerConfig.getProperty("retryTimeoutMillis", DEFAULT_RETRY_TIMEOUT_MILLIS));
    if (newRetryTimeoutMillis != retryTimeoutMillis) {
      retryTimeoutMillis = newRetryTimeoutMillis;
    }
    int newMaxAttempts = Integer.parseInt(
        outputHandlerConfig.getProperty("retryCount", DEFAULT_RETRY_COUNT)) + 1;
    if (newMaxAttempts != maxAttempts) {
      maxAttempts = newMaxAttempts;
    }
    return true;
  }
  
  @Override
  public void initReader(Properties properties, MetricRegistry registry) throws Exception {
    super.initReader(properties, registry);
    this.bucket = properties.getProperty(BUCKET);
    if (bucket == null) {
      throw new ConfigurationException("Missing S3 bucket name");
    }
    baseConnStr = S3ExpressHelper.generateBucketUrl(bucket);
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
        final String key = getKeyGenerator().generateObjectKey(
            firstMessage.getClientRequestId(),
            firstMessage.getServerRequestId(),
            k
        );
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
          registry.counter("output.s3express.responseCode." + result.getResponseCode()).inc();
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
              if (s3RetryCounters.getCount() >= maxS3Attempts) {
                logger.severe(String.format("Retried %d times for key %s, still getting 5XX, giving up",
                        maxS3Attempts, key));
                break;
              }
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
            "Upload failed due to error out: s3express://" + bucket + "/" + result.getKey());
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
      long latencySec = TimeUnit.NANOSECONDS.toSeconds(timer.stop());
      if (latencySec > HIGH_LATENCY_THRESHOLD) {
        final String s3path = "s3express://" + bucket + SLASH + result.getKey();
        logger.info("Uploaded " + s3path + " latency(" + latencySec + ")s, successful on attempt "
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
                                     int timeout) throws URISyntaxException, InterruptedException {
    Context internalLatency = s3PutInternalLatencyTimer.time();

    SessionTokenManager instance = SessionTokenManager.getInstance();
    SessionCreds credentials = instance.getCredentials(bucket);

    AwsS3V4Signer signer = AwsS3V4Signer.create();

    SdkHttpFullRequest req = SdkHttpFullRequest.builder().method(SdkHttpMethod.PUT)
        .appendHeader("x-amz-s3session-token", credentials.token)
        .appendHeader(CONTENT_LENGTH, String.valueOf(contentLength))
        .uri(URI.create(baseConnStr + key))
        .build();
    final SdkHttpFullRequest req1 = signer.sign(req,
        AwsS3V4SignerParams.builder().awsCredentials(new AwsCredentials() {
          @Override
          public String secretAccessKey() {
            return credentials.secret;
          }
          @Override
          public String accessKeyId() {
            return credentials.key;
          }
        }).signingName("s3express").signingRegion(region).build());

    s3RequestCounter.inc();
    Mono<HttpClientResponse> responseFuture = secureClient.headers(headers -> {
      for (Entry<String, List<String>> entry : req1.headers().entrySet()) {
        headers.set(entry.getKey(), entry.getValue());
      }
    }).put().uri(req.getUri()).send(bodyPublisher).response();
    HttpClientResponse response = responseFuture.block();

    HttpResponseStatus status = response.status();
    int responseCode = status.code();
    HttpHeaders responseHeaders = response.responseHeaders();

    if (responseCode != SUCCESS_CODE) {
      logger.severe(responseCode + " reason:" + status.reasonPhrase() + "\t" + responseHeaders
          + " index:" + count + " url:" + req.getUri());
    }
    if (contentMD5 != null && responseCode == SUCCESS_CODE) {
      try {
        String eTagHex = responseHeaders.get(E_TAG);
        String etagToBase64 = MemqUtils.etagToBase64(eTagHex.replace("\"", ""));
        if (!contentMD5.equals(etagToBase64)) {
          logger.severe("Request failed due to etag mismatch url:" + req.getUri());
          responseCode = ERROR_CODE;
        }
      } catch (Exception e) {
        logger.log(Level.SEVERE, "Unable to parse the returned etag", e);
      }
    }
    return new UploadResult(key, responseCode, responseHeaders, internalLatency.stop(), count);
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

  public static List<ByteBuf> messageToBufferList(List<Message> messages) {
    return messages.stream().map(m -> m.getBuf().retainedDuplicate()).collect(Collectors.toList());
  }

  public static CompositeByteBuf messageAndHeaderToCompositeBuffer(final List<ByteBuf> messageByteBufs,
                                                                   ByteBuf batchHeaders) {
    CompositeByteBuf byteBuf = PooledByteBufAllocator.DEFAULT.compositeBuffer();
    byteBuf.addComponent(true, batchHeaders.retainedDuplicate());
    byteBuf.addComponents(true,
            messageByteBufs.stream().map(ByteBuf::retainedDuplicate).collect(Collectors.toList()));
    return byteBuf;
  }

  public static Publisher<ByteBuf> getBodyPublisher(
          final List<ByteBuf> messageByteBufs, ByteBuf batchHeaders) {
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

  private SdkHttpFullRequest generateGetObjectRequest(SessionCreds creds, String objectKey) {
    return SdkHttpFullRequest.builder().method(SdkHttpMethod.GET)
            .appendHeader("x-amz-s3session-token", creds.token)
            .uri(URI.create(baseConnStr + objectKey))
            .build();
  }

  private SdkHttpFullRequest signRequest(SessionCreds creds, SdkHttpFullRequest request) {
    AwsS3V4Signer signer = AwsS3V4Signer.create();
    return signer.sign(request,
            AwsS3V4SignerParams.builder().awsCredentials(new AwsCredentials() {
              @Override
              public String secretAccessKey() {
                return creds.secret;
              }
              @Override
              public String accessKeyId() {
                return creds.key;
              }
            }).signingName("s3express").signingRegion(region).build());
  }

  private SessionCreds getCredentials(String bucket) throws IOException {
    SessionTokenManager instance = SessionTokenManager.getInstance();
    SessionCreds credentials;
    try {
      credentials = instance.getCredentials(bucket);
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
    return credentials;
  }

  public void setKeyGenerator(S3ExpressObjectKeyGenerator keyGenerator) {
    this.keyGenerator = keyGenerator;
  }

  public S3ExpressObjectKeyGenerator getKeyGenerator() {
    return keyGenerator;
  }

  private SdkHttpFullRequest generateFetchRequest(JsonObject nextNotificationToProcess) throws IOException {
    String currentBucket = nextNotificationToProcess.get(BUCKET).getAsString();
    String currentKey = nextNotificationToProcess.get(KEY).getAsString();
    int currentObjectSize = nextNotificationToProcess.get(SIZE).getAsInt();
    logger.fine("Updating bucket and key: " + currentBucket + "/" + currentKey + " {"
            + nextNotificationToProcess.get(MemqLogMessage.INTERNAL_FIELD_NOTIFICATION_PARTITION_ID)
            .getAsNumber()
            + ", " + nextNotificationToProcess
            .get(MemqLogMessage.INTERNAL_FIELD_NOTIFICATION_PARTITION_OFFSET).getAsNumber()
            + "}");
    logger.finest("Object size: " + currentObjectSize);

    SessionCreds credentials = getCredentials(currentBucket);
    SdkHttpFullRequest request = generateGetObjectRequest(credentials, currentKey);
    SdkHttpFullRequest signedRequest = signRequest(credentials, request);
    return signedRequest;
  }

  @Override
  public InputStream fetchBatchStreamForNotification(JsonObject nextNotificationToProcess) throws IOException {
    final SdkHttpFullRequest request = generateFetchRequest(nextNotificationToProcess);
    long fetchStartTime = System.currentTimeMillis();
    try {
      return httpClient.tryObjectGet(request);
    } finally {
      long fetchTime = System.currentTimeMillis() - fetchStartTime;
      getLogger().fine("Fetch Time:" + fetchTime);
    }
  }
  
  @Override
  public BatchData fetchBatchStreamForNotificationBuf(JsonObject nextNotificationToProcess) throws IOException {
    int currentObjectSize = nextNotificationToProcess.get(SIZE).getAsInt();
    final SdkHttpFullRequest request = generateFetchRequest(nextNotificationToProcess);
    return new BatchData(currentObjectSize, httpClient.tryObjectGetAsBuffer(request));
  }
}

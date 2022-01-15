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
package com.pinterest.memq.core.output.s3;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.codec.digest.DigestUtils;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.DnsResolver;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectResult;
import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.codahale.metrics.Timer.Context;
import com.pinterest.memq.core.commons.Message;
import com.pinterest.memq.core.commons.MessageBufferInputStream;
import com.pinterest.memq.core.output.OutputFailedException;
import com.pinterest.memq.core.output.OutputHandler;
import com.pinterest.memq.core.output.OutputPlugin;
import com.pinterest.memq.core.utils.MiscUtils;

@OutputPlugin(alias = "s3async2")
public class S3Async2OutputHandler implements OutputHandler {

  static {
    java.security.Security.setProperty("networkaddress.cache.ttl", "1");
  }

  public static class InitialS3Client implements Supplier<AmazonS3> {

    private static final Logger logger = Logger.getLogger(InitialS3Client.class.getCanonicalName());
    private final Properties outputHandlerConfig;

    public InitialS3Client(Properties outputHandlerConfig) {
      this.outputHandlerConfig = outputHandlerConfig;
    }

    @Override
    public AmazonS3 get() {
      ClientConfiguration config = new ClientConfiguration();
      config.setConnectionTTL(1);
      config.setSocketBufferSizeHints(1024 * 1024 * 8, 1024 * 1024);
      config.setMaxConnections(1);
      config.setRequestTimeout(20_000);
      config.setDnsResolver(new MemqS3DNSResolver());
      return AmazonS3ClientBuilder.standard().withClientConfiguration(config)
          .withRegion(outputHandlerConfig.getProperty("region")).build();
    }
  }

  private static final Logger logger = Logger.getLogger(S3Async2OutputHandler.class.getName());
  private static final String HOSTNAME = MiscUtils.getHostname();
  private String path;
  private String bucket;
  private Counter streamResetCounter;
  private NotificationSink notificationSink;
  private String topic;
  private boolean dryrun;
  private boolean disableNotifications;
  private Timer s3PutLatencyTimer;
  private boolean enableHashing;
  private ExecutorService requestExecutor;
  private int maxAttempts;
  private int retryTimeoutMillis;
  private Counter s3RetryCounters;
  private ThreadLocal<AmazonS3> threadLocalS3;
  private Timer s3AttemptLatencyTimer;

  public S3Async2OutputHandler() {
  }

  @Override
  public void init(Properties outputHandlerConfig,
                   String topic,
                   MetricRegistry registry) throws Exception {
    this.topic = topic;
    this.dryrun = Boolean.parseBoolean(outputHandlerConfig.getProperty("dryrun", "false"));
    this.disableNotifications = Boolean
        .parseBoolean(outputHandlerConfig.getProperty("disableNotifications", "true"));
    if (!disableNotifications) {
      this.notificationSink = NotificationSink.getInstance().init(outputHandlerConfig);
    }
    this.streamResetCounter = registry.counter("output.s3.streamReset");
    this.s3PutLatencyTimer = registry.timer("output.s3.putobjectlatency");
    this.s3AttemptLatencyTimer = registry.timer("output.s3.attemptputobjectlatency");
    this.bucket = outputHandlerConfig.getProperty("bucket", "test");
    this.enableHashing = Boolean
        .parseBoolean(outputHandlerConfig.getProperty("enableHashing", "true"));
    this.path = outputHandlerConfig.getProperty("path", topic);
    this.requestExecutor = Executors.newCachedThreadPool();
    this.s3RetryCounters = registry.counter("output.s3.retries");
    this.retryTimeoutMillis = Integer
        .parseInt(outputHandlerConfig.getProperty("retryTimeoutMillis", "5000"));
    this.maxAttempts = Integer.parseInt(outputHandlerConfig.getProperty("retryCount", "2")) + 1;
    if (!dryrun) {
      threadLocalS3 = ThreadLocal.withInitial(new InitialS3Client(outputHandlerConfig));
    }
  }

  @Override
  public void writeOutput(int sizeInBytes,
                          final List<Message> messages) throws OutputFailedException {
    Context timer = s3PutLatencyTimer.time();
    try {
      final String key = createKey(messages).toString();
      if (!dryrun) {
        long ts = System.currentTimeMillis();
        boolean hasSucceeded = false;
        int i = 0;
        Exception lastException = null;
        while (!hasSucceeded && i < maxAttempts) {
          Context attemptTimer = s3AttemptLatencyTimer.time();
          Future<PutObjectResult> response = requestExecutor.submit(() -> {
            MessageBufferInputStream input = new MessageBufferInputStream(messages,
                streamResetCounter);
            ObjectMetadata metadata = new ObjectMetadata();
            metadata.setContentLength(sizeInBytes);
            AmazonS3 s3 = threadLocalS3.get();
            PutObjectResult putObject = s3.putObject(bucket, key, input, metadata);
            return putObject;
          });
          try {
            response.get(retryTimeoutMillis + i * 2000, TimeUnit.MILLISECONDS);
            hasSucceeded = true;
            break;
          } catch (Exception e) {
            lastException = e;
            response.cancel(true);
            threadLocalS3.get().shutdown();
            threadLocalS3.remove();
            s3RetryCounters.inc();
          } finally {
            attemptTimer.stop();
          }
          i++;
        }
        if (!hasSucceeded) {
          ts = System.currentTimeMillis() - ts;
          throw new OutputFailedException(
              "Upload timed out after:" + (ts / 1000) + "s reason:" + lastException != null
                  ? lastException.getMessage()
                  : "null");
        }
      }
      if (!disableNotifications) {
        try {
          notificationSink.notifyCompactionTopic(topic, bucket, key, sizeInBytes);
        } catch (InterruptedException | ExecutionException e) {
          logger.log(Level.SEVERE, "Failed to notify compaction for topic:" + topic + " bucket:"
              + bucket + " key:" + key, e);
        }
      }
      long latency = timer.stop() / 1000_000_000;
      if (latency > 5) {
        logger.info("Uploaded s3://" + bucket + "/" + key + " latency(" + latency + ")s");
      }
    } catch (Exception e) {
      timer.stop();
      throw new OutputFailedException(e);
    } finally {
    }
  }

  private StringBuilder createKey(List<Message> messages) {
    Message firstMessage = messages.get(0);
    StringBuilder keyBuilder = new StringBuilder();
    if (enableHashing) {
      String hash = DigestUtils.md2Hex(String.valueOf(firstMessage.getClientRequestId()));
      keyBuilder.append(hash.substring(0, 2));
      keyBuilder.append("/");
    }
    keyBuilder.append(path);
    keyBuilder.append("/");
    keyBuilder.append(firstMessage.getClientRequestId());
    keyBuilder.append("_");
    keyBuilder.append(firstMessage.getServerRequestId());
    keyBuilder.append("_");
    keyBuilder.append(HOSTNAME);
    return keyBuilder;
  }

  public void close() {
  }

}
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

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.codec.digest.DigestUtils;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.codahale.metrics.Timer.Context;
import com.pinterest.memq.core.commons.Message;
import com.pinterest.memq.core.output.OutputFailedException;
import com.pinterest.memq.core.output.OutputHandler;
import com.pinterest.memq.core.output.OutputPlugin;
import com.pinterest.memq.core.utils.MiscUtils;

import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.metrics.MetricCollection;
import software.amazon.awssdk.metrics.MetricPublisher;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;

@OutputPlugin(alias = "s3async")
public class S3AsyncOutputHandler implements OutputHandler {

  protected final class MessageBatchPublisher implements Publisher<ByteBuffer> {
    private final Iterator<Message> iterator;

    protected MessageBatchPublisher(Iterator<Message> iterator) {
      this.iterator = iterator;
    }

    @Override
    public void subscribe(Subscriber<? super ByteBuffer> s) {
      s.onSubscribe(new Subscription() {

        @Override
        public void request(long n) {
          int i = 0;
          while (iterator.hasNext() && i < n) {
            ByteBuffer buf = iterator.next().getBuf();
            buf.position(0);
            s.onNext(buf);
            i++;
          }
          if (!iterator.hasNext()) {
            s.onComplete();
          }
        }

        @Override
        public void cancel() {
        }
      });
    }
  }

  private static final Logger logger = Logger.getLogger(S3AsyncOutputHandler.class.getName());
  private static final String HOSTNAME = MiscUtils.getHostname();
  private String path;
  private String bucket;
  private NotificationSink notificationSink;
  private String topic;
  private boolean dryrun;
  private boolean disableNotifications;
  private Timer s3PutLatencyTimer;
  private boolean enableHashing;
  private int retryTimeoutMillis;
  private int maxAttempts;
  private Counter s3RetryCounters;
  private static ThreadLocal<S3AsyncClient> s3ThreadLocal = ThreadLocal
      .withInitial(new Supplier<S3AsyncClient>() {

        @Override
        public S3AsyncClient get() {
          MetricPublisher metricPublisher = new MetricPublisher() {

            @Override
            public void publish(MetricCollection metricCollection) {
              logger.info(metricCollection.toString());
            }

            @Override
            public void close() {
            }
          };
          return S3AsyncClient.builder()
              .overrideConfiguration(o -> o.addMetricPublisher(metricPublisher))
              .region(Region.US_EAST_1).build();
        }
      });

  public S3AsyncOutputHandler() {
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
    this.retryTimeoutMillis = Integer
        .parseInt(outputHandlerConfig.getProperty("retryTimeoutMillis", "5000"));
    this.maxAttempts = Integer.parseInt(outputHandlerConfig.getProperty("retryCount", "1")) + 1;

    this.s3RetryCounters = registry.counter("output.s3.retries");
    this.s3PutLatencyTimer = registry.timer("output.s3.putobjectlatency");
    this.bucket = outputHandlerConfig.getProperty("bucket", "test");
    this.enableHashing = Boolean
        .parseBoolean(outputHandlerConfig.getProperty("enableHashing", "true"));
    this.path = outputHandlerConfig.getProperty("path", topic);
  }

  @Override
  public void writeOutput(int sizeInBytes,
                          final List<Message> messages) throws OutputFailedException {
    Context timer = s3PutLatencyTimer.time();
    String key = createKey(messages).toString();
    S3AsyncClient s3 = null;
    try {
      if (!dryrun) {
        s3 = s3ThreadLocal.get();
        boolean hasSucceeded = false;
        int i = 0;
        while (!hasSucceeded && i < maxAttempts - 1) {
          CompletableFuture<PutObjectResponse> response = makePutRequest(sizeInBytes, messages, key,
              s3);
          try {
            response.get(retryTimeoutMillis, TimeUnit.MILLISECONDS);
            hasSucceeded = true;
            break;
          } catch (TimeoutException e) {
            s3RetryCounters.inc();
          }
          i++;
        }
        if (!hasSucceeded) {
          CompletableFuture<PutObjectResponse> response = makePutRequest(sizeInBytes, messages, key,
              s3);
          // if upload still hasn't succeeded then run a blocking call
          response.get(retryTimeoutMillis, TimeUnit.MILLISECONDS);
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
    } catch (Exception e) {
      try {
        s3.close();
      } catch (Exception e1) {
        logger.log(Level.SEVERE, "Failed to close s3 client", e1);
      }
      s3ThreadLocal.remove();
      throw new OutputFailedException(e);
    } finally {
      long latency = timer.stop();
      logger.fine(() -> "Uploaded s3://" + bucket + "/" + key + " latency:" + latency / 1000000);
    }
  }

  private CompletableFuture<PutObjectResponse> makePutRequest(int sizeInBytes,
                                                              final List<Message> messages,
                                                              String key,
                                                              S3AsyncClient s3) {
    final Iterator<Message> iterator = messages.iterator();
    CompletableFuture<PutObjectResponse> response = s3.putObject(PutObjectRequest.builder()
        .bucket(bucket).key(key).contentLength((long) sizeInBytes).build(),
        AsyncRequestBody.fromPublisher(new MessageBatchPublisher(iterator)));
    return response;
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
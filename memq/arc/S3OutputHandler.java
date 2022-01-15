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

import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.codec.digest.DigestUtils;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ObjectMetadata;
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

@OutputPlugin(alias = "s3")
public class S3OutputHandler implements OutputHandler {

//  protected final class MessageBatchPublisher implements Publisher<ByteBuffer> {
//    private final Iterator<Message> iterator;
//
//    protected MessageBatchPublisher(Iterator<Message> iterator) {
//      this.iterator = iterator;
//    }
//
//    @Override
//    public void subscribe(Subscriber<? super ByteBuffer> s) {
//      s.onSubscribe(new Subscription() {
//
//        @Override
//        public void request(long n) {
//          int i = 0;
//          while (iterator.hasNext() && i < n) {
//            ByteBuffer buf = iterator.next().getBuf();
//            buf.position(0);
//            s.onNext(buf);
//            i++;
//          }
//          if (!iterator.hasNext()) {
//            s.onComplete();
//          }
//        }
//
//        @Override
//        public void cancel() {
//        }
//      });
//    }
//  }

  private static final Logger logger = Logger.getLogger(S3OutputHandler.class.getName());
  private static final String HOSTNAME = MiscUtils.getHostname();
  private String path;
  private String bucket;
//  private S3AsyncClient s3;
  private Counter streamResetCounter;
  private NotificationSink notificationSink;
  private String topic;
  private boolean dryrun;
  private boolean disableNotifications;
  private Timer s3PutLatencyTimer;
  private boolean enableHashing;
  private AmazonS3 s3;

  public S3OutputHandler() {
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
    this.bucket = outputHandlerConfig.getProperty("bucket", "test");
    this.enableHashing = Boolean
        .parseBoolean(outputHandlerConfig.getProperty("enableHashing", "true"));
    this.path = outputHandlerConfig.getProperty("path", topic);
    if (!dryrun) {
      this.s3 = AmazonS3ClientBuilder.standard()
          .withRegion(outputHandlerConfig.getProperty("region")).build();
    }
  }

  @Override
  public void writeOutput(int sizeInBytes,
                          final List<Message> messages) throws OutputFailedException {
    MessageBufferInputStream input = new MessageBufferInputStream(messages, streamResetCounter);
    try {
      String key = createKey(messages).toString();
      if (!dryrun) {
        ObjectMetadata metadata = new ObjectMetadata();
        metadata.setContentLength(sizeInBytes);
        Context timer = s3PutLatencyTimer.time();
        s3.putObject(bucket, key, input, metadata);
        long latency = timer.stop();
        logger.fine(() -> "Uploaded s3://" + bucket + "/" + key + " latency:" + latency / 1000000);
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
    s3.shutdown();
  }

}
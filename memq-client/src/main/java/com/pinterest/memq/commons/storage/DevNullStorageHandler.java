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
package com.pinterest.memq.commons.storage;

import com.pinterest.memq.commons.storage.s3.KafkaNotificationSink;
import com.pinterest.memq.core.commons.Message;
import com.pinterest.memq.core.utils.MiscUtils;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.google.gson.JsonObject;
import io.netty.buffer.ByteBuf;

import java.io.File;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.Properties;

@StorageHandlerName(name = "devnull")
public class DevNullStorageHandler implements StorageHandler {

  private KafkaNotificationSink notificationSink;
  private boolean disableNotifications;
  private Timer notificationPublishingTimer;

  @Override
  public void initWriter(Properties outputHandlerConfig, String topic, MetricRegistry registry)
      throws Exception {
    this.disableNotifications = Boolean
        .parseBoolean(outputHandlerConfig.getProperty("disableNotifications", "true"));
    if (!disableNotifications) {
      this.notificationSink = new KafkaNotificationSink();
      this.notificationSink.init(outputHandlerConfig);
      this.notificationPublishingTimer = MiscUtils.oneMinuteWindowTimer(registry,"output.notification.publish.latency");
    }
  }

  @Override
  public void writeOutput(int sizeInBytes, int checksum, List<Message> messages)
      throws WriteFailedException {
    File fileToWrite = new File("/dev/null");

    ByteBuf batchHeader = StorageHandler.getBatchHeadersAsByteArray(messages);

    try (FileChannel fc = FileChannel.open(fileToWrite.toPath(), StandardOpenOption.WRITE)) {
      batchHeader.readBytes(fc, batchHeader.readableBytes());
      for (Message m : messages) {
        ByteBuf buf = m.getBuf();
        buf.readBytes(fc, buf.readableBytes());
      }

      if (!disableNotifications) {
        JsonObject payload = new JsonObject();
        payload.addProperty("type", "devnull");
        Timer.Context publishTime = notificationPublishingTimer.time();
        notificationSink.notify(payload, 0);
        publishTime.stop();
      }
    } catch (Exception e) {
      throw new WriteFailedException(e);
    } finally {
      batchHeader.release();
    }
  }

  @Override
  public String getReadUrl() {
    return "devnull";
  }
}

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
package com.pinterest.memq.core.processing.bucketing;

import java.io.IOException;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.codahale.metrics.Timer.Context;
import com.pinterest.memq.commons.protocol.RequestType;
import com.pinterest.memq.commons.protocol.ResponseCodes;
import com.pinterest.memq.commons.protocol.ResponsePacket;
import com.pinterest.memq.commons.protocol.WriteResponsePacket;
import com.pinterest.memq.commons.storage.WriteFailedException;
import com.pinterest.memq.commons.storage.StorageHandler;
import com.pinterest.memq.core.commons.Message;
import com.pinterest.memq.core.utils.CoreUtils;
import com.pinterest.memq.core.utils.MiscUtils;

public class BatchOutputTask implements Runnable {

  private static final Logger logger = Logger.getLogger(BatchOutputTask.class.getName());
  private Batch batch;
  private StorageHandler handler;

  // metrics
  private Counter uploadBatchCounter;
  private Counter uploadBytesCounter;
  private Timer uploadLatency;
  private Counter outputErrorCounter;
  private Counter uploadMessageCounter;
  private Counter ackChannelWriteError;
  private Timer ackLatency;
  private Counter activeParallelTasks;
  private Histogram batchSizeBytes;

  public BatchOutputTask(String topic,
                         MetricRegistry registry,
                         Batch batch,
                         StorageHandler handler) {
    this.batch = batch;
    this.handler = handler;
    initializeMetric(topic, registry);
  }

  private void initializeMetric(String topic, MetricRegistry registry) {
    this.uploadBatchCounter = registry.counter("output.batchCount");
    this.activeParallelTasks = registry.counter("output.activeParallelTasks");
    this.uploadMessageCounter = registry.counter("output.messageCount");
    this.uploadBytesCounter = registry.counter("output.uploadBytes");
    this.outputErrorCounter = registry.counter("output.error");
    this.ackChannelWriteError = registry.counter("acker.ackerror");
    this.ackLatency = MiscUtils.oneMinuteWindowTimer(registry,"acker.push.latency");
    this.uploadLatency = MiscUtils.oneMinuteWindowTimer(registry,"output.uploadLatency");
    this.batchSizeBytes = registry.histogram("output.batchSizeBytes");
  }

  @Override
  public void run() {
    List<Message> messageBatchAsList = batch.getMessageBatchAsList();
    if (messageBatchAsList.isEmpty()) {
      return;
    }
    activeParallelTasks.inc();
    int sizeInBytes = CoreUtils.batchSizeInBytes(messageBatchAsList);
    try {
      int checksum = CoreUtils.batchChecksum(messageBatchAsList);
      Context uploadTimer = uploadLatency.time();
      handler.writeOutput(sizeInBytes, checksum, messageBatchAsList);
      uploadTimer.stop();
      for (Message message : messageBatchAsList) {
        // notify
        if (message.getPipelineReference() != null) {
          try {
            message.getPipelineReference()
                .writeAndFlush(new ResponsePacket(message.getClientProtocolVersion(),
                    message.getClientRequestId(), RequestType.WRITE, ResponseCodes.OK,
                    new WriteResponsePacket()));
          } catch (Exception e) {
            ackChannelWriteError.inc();
          }
        }
      }
      uploadBytesCounter.inc(sizeInBytes);
      uploadMessageCounter.inc(messageBatchAsList.size());
      uploadBatchCounter.inc();
      batchSizeBytes.update(sizeInBytes);
    } catch (WriteFailedException | IOException e) {
      logger.log(Level.SEVERE, "Failed to write output", e);
      Context ackTimer = ackLatency.time();
      for (Message message : messageBatchAsList) {
        // notify failure
        if (message.getPipelineReference() != null) {
          try {
            message.getPipelineReference()
                .writeAndFlush(new ResponsePacket(message.getClientProtocolVersion(),
                    message.getClientRequestId(), RequestType.WRITE, ResponseCodes.REQUEST_FAILED,
                    new WriteResponsePacket()));
          } catch (Exception e2) {
            ackChannelWriteError.inc();
          }
        }
      }
      ackTimer.stop();
      outputErrorCounter.inc();
    } finally {
      // release batch
      activeParallelTasks.dec();
      batch.reset();
    }
  }

}

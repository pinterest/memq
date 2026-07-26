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
package com.pinterest.memq.commons.protocol;

import java.util.Objects;
import java.util.Properties;

import com.google.gson.annotations.SerializedName;

public class TopicConfig implements Comparable<TopicConfig> {

  private long topicOrder;

  /**
   * Slot size
   */
  private int bufferSize;

  private int ringBufferSize;

  private int batchMilliSeconds = 60_000;

  @Deprecated
  private int batchSizeMB = -1;

  private long batchSizeBytes = 100 * 1024 * 1024;

  private int outputParallelism = 2;

  private int maxDispatchCount = 100;

  /**
   * Backpressure cap on the number of concurrently in-flight batches (batches
   * that have been opened and not yet fully uploaded/released) on the broker.
   * This is the broker-side analog of the producer's {@code maxInflightRequests}
   * and, together with {@link #maxInflightBatchBytes}, bounds the direct memory
   * the ingest-to-upload pipeline can hold so downstream (S3/notification)
   * slowness cannot exhaust direct memory.
   * <p>
   * A value {@code <= 0} means "auto": {@code max(4 * outputParallelism, 8)},
   * which leaves headroom above the {@code outputParallelism} concurrent uploads
   * plus the accumulating batch so normal traffic is never throttled.
   * <p>
   * Applied at BatchManager construction; changing it requires a restart.
   */
  private int maxInflightBatches = 0;

  /**
   * Backpressure cap on the total direct memory (in bytes) reserved by
   * in-flight batches on the broker. Broker-side analog of the producer's
   * {@code maxInflightRequestsMemoryBytes}. Each opened batch reserves up to
   * {@code batchSizeBytes} until it is uploaded and released.
   * <p>
   * A value {@code <= 0} means "auto": {@code maxInflightBatches * batchSizeBytes}.
   * <p>
   * Applied at BatchManager construction; changing it requires a restart.
   */
  private long maxInflightBatchBytes = 0;

  /**
   * Maximum time (ms) a write will wait to acquire an in-flight batch permit
   * before the batch backpressure caps reject it with SERVICE_UNAVAILABLE.
   * Broker-side analog of the producer's {@code maxBlockMs}. Because writes are
   * processed on the Netty event loop, this defaults to {@code 0} (non-blocking,
   * reject immediately) to avoid stalling other connections on the same loop.
   */
  private int maxBlockMs = 0;

  private String topic;

  private int tickFrequencyMillis = 1000;

  private boolean enableBucketing2Processor = true;

  private boolean enableServerHeaderValidation = false;

  /**
   * Per-topic opt-in for the eviction-based balancing mechanism. Only takes
   * effect when the broker-wide {@code EvictionConfig.enabled} is also true;
   * i.e. balancing is active for this topic iff
   * {@code EvictionConfig.enabled && enableBalancing}. Disabled by default.
   */
  private boolean enableBalancing = false;

  private int clusteringMultiplier = 3;

  @SerializedName(value = "storageHandlerName", alternate = "outputHandler")
  private String storageHandlerName;
  
  @SerializedName(value = "storageHandlerConfig", alternate = "outputHandlerConfig")
  private Properties storageHandlerConfig = new Properties();

  private volatile double inputTrafficMB = 0.0;

  public TopicConfig() {
  }

  public TopicConfig(TopicConfig config) {
    this.topicOrder = config.topicOrder;
    this.topic = config.topic;
    this.storageHandlerName = config.storageHandlerName;
    this.bufferSize = config.bufferSize;
    this.outputParallelism = config.outputParallelism;
    this.maxDispatchCount = config.maxDispatchCount;
    this.maxInflightBatches = config.maxInflightBatches;
    this.maxInflightBatchBytes = config.maxInflightBatchBytes;
    this.maxBlockMs = config.maxBlockMs;
    this.tickFrequencyMillis = config.tickFrequencyMillis;
    this.batchMilliSeconds = config.batchMilliSeconds;
    this.storageHandlerConfig = config.storageHandlerConfig;
    this.ringBufferSize = config.ringBufferSize;
    this.batchSizeMB = config.batchSizeMB;
    this.batchSizeBytes = config.batchSizeBytes;
    this.enableBucketing2Processor = config.enableBucketing2Processor;
    this.enableServerHeaderValidation = config.enableServerHeaderValidation;
    this.enableBalancing = config.enableBalancing;
    this.clusteringMultiplier = config.clusteringMultiplier;
  }

  public TopicConfig(int topicOrder,
                     int bufferSize,
                     int ringBufferSize,
                     String topic,
                     int batchSizeMB,
                     double inputTrafficMB,
                     int clusteringMultiplier) {
    super();
    this.topicOrder = topicOrder;
    this.bufferSize = bufferSize;
    this.ringBufferSize = ringBufferSize;
    this.topic = topic;
    this.inputTrafficMB = inputTrafficMB;
    this.batchSizeMB = batchSizeMB;
    this.clusteringMultiplier = clusteringMultiplier;
  }

  public TopicConfig(String topic, String storageHandler) {
    this.topic = topic;
    this.storageHandlerName = storageHandler;
  }

  @Override
  public int compareTo(TopicConfig o) {
    return Long.compare(topicOrder, o.getTopicOrder());
  }

  public long getTopicOrder() {
    return topicOrder;
  }

  public void setTopicOrder(long topicOrder) {
    this.topicOrder = topicOrder;
  }

  /**
   * @return the bufferSize
   */
  public int getBufferSize() {
    return bufferSize;
  }

  /**
   * @param bufferSize the bufferSize to set
   */
  public void setBufferSize(int bufferSize) {
    this.bufferSize = bufferSize;
  }

  /**
   * @return the ringBufferSize
   */
  public int getRingBufferSize() {
    return ringBufferSize;
  }

  /**
   * @param ringBufferSize the ringBufferSize to set
   */
  public void setRingBufferSize(int ringBufferSize) {
    this.ringBufferSize = ringBufferSize;
  }

  /**
   * @return the batchMilliSeconds
   */
  public int getBatchMilliSeconds() {
    return batchMilliSeconds;
  }

  /**
   * @param batchMilliSeconds the batchMilliSeconds to set
   */
  public void setBatchMilliSeconds(int batchMilliSeconds) {
    this.batchMilliSeconds = batchMilliSeconds;
  }

  /**
   * @param batchSizeMB the batchSizeMB to set
   */
  public void setBatchSizeMB(int batchSizeMB) {
    this.batchSizeBytes = batchSizeMB * 1024 * 1024;
  }

  public void setBatchSizeKB(int batchSizeKB) {
    this.batchSizeBytes = batchSizeKB * 1024;
  }

  public void setBatchSizeBytes(long batchSizeBytes) {
    this.batchSizeBytes = batchSizeBytes;
  }

  public long getBatchSizeBytes() {
    if (batchSizeMB != -1) return batchSizeMB * 1024 * 1024;
    return batchSizeBytes;
  }


  public int getMaxDispatchCount() {
    return maxDispatchCount;
  }

  public void setMaxDispatchCount(int maxDispatchCount) {
    this.maxDispatchCount = maxDispatchCount;
  }

  /**
   * @return the configured in-flight batch count cap, or the self-scaling
   *         default {@code max(4 * outputParallelism, 8)} when unset ({@code <= 0}).
   */
  public int getMaxInflightBatches() {
    if (maxInflightBatches > 0) {
      return maxInflightBatches;
    }
    return Math.max(4 * outputParallelism, 8);
  }

  public void setMaxInflightBatches(int maxInflightBatches) {
    this.maxInflightBatches = maxInflightBatches;
  }

  /**
   * @return the configured in-flight batch memory cap in bytes, or the
   *         self-scaling default {@code getMaxInflightBatches() * batchSizeBytes}
   *         when unset ({@code <= 0}).
   */
  public long getMaxInflightBatchBytes() {
    if (maxInflightBatchBytes > 0) {
      return maxInflightBatchBytes;
    }
    return (long) getMaxInflightBatches() * getBatchSizeBytes();
  }

  public void setMaxInflightBatchBytes(long maxInflightBatchBytes) {
    this.maxInflightBatchBytes = maxInflightBatchBytes;
  }

  public int getMaxBlockMs() {
    return maxBlockMs;
  }

  public void setMaxBlockMs(int maxBlockMs) {
    this.maxBlockMs = maxBlockMs;
  }

  /**
   * @return the topic
   */
  public String getTopic() {
    return topic;
  }

  /**
   * @param topic the topic to set
   */
  public void setTopic(String topic) {
    this.topic = topic;
  }

  public Properties getStorageHandlerConfig() {
    return storageHandlerConfig;
  }

  public void setStorageHandlerConfig(Properties storageHandlerConfig) {
    this.storageHandlerConfig = storageHandlerConfig;
  }

  public String getStorageHandlerName() {
    return storageHandlerName;
  }

  public void setStorageHandlerName(String storageHandlerName) {
    this.storageHandlerName = storageHandlerName;
  }

  /**
   * @return the outputParallelism
   */
  public int getOutputParallelism() {
    return outputParallelism;
  }

  /**
   * @param outputParallelism the outputParallelism to set
   */
  public void setOutputParallelism(int outputParallelism) {
    this.outputParallelism = outputParallelism;
  }

  /**
   * @return the inputTrafficMB
   */
  public double getInputTrafficMB() {
    return inputTrafficMB;
  }

  /**
   * @param inputTrafficMB the inputTrafficMB to set
   */
  public void setInputTrafficMB(double inputTrafficMB) {
    this.inputTrafficMB = inputTrafficMB;
  }

  public int getTickFrequencyMillis() {
    return tickFrequencyMillis;
  }

  public void setTickFrequencyMillis(int tickFrequencyMillis) {
    this.tickFrequencyMillis = tickFrequencyMillis;
  }

  public boolean isEnableBucketing2Processor() {
    return enableBucketing2Processor;
  }

  public void setEnableBucketing2Processor(boolean enableBucketing2Processor) {
    this.enableBucketing2Processor = enableBucketing2Processor;
  }

  public boolean isEnableServerHeaderValidation() {
    return enableServerHeaderValidation;
  }

  public void setEnableServerHeaderValidation(boolean enableServerHeaderValidation) {
    this.enableServerHeaderValidation = enableServerHeaderValidation;
  }

  public boolean isEnableBalancing() {
    return enableBalancing;
  }

  public void setEnableBalancing(boolean enableBalancing) {
    this.enableBalancing = enableBalancing;
  }

  public int getClusteringMultiplier() {
    return clusteringMultiplier;
  }

  public void setClusteringMultiplier(int clusteringMultiplier) {
    this.clusteringMultiplier = clusteringMultiplier;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof TopicConfig) {
      return ((TopicConfig) obj).getTopic().equals(topic);
    }
    return false;
  }

  public boolean isDifferentFrom(Object o) {
    if (this == o) {
      return false;
    }
    if (o == null || getClass() != o.getClass()) {
      return true;
    }

    TopicConfig that = (TopicConfig) o;

    if (bufferSize != that.bufferSize) {
      return true;
    }
    if (ringBufferSize != that.ringBufferSize) {
      return true;
    }
    if (batchMilliSeconds != that.batchMilliSeconds) {
      return true;
    }
    if (batchSizeMB != that.batchSizeMB) {
      return true;
    }
    if (batchSizeBytes != that.batchSizeBytes) {
      return true;
    }
    if (outputParallelism != that.outputParallelism) {
      return true;
    }
    if (maxDispatchCount != that.maxDispatchCount) {
      return true;
    }
    if (tickFrequencyMillis != that.tickFrequencyMillis) {
      return true;
    }
    if (enableBucketing2Processor != that.enableBucketing2Processor) {
      return true;
    }
    if (enableServerHeaderValidation != that.enableServerHeaderValidation) {
      return true;
    }
    if (enableBalancing != that.enableBalancing) {
      return true;
    }
    if (Double.compare(that.inputTrafficMB, inputTrafficMB) != 0) {
      return true;
    }
    if (!Objects.equals(storageHandlerConfig, that.storageHandlerConfig)) {
      return true;
    }
    return !Objects.equals(storageHandlerName, that.storageHandlerName);
  }

  @Override
  public int hashCode() {
    return topic.hashCode();
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString() {
    return "TopicConfig [bufferSize=" + bufferSize + ", ringBufferSize=" + ringBufferSize
        + ", batchMilliSeconds=" + batchMilliSeconds + ", batchByteSize=" + batchSizeBytes
        + ", outputParallelism=" + outputParallelism + ", topic=" + topic + ", storageHandlerConfig="
        + storageHandlerConfig + ", storageHandler=" + storageHandlerName + "]";
  }

}

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
package com.pinterest.memq.client.producer2;

import com.pinterest.memq.client.commons.Compression;
import com.pinterest.memq.client.commons2.MemqCommonClient;
import com.pinterest.memq.client.commons2.retry.RetryStrategy;
import com.pinterest.memq.client.producer.MemqWriteResult;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class RequestManager implements Closeable {
  private final ScheduledExecutorService scheduler;
  private final ExecutorService dispatcher;
  private final MemqCommonClient client;
  private final String topic;
  private final MemqProducer<?, ?> producer;
  private final long sendRequestTimeout;
  private final int maxPayloadBytes;
  private final int lingerMs;
  private final int maxInflightRequests;
  private final Semaphore maxInflightRequestLock;
  private final Compression compression;
  private final boolean disableAcks;
  private final RetryStrategy retryStrategy;

  private final AtomicInteger clientIdGenerator = new AtomicInteger(0);
  private final MetricRegistry metricRegistry;

  private volatile Request currentRequest;
  private Counter requestCounter;

  public RequestManager(MemqCommonClient client,
                        String topic,
                        MemqProducer<?, ?> producer,
                        long sendRequestTimeout,
                        RetryStrategy retryStrategy,
                        int maxPayloadBytes,
                        int lingerMs,
                        int maxInflightRequests,
                        Compression compression,
                        boolean disableAcks,
                        MetricRegistry metricRegistry) {
    ScheduledThreadPoolExecutor tmpScheduler = new ScheduledThreadPoolExecutor(1);
    tmpScheduler.setRemoveOnCancelPolicy(true);
    this.scheduler = tmpScheduler;
    this.dispatcher = Executors.newSingleThreadExecutor(new ThreadFactoryBuilder().setNameFormat("request-dispatch-" + topic).build());
    this.client = client;
    this.topic = topic;
    this.producer = producer;
    this.sendRequestTimeout = sendRequestTimeout;
    this.maxPayloadBytes = maxPayloadBytes;
    this.lingerMs = lingerMs;
    this.maxInflightRequests = maxInflightRequests;
    this.maxInflightRequestLock = new Semaphore(maxInflightRequests);
    this.compression = compression;
    this.disableAcks = disableAcks;
    this.retryStrategy = retryStrategy;
    this.metricRegistry = metricRegistry;
    initializeMetrics();
  }

  @VisibleForTesting
  protected int getAvailablePermits() {
    return maxInflightRequestLock.availablePermits();
  }

  private void initializeMetrics() {
    requestCounter = metricRegistry.counter("requests.created");
    metricRegistry.gauge("requests.inflight", () -> () -> maxInflightRequests - maxInflightRequestLock.availablePermits());
  }

  public Future<MemqWriteResult> write(RawRecord record) throws IOException {
    if (client.isClosed()) {
      throw new IOException("Cannot write to topic " + topic + " when client is closed");
    }
    Request request = getAvailableRequest();
    while (request != null) {
      Future<MemqWriteResult> ret = request.write(record);
      if(ret != null) {
        return ret;
      } else {
        request = getAvailableRequest();
      }
    }
    return null;
  }

  public Request getAvailableRequest() throws IOException {
    if (currentRequest == null || !currentRequest.isAvailable()) {
      synchronized (this) {
        if (currentRequest == null || !currentRequest.isAvailable()) {
          boolean acquired;
          try {
            acquired = maxInflightRequestLock.tryAcquire(0, TimeUnit.MILLISECONDS);
          } catch (InterruptedException ie) {
            throw new IOException("Failed to acquire request lock for topic " + topic + " :", ie);
          }
          if (!acquired) {
            throw new IOException("Could not acquire semaphore, too many inflight requests");
          }
          try {
            if (client.isClosed()) {
              throw new IOException("Cannot write to topic " + topic + " when client is closed");
            }
            currentRequest = new Request(
                dispatcher,
                scheduler,
                client,
                this,
                maxInflightRequestLock,
                topic,
                clientIdGenerator.getAndIncrement(),
                maxPayloadBytes,
                lingerMs,
                sendRequestTimeout,
                retryStrategy,
                disableAcks,
                compression,
                metricRegistry);
            requestCounter.inc();
          } catch (Throwable t) {
            maxInflightRequestLock.release();
            throw t;
          }
        }
        return currentRequest;
      }
    }
    return currentRequest;
  }

  public void flush() {
    if (currentRequest != null) {
      currentRequest.flush();
    }
  }

  public MemqProducer<?, ?> getProducer() {
    return producer;
  }


  @Override
  public void close() throws IOException {
    flush();
    scheduler.shutdown();
    dispatcher.shutdown();
  }
}

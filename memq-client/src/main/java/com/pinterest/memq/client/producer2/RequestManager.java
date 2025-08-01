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
import com.pinterest.memq.client.commons2.MemoryAllocationException;
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

/**
 * This class manages Request creation upon write requests.
 */
public class RequestManager implements Closeable {
  private final ScheduledExecutorService scheduler;
  private final ExecutorService dispatcher;
  private final MemqCommonClient client;
  private final String topic;
  private final MemqProducer<?, ?> producer;
  private final long sendRequestTimeout;
  private final int maxPayloadBytes;
  private final int lingerMs;
  private final long maxInflightRequestsMemoryBytes;
  private final int maxInflightRequests;
  private final Semaphore requestCountPermits;  // to limit the number of inflight requests

  // TODO: perhaps the inflightMemoryPermits should be a static variable shared in the JVM
  private final Semaphore inflightMemoryPermits;  // to limit the memory used by inflight requests
  private final Compression compression;
  private final boolean disableAcks;
  private final RetryStrategy retryStrategy;

  private final AtomicInteger clientIdGenerator = new AtomicInteger(0);
  private final MetricRegistry metricRegistry;
  private final int maxBlockMs;
  private volatile Request currentRequest;
  private Counter requestCounter;

  public RequestManager(MemqCommonClient client,
                        String topic,
                        MemqProducer<?, ?> producer,
                        long sendRequestTimeout,
                        RetryStrategy retryStrategy,
                        int maxPayloadBytes,
                        int lingerMs,
                        int maxBlockMs,
                        int maxInflightRequestsMemoryBytes,
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
    this.maxBlockMs = maxBlockMs;
    this.maxInflightRequestsMemoryBytes = maxInflightRequestsMemoryBytes;
    this.inflightMemoryPermits = new Semaphore(maxInflightRequestsMemoryBytes);
    this.maxInflightRequests = maxInflightRequests;
    this.requestCountPermits = new Semaphore(maxInflightRequests);
    this.compression = compression;
    this.disableAcks = disableAcks;
    this.retryStrategy = retryStrategy;
    this.metricRegistry = metricRegistry;
    initializeMetrics();
  }

  @VisibleForTesting
  protected int getRequestCountAvailablePermits() {
    return requestCountPermits.availablePermits();
  }

  @VisibleForTesting
  protected int getInflightMemoryAvailablePermits() {
    return inflightMemoryPermits.availablePermits();
  }

  private void initializeMetrics() {
    requestCounter = metricRegistry.counter("requests.created");
    metricRegistry.gauge("requests.inflight", () -> () -> maxInflightRequests - requestCountPermits.availablePermits());
    metricRegistry.gauge("requests.memory.inflight", () -> () -> maxInflightRequestsMemoryBytes - inflightMemoryPermits.availablePermits());
  }

  public Future<MemqWriteResult> write(RawRecord record) throws IOException, MemoryAllocationException {
    if (client.isClosed()) {
      throw new IOException("Cannot write to topic " + topic + " when client is closed");
    }
    Request request = getAvailableRequest();
    while (request != null) {
      Future<MemqWriteResult> ret = request.write(record);  // completes exceptionally with MemoryAllocationException if it happens in NetworkClient
      if(ret != null) {
        return ret;
      } else {
        request = getAvailableRequest();
      }
    }
    return null;
  }

  /**
   * Get an available request for the topic. If the current request is not available, it will try to create a new one.
   *
   * When a new request needs to be created, it will try to acquire the request count semaphore and the inflight memory semaphore.
   * The request count semaphore limits the number of inflight requests, while the inflight memory semaphore limits the total memory used by inflight requests.
   *
   * If it successfully acquires both semaphores, it will create a new Request object and return it.
   * Request creation will allocate a buffer of size maxPayloadBytes, which is the maximum payload size for the request.
   *
   * If it fails to acquire either of them, it will throw an IOException upon exhausting the request count semaphore,
   * or MemoryAllocationException upon exhausting the inflight memory semaphore.
   *
   * This method is synchronized to ensure that only one thread can create a new request at a time.
   *
   * @return an available Request object
   * @throws IOException
   * @throws MemoryAllocationException
   */
  public Request getAvailableRequest() throws IOException, MemoryAllocationException {
    if (currentRequest == null || !currentRequest.isAvailable()) {
      synchronized (this) {
        if (currentRequest == null || !currentRequest.isAvailable()) {
          boolean countPermitAcquired = false;
          boolean memoryPermitAcquired = false;
          try {
            countPermitAcquired = requestCountPermits.tryAcquire(0, TimeUnit.MILLISECONDS);
            memoryPermitAcquired = inflightMemoryPermits.tryAcquire(maxPayloadBytes, maxBlockMs, TimeUnit.MILLISECONDS);
          } catch (InterruptedException ie) {
            maybeReleaseMemoryAndCountPermit(memoryPermitAcquired, countPermitAcquired);
            throw new IOException("Failed to acquire request locks for topic " + topic + " :", ie);
          }
          if (!countPermitAcquired) {
            maybeReleaseMemoryAndCountPermit(memoryPermitAcquired, countPermitAcquired);
            throw new IOException(
                    String.format(
                      "Could not acquire request count semaphore. " +
                      "Current count: %s, Max count: %s for topic: %s",
                      maxInflightRequests - requestCountPermits.availablePermits(), maxInflightRequests, topic
                    )
            );
          }
          if (!memoryPermitAcquired) {
            maybeReleaseMemoryAndCountPermit(memoryPermitAcquired, countPermitAcquired);
            throw new MemoryAllocationException(
                    String.format(
                      "Could not acquire inflight request memory semaphore in %sms. " +
                      "Current memory: %s bytes, Max memory: %s bytes for topic: %s",
                      maxBlockMs,
                      maxInflightRequestsMemoryBytes - inflightMemoryPermits.availablePermits(),
                      maxInflightRequestsMemoryBytes, topic
                    )
            );
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
                requestCountPermits,
                inflightMemoryPermits,
                topic,
                clientIdGenerator.getAndIncrement(),
                maxPayloadBytes,
                lingerMs,
                maxBlockMs,
                sendRequestTimeout,
                retryStrategy,
                disableAcks,
                compression,
                metricRegistry);
            requestCounter.inc();
          } catch (MemoryAllocationException ibme) {
            // specifically re-throw MemoryAllocationException to let upstream handle it
            requestCountPermits.release();
            inflightMemoryPermits.release(maxPayloadBytes);
            throw new MemoryAllocationException("Failed to allocate buffer memory for topic " + topic + ": ", ibme);
          } catch (Throwable t) {
            requestCountPermits.release();
            inflightMemoryPermits.release(maxPayloadBytes);
            throw t;
          }
        }
        return currentRequest;
      }
    }
    return currentRequest;
  }

  private void maybeReleaseMemoryAndCountPermit(boolean memoryPermitAcquired, boolean countPermitAcquired) {
    if (countPermitAcquired) {
      requestCountPermits.release();
    }
    if (memoryPermitAcquired) {
      inflightMemoryPermits.release(maxPayloadBytes);
    }

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

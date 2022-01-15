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

import java.io.IOException;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;

import com.codahale.metrics.MetricRegistry;
import com.pinterest.memq.core.commons.Message;
import com.pinterest.memq.core.commons.MessageBufferInputStream;

@StorageHandlerName(name = "delayeddevnull")
public class DelayedDevNullStorageHandler implements StorageHandler {

  private int maxDelay;
  private ThreadLocalRandom rand;
  private int minDelay;
  private static AtomicLong counter = new AtomicLong();
  private static AtomicLong byteCounter = new AtomicLong();
  private static AtomicLong inputStreamCounter = new AtomicLong();

  @Override
  public void initWriter(Properties outputHandlerConfig,
                   String topic,
                   MetricRegistry registry) throws Exception {
    minDelay = Integer.parseInt(outputHandlerConfig.getProperty("delay.min.millis", "100"));
    maxDelay = Integer.parseInt(outputHandlerConfig.getProperty("delay.max.millis", "2000"));
    rand = ThreadLocalRandom.current();
  }

  @Override
  public void writeOutput(int sizeInBytes,
                          int checksum,
                          List<Message> messages) throws WriteFailedException {
    try {
      Thread.sleep(rand.nextInt(minDelay, maxDelay));
      counter.accumulateAndGet(messages.size(), (v1, v2) -> v1 + v2);
      byteCounter.accumulateAndGet(sizeInBytes, (v1, v2) -> v1 + v2);
      MessageBufferInputStream is = new MessageBufferInputStream(messages, null);
      while (is.read() != -1) {
        inputStreamCounter.incrementAndGet();
      }
      is.close();
    } catch (InterruptedException | IOException e) {
      // ignore errors
    }
  }

  public static long getCounter() {
    return counter.get();
  }

  public static long getByteCounter() {
    return byteCounter.get();
  }

  public static long getInputStreamCounter() {
    return inputStreamCounter.get();
  }

  public static void reset() {
    counter.set(0);
    byteCounter.set(0);
    inputStreamCounter.set(0);
  }

  @Override
  public String getReadUrl() {
    return "delayeddevnull";
  }
}
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
package com.pinterest.memq.core.utils;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.pinterest.memq.core.commons.Message;
import com.pinterest.memq.core.processing.ProcessingStatus;

public class TestCoreUtils {

  @Test
  public void testBatchSizeCalculator() {
    List<Message> batch = new ArrayList<>();
    int totalBytes = 0;
    for (int i = 0; i < 100; i++) {
      int size = ThreadLocalRandom.current().nextInt(800);
      totalBytes += size;
      byte[] testData = new byte[size];
      Message m = new Message(1024, false);
      m.put(testData);
      batch.add(m);
    }
    assertEquals(totalBytes, CoreUtils.batchSizeInBytes(batch));
  }

  @Test
  public void testLRUExpiration() throws InterruptedException {
    LoadingCache<Long, ProcessingStatus> ackMap = CacheBuilder.newBuilder().maximumSize(10000)
        .expireAfterWrite(1, TimeUnit.SECONDS)
        .removalListener(new RemovalListener<Long, ProcessingStatus>() {

          @Override
          public void onRemoval(RemovalNotification<Long, ProcessingStatus> notification) {
            System.out.println("Removed");
          }
        }).build(new CacheLoader<Long, ProcessingStatus>() {

          @Override
          public ProcessingStatus load(Long key) throws Exception {
            System.out.println("Loading:" + key);
            return ProcessingStatus.FAILED;
          }
        });
    ackMap.put(1L, ProcessingStatus.PENDING);
    assertEquals(1, ackMap.size());
    Executors.newScheduledThreadPool(1, DaemonThreadFactory.INSTANCE)
        .scheduleWithFixedDelay(() -> ackMap.cleanUp(), 0, 1, TimeUnit.SECONDS);
    Thread.sleep(3000);
    assertEquals(0, ackMap.size());
  }
}

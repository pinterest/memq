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
package com.pinterest.memq.commons.storage.s3;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import org.junit.Test;

import com.codahale.metrics.MetricRegistry;

public class TestCustomS3Async2OutputHandler {

  @Test
  public void testAnyUploadResultOrTimeout() throws Exception {
    CustomS3Async2StorageHandler handler = new CustomS3Async2StorageHandler();
    Properties props = new Properties();
    props.setProperty("bucket", "test");
    handler.initWriter(props, "test", new MetricRegistry());
    List<CompletableFuture<CustomS3Async2StorageHandler.UploadResult>> tasks = new ArrayList<>();

    for(int i = 1; i <= 5; i++) {
      final int j = i;
      tasks.add(CompletableFuture.supplyAsync(() -> {
        try {
          Thread.sleep(200 * j);
        } catch (Exception e) {
        }
        return new CustomS3Async2StorageHandler.UploadResult("task-" + j, 200, null, 0, j);
      }));
    }

    try {
      CustomS3Async2StorageHandler.UploadResult r = handler.anyUploadResultOrTimeout(tasks, Duration.ofMillis(1000)).get();
      assertEquals(r.getKey(), "task-1");
    } catch (Exception e) {
      fail("Should not fail: " + e);
    }

    tasks.clear();

    for(int i = 1; i <= 5; i++) {
      final int j = i;
      tasks.add(CompletableFuture.supplyAsync(() -> {
        try {
          Thread.sleep(200 * j + 1000);
        } catch (Exception e) {
        }
        return new CustomS3Async2StorageHandler.UploadResult("task-" + j, 200, null, 0, j);
      }));
    }

    try {
      CustomS3Async2StorageHandler.UploadResult r = handler.anyUploadResultOrTimeout(tasks, Duration.ofMillis(1000)).get();
      fail("Should timeout");
    } catch (ExecutionException ee) {
      System.out.println(ee);
      assertTrue(ee.getCause() instanceof TimeoutException);
    } catch (Exception e) {
      fail("Should throw timeout exception");
    }
  }
}
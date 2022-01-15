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
package com.pinterest.memq.core.commons;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

public final class MemqProcessingThreadFactory implements ThreadFactory {
  private final String threadBaseName;
  private AtomicInteger counter = new AtomicInteger();

  public MemqProcessingThreadFactory(String threadBaseName) {
    this.threadBaseName = threadBaseName;
  }

  @Override
  public Thread newThread(Runnable r) {
    Thread th = new Thread(r);
    th.setName(threadBaseName + counter.incrementAndGet());
    th.setDaemon(true);
    return th;
  }
}
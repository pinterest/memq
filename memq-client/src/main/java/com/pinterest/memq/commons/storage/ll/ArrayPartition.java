/**
 * Copyright 2023 Pinterest, Inc.
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
package com.pinterest.memq.commons.storage.ll;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.google.gson.Gson;
import com.google.gson.JsonObject;

public class ArrayPartition implements Partition {

  private static final Gson GSON = new Gson();
  
  private ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
  private ReentrantReadWriteLock.ReadLock readLock = lock.readLock();
  private ReentrantReadWriteLock.WriteLock writeLock = lock.writeLock();
  private int partitionId;
  private JsonObject[] offsetQueue;
  private volatile long startOffset;
  private volatile long writeOffset;
  private volatile long partitionSizeInBytes;
  private FileOutputStream fos;
  private LoadingCache<Long, JsonObject[]> backfillCache;
  private String basePath;

  public ArrayPartition(String basePath, int partitionId) {
    this.basePath = basePath;
    this.partitionId = partitionId;
    this.offsetQueue = new JsonObject[1000];
    this.backfillCache = Caffeine.newBuilder().maximumSize(10)
        .expireAfterAccess(100, TimeUnit.SECONDS).build(k -> {
          return readNotifications(k);
        });
  }

  @Override
  public void init(Properties props) throws IOException {
  }

  @Override
  public long write(JsonObject entry) throws IOException {
    if (entry.get(TIMESTAMP).getAsLong() == 0) {
      entry.addProperty(TIMESTAMP, System.currentTimeMillis());
    }
    long offset = 0;
    writeLock.lock();
    try {
      entry.addProperty(OFFSET, writeOffset);
      offset = writeOffset;
      offsetQueue[calculateOffset(writeOffset)] = entry;
      ++writeOffset;
      partitionSizeInBytes += entry.get(SIZE).getAsLong();
      // save the update to file
      saveEntryToCurrent(entry);
    } finally {
      writeLock.unlock();
    }
    return offset;
  }

  private JsonObject[] readNotifications(long fileNumber) throws Exception {
    String calculateFilePath = calculateFilePath(fileNumber);
    File file = new File(calculateFilePath);
    if (!file.exists()) {
      return null;
    }
    JsonObject[] entries = new JsonObject[offsetQueue.length];
    List<String> lines = Files.readAllLines(file.toPath());
    for (int i = 0; i < lines.size(); i++) {
      entries[i] = GSON.fromJson(lines.get(i), JsonObject.class);
    }
    return entries;
  }

  protected String calculateFilePath(long fileNumber) {
    StringBuilder builder = new StringBuilder();
    builder.append(basePath);
    builder.append(String.format("%015d", fileNumber));
    return builder.toString();
  }

  protected void saveEntryToCurrent(JsonObject entry) throws IOException {
    long fileNumber = getFileNumber(entry.get(OFFSET).getAsLong());
    if (fileNumber == entry.get(OFFSET).getAsLong()) {
      if (fos != null) {
        fos.close();
      }
      fos = null;
    }
    if (fos == null) {
      fos = new FileOutputStream(calculateFilePath(fileNumber));
    }
    fos.write(entry.toString().getBytes());
    fos.write('\n');
    fos.getFD().sync();
  }

  protected int calculateOffset(long offset) {
    return (int) (offset % offsetQueue.length);
  }

  @Override
  public JsonObject get(long offset) {
    readLock.lock();
    JsonObject val = offsetQueue[calculateOffset(offset)];
    readLock.unlock();
    return val;
  }

  @Override
  public long earliestOffset() {
    readLock.lock();
    try {
      return startOffset;
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public long latestOffset() {
    readLock.lock();
    try {
      return writeOffset - 1;
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public long getPartitionSizeInBytes() {
    readLock.lock();
    try {
      return partitionSizeInBytes;
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public void applyRetention(long earliestTimestamp, long maxSize) {
    writeLock.lock();
    if (partitionSizeInBytes > maxSize
        || (writeOffset > 0 && offsetQueue[calculateOffset(writeOffset)].get(TIMESTAMP)
            .getAsLong() < earliestTimestamp)) {
    }
    writeLock.unlock();
  }

  @Override
  public JsonObject next(long offset) throws OffsetOutOfRangeException {
    if (offset > writeOffset) {
      throw new IllegalArgumentException();
    }
    readLock.lock();
    try {
      int queueOffset = (int) (offset % offsetQueue.length);
      JsonObject notificationEntry = offsetQueue[queueOffset];
      if (notificationEntry.get(OFFSET).getAsLong() != offset) {
        // offset is older than one in the queue
        long filenumber = getFileNumber(offset);
        JsonObject[] notificationEntries = backfillCache.get(filenumber);
        if (notificationEntries != null && notificationEntries[queueOffset] != null) {
          return notificationEntries[queueOffset];
        }
      } else {
        return notificationEntry;
      }
    } finally {
      readLock.unlock();
    }
    throw new OffsetOutOfRangeException();
  }

  protected long getFileNumber(long offset) {
    return (offset / offsetQueue.length) * offsetQueue.length;
  }

  @Override
  public int getPartitionId() {
    return partitionId;
  }

  protected JsonObject[] getOffsetQueue() {
    return offsetQueue;
  }
}
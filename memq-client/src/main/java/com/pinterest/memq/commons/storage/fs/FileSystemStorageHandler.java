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
package com.pinterest.memq.commons.storage.fs;

import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.io.input.BoundedInputStream;
import org.apache.commons.io.input.RandomAccessFileInputStream;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.codahale.metrics.Timer.Context;
import com.google.gson.JsonObject;
import com.pinterest.memq.client.commons2.DataNotFoundException;
import com.pinterest.memq.client.commons2.TopicNotFoundException;
import com.pinterest.memq.commons.BatchHeader;
import com.pinterest.memq.commons.protocol.BatchData;
import com.pinterest.memq.commons.storage.ReadBrokerStorageHandler;
import com.pinterest.memq.commons.storage.StorageHandler;
import com.pinterest.memq.commons.storage.StorageHandlerName;
import com.pinterest.memq.commons.storage.WriteFailedException;
import com.pinterest.memq.commons.storage.s3.CustomS3Async2StorageHandler;
import com.pinterest.memq.commons.storage.s3.KafkaNotificationSink;
import com.pinterest.memq.core.commons.Message;
import com.pinterest.memq.core.utils.DaemonThreadFactory;
import com.pinterest.memq.core.utils.MiscUtils;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.util.ReferenceCounted;

/**
 * This StorageHandler is used for local filesystem based PubSub.
 */
@StorageHandlerName(name = "filesystem")
public class FileSystemStorageHandler extends ReadBrokerStorageHandler {

  public static final String OPTIMIZATION_SENDFILE = "optimization.sendfile";
  public static final String NOTIFICATIONS_DISABLE = "disableNotifications";
  private static final String ROOT_DIRS = "rootDir";
  public static final String STORAGE_DIRS = "storageDirs";
  private static final String HOSTNAME = MiscUtils.getHostname();
  public static final String PATH = "path";
  public static final String TOPIC = "topic";
  public static final String HEADER_SIZE = "headerSize";
  public static final String NUMBER_OF_MESSAGES_IN_BATCH = "numBatchMessages";
  private static final int LAST_ATTEMPT_TIMEOUT = 60_000;

  private Logger logger = Logger.getLogger(FileSystemStorageHandler.class.getName());
  private String[] storageDirs;
  private KafkaNotificationSink notificationSink;
  private String topic;
  private MetricRegistry registry;
  private boolean dryrun;
  private boolean disableNotifications;

  private Timer notificationPublishingTimer;
  private ExecutorService requestExecutor;
  private ScheduledExecutorService executionTimer;
  private int retryTimeoutMillis;
  private int maxAttempts;
  private Counter timeoutExceptionCounter;
  private List<String> storageDirList;
  private boolean useSendFileOptimization;
  private Timer persistTimer;

  @Override
  public void initWriter(Properties properties,
                         String topic,
                         MetricRegistry registry) throws Exception {
    this.logger = Logger.getLogger(FileSystemStorageHandler.class.getName() + "-" + topic);
    this.topic = topic;
    this.registry = registry;
    this.dryrun = Boolean.parseBoolean(properties.getProperty("dryrun", "false"));
    this.disableNotifications = Boolean
        .parseBoolean(properties.getProperty(NOTIFICATIONS_DISABLE, "true"));
    if (!disableNotifications) {
      this.notificationSink = new KafkaNotificationSink();
      this.notificationSink.init(properties);
      this.notificationPublishingTimer = MiscUtils.oneMinuteWindowTimer(registry,
          "output.notification.publish.latency");
    }
    this.persistTimer = MiscUtils.oneMinuteWindowTimer(registry,
        "storage.persist.latency");
    this.timeoutExceptionCounter = registry.counter("output.timeout.exceptions");
    this.requestExecutor = Executors.newCachedThreadPool(new DaemonThreadFactory());
    this.executionTimer = Executors.newSingleThreadScheduledExecutor(new DaemonThreadFactory());
    this.useSendFileOptimization = Boolean
        .parseBoolean(properties.getProperty(OPTIMIZATION_SENDFILE, "false"));
    initDataDirs(properties);
    this.retryTimeoutMillis = Integer.parseInt(properties.getProperty("retryTimeoutMillis", "500"));
    this.maxAttempts = storageDirs.length;
    for (String dir : storageDirs) {
      Files.createDirectories(Paths.get(dir, topic));
    }
    setLocalRead(true);
  }

  private void initDataDirs(Properties props) {
    if (props.containsKey(ROOT_DIRS)) {
      this.storageDirs = props.getProperty(ROOT_DIRS).split(",");
    }
    // added for backwards compatibility reasons
    if (props.containsKey(STORAGE_DIRS)) {
      this.storageDirs = props.getProperty(STORAGE_DIRS).split(",");
    }
    storageDirList = Arrays.asList(this.storageDirs);
  }

  @Override
  public boolean reconfigure(Properties outputHandlerConfig) {
    return super.reconfigure(outputHandlerConfig);
  }

  @Override
  public void writeOutput(int sizeInBytes,
                          int checksum,
                          List<Message> messages) throws WriteFailedException {
    Message firstMessage = messages.get(0);
    String relativeFileName = createFileName(topic, firstMessage.getClientRequestId(),
        firstMessage.getServerRequestId());
    List<ByteBuf> buffers = CustomS3Async2StorageHandler.messageToBufferList(messages);
    ByteBuf batchHeader = StorageHandler.getBatchHeadersAsByteArray(messages);
    CompositeByteBuf content = CustomS3Async2StorageHandler
        .messageAndHeaderToCompositeBuffer(buffers, batchHeader);
    Context persistTime = persistTimer.time();
    String writePath = speculativeUpload(relativeFileName, content);
    persistTime.stop();
    buffers.forEach(ReferenceCounted::release);
    content.release();
    batchHeader.release();
    if (!disableNotifications) {
      final JsonObject payload = buildNotification(topic, writePath,
          sizeInBytes + batchHeader.readableBytes(), messages.size(), batchHeader.capacity());
      Timer.Context publishTime = notificationPublishingTimer.time();
      try {
        notificationSink.notify(payload, 0);
        logger.fine(() -> "Notifying:" + payload);
      } catch (Exception e) {
        throw new WriteFailedException(e);
      } finally {
        publishTime.stop();
      }
    }
  }

  private String speculativeUpload(String baseFileName,
                                   CompositeByteBuf content) throws WriteFailedException {
    int attempt = 0;
    Map<String, Future<String>> futureMap = new HashMap<>();
    Map<String, CompletableFuture<String>> taskMap = new HashMap<>();
    final int currentMaxAttempts = maxAttempts;
    final int currentRetryTimeoutMs = retryTimeoutMillis;
    String result = null;
    boolean hasSucceeded = false;
    while (attempt < maxAttempts) {
      final int timeout = attempt == currentMaxAttempts - 1 ? LAST_ATTEMPT_TIMEOUT
          : currentRetryTimeoutMs;
      CompletableFuture<String> task = new CompletableFuture<>();
      final int localAttemptId = attempt;
      // add attempt number as a suffix
      final String key = topic + "/" + HOSTNAME + "/" + baseFileName + "_" + localAttemptId;
      Callable<String> uploadAttempt = writeFileTask(content, task, localAttemptId, key);
      Future<String> future = requestExecutor.submit(uploadAttempt);
      futureMap.put(key, future);
      taskMap.put(key, task);

      CompletableFuture<String> resultFuture = anyUploadResultOrTimeout(taskMap.values(),
          Duration.ofMillis(timeout));
      try {
        result = resultFuture.get();
        registry.counter("storage.fs.succeeded").inc();
        hasSucceeded = true;
      } catch (ExecutionException ee) {
        if (ee.getCause() instanceof TimeoutException) {
          timeoutExceptionCounter.inc();
        } else {
          logger.log(Level.SEVERE, "Request failed", ee);
        }
      } catch (Exception e) {
        logger.log(Level.SEVERE, "Request failed", e);
      }
      attempt++;
    }
    for (Map.Entry<String, Future<String>> entry : futureMap.entrySet()) {
      if (result != null && entry.getKey().endsWith(result)) {
        continue;
      }
      entry.getValue().cancel(true);
    }
    if (result == null) {
      throw new WriteFailedException("All upload attempts failed");
    } else if (!hasSucceeded) {
      throw new WriteFailedException("Upload failed due to error out: " + result);
    } else {
      registry.counter("storage.fs.attempt." + attempt).inc();
      return result;
    }
  }

  public Callable<String> writeFileTask(CompositeByteBuf content,
                                        CompletableFuture<String> task,
                                        final int attemptId,
                                        final String key) {
    return new Callable<String>() {
      @Override
      public String call() throws Exception {
        try {
          File fileToWrite = new File(storageDirs[attemptId], key);
          fileToWrite.getParentFile().mkdirs();
          try (OutputStream os = new BufferedOutputStream(new FileOutputStream(fileToWrite))) {
            if (!dryrun) {
              content.readBytes(os, content.readableBytes());
            }
            os.close();
          } catch (Exception e) {
            throw new WriteFailedException(e);
          }
          String ur = fileToWrite.getAbsolutePath();
          task.complete(ur);
          return ur;
        } catch (Exception e) {
          task.completeExceptionally(e);
          throw e;
        }
      }
    };
  }

  public static String createFileName(String topic, long clientRequestId, long serverRequestId) {
    StringBuilder sb = new StringBuilder();
    sb.append(clientRequestId).append("_").append(serverRequestId);
    return sb.toString();
  }

  private JsonObject buildNotification(String topic,
                                       String path,
                                       int objectSize,
                                       int numberOfMessages,
                                       int batchHeaderLength) {
    JsonObject payload = new JsonObject();
    payload.addProperty(PATH, path);
    payload.addProperty(SIZE, objectSize);
    payload.addProperty(TOPIC, topic);
    payload.addProperty(HEADER_SIZE, batchHeaderLength);
    payload.addProperty(NUMBER_OF_MESSAGES_IN_BATCH, numberOfMessages);

    return payload;
  }

  @Override
  public String getReadUrl() {
    return null;
  }

  @Override
  public void closeWriter() {
    super.closeWriter();
  }

  @Override
  public void initReader(Properties properties, MetricRegistry registry) throws Exception {
    super.initReader(properties, registry);
    useSendFileOptimization = Boolean
        .parseBoolean(properties.getProperty(OPTIMIZATION_SENDFILE, "false"));
    initDataDirs(properties);
  }

  @Override
  public BatchData fetchBatchStreamForNotificationBuf(JsonObject nextNotificationToProcess) throws IOException,
                                                                                            DataNotFoundException {
    File file = validateAndGetReadPath(nextNotificationToProcess);
    int length = (int) file.length();
    if (useSendFileOptimization) {
      logger.fine(
          () -> "Sendfile Read:" + length + " into buffer for path:" + file.getAbsolutePath());
      return new BatchData(length, file);
    } else {
      FileInputStream stream = new FileInputStream(file);
      ByteBuf buffer = PooledByteBufAllocator.DEFAULT.buffer(length);
      int writeBytes = buffer.writeBytes(stream, length);
      logger.fine(() -> "Read:" + writeBytes + " into buffer for path:" + file.getAbsolutePath());
      return new BatchData(length, buffer);
    }
  }

  @Override
  public InputStream fetchBatchStreamForNotification(JsonObject objectNotification) throws IOException,
                                                                                    DataNotFoundException {
    if (isLocalRead()) {
      File filePath = validateAndGetReadPath(objectNotification);
      return new FileInputStream(filePath);
    } else {
      try {
        long ts = System.currentTimeMillis();
        BatchData batch = readBatch(objectNotification.get(TOPIC).getAsString(),
            objectNotification);
        // release buffer on close
        logger.fine(
            () -> "Received:" + batch.getLength() + " Time:" + (System.currentTimeMillis() - ts));
        return new ByteBufInputStream(batch.getDataAsBuf(), true);
      } catch (TopicNotFoundException e) {
        throw new IOException(e);
      } catch (DataNotFoundException e) {
        throw e;
      } catch (Exception e) {
        throw new IOException(e);
      }
    }
  }

  /**
   * Validate read path in order to prevent random file reads from the disk.
   * 
   * @param objectNotification
   * @return
   * @throws IOException
   * @throws DataNotFoundException
   */
  protected File validateAndGetReadPath(JsonObject objectNotification) throws IOException,
                                                                       DataNotFoundException {
    String filePath = objectNotification.get(PATH).getAsString();
    String topic = objectNotification.get(TOPIC).getAsString();
    if (!isValidReadRequest(topic, filePath)) {
      throw new IOException("Invalid read path:" + filePath);
    }
    File file = new File(filePath);
    if (!file.exists()) {
      throw new DataNotFoundException(filePath + " does not exist");
    }
    return file;
  }

  @Override
  public BatchData fetchHeaderForBatchBuf(JsonObject nextNotificationToProcess) throws IOException,
                                                                                DataNotFoundException {
    BatchHeader header = fetchHeaderForBatch(nextNotificationToProcess);
    ByteBuf dataBuf = header.writeHeaderToByteBuf(PooledByteBufAllocator.DEFAULT.buffer());
    return new BatchData(dataBuf.readableBytes(), dataBuf);
  }

  @Override
  public BatchHeader fetchHeaderForBatch(JsonObject objectNotification) throws IOException,
                                                                        DataNotFoundException {
    if (isLocalRead()) {
      try (DataInputStream dis = new DataInputStream(
          fetchBatchStreamForNotification(objectNotification))) {
        return new BatchHeader(dis);
      }
    } else {
      BatchData batch;
      try {
        batch = readBatchHeader(objectNotification.get(TOPIC).getAsString(),
            objectNotification);
      } catch (Exception e) {
        throw new IOException(e);
      }
      // release buffer on close
      DataInputStream stream = new DataInputStream(
          new ByteBufInputStream(batch.getDataAsBuf(), true));
      BatchHeader batchHeader = new BatchHeader(stream);
      stream.close();
      return batchHeader;
    }
  }

  @Override
  public BatchData fetchMessageAtIndexBuf(JsonObject objectNotification,
                                          BatchHeader.IndexEntry index) throws IOException,
                                                                        DataNotFoundException {
    DataInputStream stream = fetchMessageAtIndex(objectNotification, index);
    ByteBuf buffer = PooledByteBufAllocator.DEFAULT.buffer();
    buffer.writeBytes(stream, index.getSize());
    return new BatchData(index.getSize(), buffer);
  }

  @Override
  public DataInputStream fetchMessageAtIndex(JsonObject objectNotification,
                                             BatchHeader.IndexEntry index) throws IOException,
                                                                           DataNotFoundException {
    if (isLocalRead()) {
      File file = validateAndGetReadPath(objectNotification);
      RandomAccessFile raf = new RandomAccessFile(file, "r");
      raf.seek(index.getOffset());
      RandomAccessFileInputStream is = new RandomAccessFileInputStream(raf, true);
      return new DataInputStream(new BoundedInputStream(is, index.getSize()));
    } else {
      BatchData batch;
      try {
        logger.fine(
            () -> "Making index message fetch request:" + objectNotification + " index:" + index);
        batch = readBatchAtIndex(objectNotification.get(TOPIC).getAsString(),
            objectNotification, index);
      } catch (Exception e) {
        throw new IOException(e);
      }
      return new DataInputStream(new ByteBufInputStream(batch.getDataAsBuf(), true));
    }
  }

  /**
   * A valid read request must start with storageDir/topic
   * 
   * @param topic
   * @param filePath
   * @return
   */
  protected boolean isValidReadRequest(String topic, String filePath) {
    return storageDirList.stream()
        .anyMatch(storageDir -> filePath.startsWith(storageDir));
  }

  public CompletableFuture<String> anyUploadResultOrTimeout(Collection<CompletableFuture<String>> tasks,
                                                            Duration duration) {
    final CompletableFuture<String> promise = new CompletableFuture<>();
    executionTimer.schedule(() -> {
      final TimeoutException ex = new TimeoutException(
          "Timeout after " + duration.toMillis() + " milliseconds");
      return promise.completeExceptionally(ex);
    }, duration.toMillis(), TimeUnit.MILLISECONDS);
    CompletableFuture<String> anyUploadResultFuture = CompletableFuture
        .anyOf(tasks.toArray(new CompletableFuture[0])).thenApply(o -> (String) o);
    return anyUploadResultFuture.applyToEither(promise, Function.identity());
  }

  @Override
  public void closeReader() {
    super.closeReader();
  }
}

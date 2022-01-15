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
package com.pinterest.memq.client.commons;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;
import java.util.logging.Logger;

import com.codahale.metrics.MetricRegistry;
import com.google.gson.JsonObject;
import com.pinterest.memq.client.commons.audit.Auditor;
import com.pinterest.memq.commons.BatchHeader;
import com.pinterest.memq.commons.CloseableIterator;
import com.pinterest.memq.commons.MemqLogMessage;
import com.pinterest.memq.commons.MessageId;
import com.pinterest.memq.core.utils.MemqUtils;

public class MemqLogMessageIterator<K, V> implements CloseableIterator<MemqLogMessage<K, V>> {

  private static final Logger logger = Logger
      .getLogger(MemqLogMessageIterator.class.getCanonicalName());
  public static final String METRICS_PREFIX = "memqConsumer";
  public static final String MESSAGES_PROCESSED_COUNTER_KEY = METRICS_PREFIX
      + ".messagesProcessedCounter";
  public static final String BYTES_PROCESSED_METER_KEY = METRICS_PREFIX + ".bytesProcessedCounter";
  protected DataInputStream uncompressedBatchInputStream;
  protected Deserializer<V> valueDeserializer;
  protected Deserializer<K> headerDeserializer;
  protected int messagesToRead;
  protected MetricRegistry metricRegistry;
  protected JsonObject currNotificationObj;
  protected int currentMessageOffset;
  protected DataInputStream stream;
  private int notificationPartitionId;
  private long notificationPartitionOffset;
  private long notificationReadTimestamp;
  private int objectSize;
  private Auditor auditor;
  private String cluster;
  private String topic;
  private MemqMessageHeader header;
  private byte[] messageIdHash;
  private int auditedMessageCount;
  private BatchHeader batchHeader = null;
  private String clientId;
  
  protected MemqLogMessageIterator() {
  }

  public MemqLogMessageIterator(String cluster,
                                String clientId,
                                DataInputStream stream,
                                JsonObject currNotificationObj,
                                Deserializer<K> headerDeserializer,
                                Deserializer<V> valueDeserializer,
                                MetricRegistry metricRegistry,
                                boolean skipHeaderRead,
                                Auditor auditor) throws IOException {
    this.cluster = cluster;
    this.clientId = clientId;
    this.stream = stream;
    this.currNotificationObj = currNotificationObj;
    this.auditor = auditor;
    try {
      this.topic = currNotificationObj.get(MemqLogMessage.INTERNAL_FIELD_TOPIC).getAsString();
      this.objectSize = currNotificationObj.get(MemqLogMessage.INTERNAL_FIELD_OBJECT_SIZE).getAsInt();
      this.notificationPartitionId = currNotificationObj
          .get(MemqLogMessage.INTERNAL_FIELD_NOTIFICATION_PARTITION_ID).getAsInt();
      this.notificationPartitionOffset = currNotificationObj
          .get(MemqLogMessage.INTERNAL_FIELD_NOTIFICATION_PARTITION_OFFSET).getAsLong();
      this.notificationReadTimestamp = currNotificationObj
          .get(MemqLogMessage.INTERNAL_FIELD_NOTIFICATION_READ_TIMESTAMP).getAsLong();
    } catch (Exception e) {
    }
    this.headerDeserializer = headerDeserializer;
    this.valueDeserializer = valueDeserializer;
    this.metricRegistry = metricRegistry;
    if (!skipHeaderRead) {
      batchHeader = new BatchHeader(stream);
    }
    readHeaderAndLoadBatch();
  }

  @Override
  public boolean hasNext() {
    // no more notifications
    try {
      if (stream.available() <= 0) {
        // last batch remaining to read
        return messagesToRead > 0;
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    // more notifications or object not fully read
    return true;
  }

  @Override
  public MemqLogMessage<K, V> next() {
    if (messagesToRead > 0) {
      // still more MemqLogMessages in this batch
      try {
        return getMemqLogMessage();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    } else {
      // no more MemqLogMessages in this batch
      try {
        if (stream.available() > 0) {
          // there are more batches, process new batch
          if (readHeaderAndLoadBatch()) {
            return next();
          }
        }
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
    throw new RuntimeException(new DataCorruptionException("No next"));
  }

  /**
   * Reads the next batch's header and returns whether there are bytes to read
   *
   * @return true if there are batch bytes to read, false otherwise
   */
  protected boolean readHeaderAndLoadBatch() {
    try {
      // since we are about to read a brand new batch therefore we should reset the
      // messageIdHash
      messageIdHash = null;
      auditedMessageCount = 0;
      header = readHeader();
      logger.fine(() -> "Message header:" + header);
      byte[] batch = new byte[header.getMessageLength()];
      stream.read(batch);
      if (!CommonUtils.crcChecksumMatches(batch, header.getCrc())) {
        // CRC checksum mismatch
        throw new RuntimeException(new DataCorruptionException("CRC checksum mismatch"));
      }
      if (uncompressedBatchInputStream != null) {
        uncompressedBatchInputStream.close();
      }

      // using byte array here rather than ByteBuf since the getMemqLogMessage will iterate through one message at a time,
      // which would be in the range of tens of KBs, unless the message itself has a extremely high compression ratio
      uncompressedBatchInputStream = CommonUtils.getUncompressedInputStream(header.getCompression(),
          new ByteArrayInputStream(batch));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    return messagesToRead > 0;
  }

  protected MemqMessageHeader readHeader() throws IOException {
    MemqMessageHeader header = new MemqMessageHeader(stream);
    if (header.getMessageLength() > objectSize) {
      logger.severe("BatchLength (" + header.getMessageLength() + ") is larger than objectSize ("
          + objectSize + ")");
      throw new IOException(
          "Invalid batchLength found:" + header.getMessageLength() + " vs:" + objectSize);
    } else if (header.getMessageLength() < 0) {
      logger.severe("BatchLength (" + header.getMessageLength() + ") is less than zero");
      throw new IOException("Invalid batchLength found:" + header.getMessageLength());
    }
    messagesToRead = header.getLogmessageCount();
    return header;
  }

  /**
   * This method allows skipping to last message of a Batch efficiently. <br>
   * Using this method allows clients to skip decompression of one message at the
   * time and simply scroll to the last message. <br>
   * <br>
   * This method skips over all Messages but the last one and then reads the last
   * message till (N-1)th message leaving the last message to be accessed via the
   * next() method.
   * 
   * @throws IOException
   */
  public void skipToLastLogMessage() throws IOException {
    currentMessageOffset = 0;
    if (stream == null) {
      throw new IOException("Stream is null can't skip to last");
    }
    while (stream.available() > 0) {
      readHeaderAndLoadBatch();
    }
    while (messagesToRead > 1) {
      getMemqLogMessage();
    }
  }

  /**
   * Reads the uncompressed input stream and returns a MemqLogMessage
   *
   * @return a MemqLogMessage
   * @throws IOException
   */
  protected MemqLogMessage<K, V> getMemqLogMessage() throws IOException {
    short logMessageInternalFieldsLength = uncompressedBatchInputStream.readShort();
    long writeTimestamp = 0;
    MessageId messageId = null;
    Map<String, byte[]> headers = null;
    if (logMessageInternalFieldsLength > 0) {
      writeTimestamp = uncompressedBatchInputStream.readLong();

      // messageId
      int messageIdLength = uncompressedBatchInputStream.read();
      if (messageIdLength > 0) {
        byte[] messageIdAry = new byte[messageIdLength];
        uncompressedBatchInputStream.readFully(messageIdAry);
        messageId = new MessageId(messageIdAry);
        messageIdHash = MemqUtils.calculateMessageIdHash(messageIdHash, messageId.toByteArray());
        auditedMessageCount++;
      }

      // headers
      short headerLength = uncompressedBatchInputStream.readShort();
      if (headerLength > 0) {
        headers = deserializeHeaders(headerLength, uncompressedBatchInputStream);
      }

      // ###################################################
      // do something with internal headers here in future
      // ###################################################
    }
    int keyLength = uncompressedBatchInputStream.readInt();
    byte[] keyBytes = null;
    if (keyLength > 0) {
      keyBytes = new byte[keyLength];
      uncompressedBatchInputStream.readFully(keyBytes);
    }
    int logMessageBytesToRead = uncompressedBatchInputStream.readInt();
    byte[] logMessageBytes = new byte[logMessageBytesToRead];
    uncompressedBatchInputStream.readFully(logMessageBytes);
    messagesToRead--;
    if (messagesToRead == 0) {
      tryAndSendAuditMessage();
    }
    metricRegistry.counter(MESSAGES_PROCESSED_COUNTER_KEY).inc();
    metricRegistry.meter(BYTES_PROCESSED_METER_KEY).mark(logMessageBytesToRead);
    MemqLogMessage<K, V> logMessage = new MemqLogMessage<>(messageId, headers,
        headerDeserializer.deserialize(keyBytes), valueDeserializer.deserialize(logMessageBytes), messagesToRead == 0);
    populateInternalFields(writeTimestamp, logMessage);
    currentMessageOffset++;
    return logMessage;
  }

  private void tryAndSendAuditMessage() throws IOException {
    // send audit message if the header and auditor are present
    // note that this logic basically represents that we send the message hash after
    // we have read all LogMessages from a given Message within a batch
    if (auditor != null && messageIdHash != null) {
      if (header != null) {
        auditor.auditMessage(cluster.getBytes(MemqUtils.CHARSET), topic.getBytes(MemqUtils.CHARSET),
            header.getProducerAddress(), header.getProducerEpoch(), header.getProducerRequestId(),
            messageIdHash, auditedMessageCount, false, clientId);
      } else {
        logger.warning("Header is null, we can't send audit");
      }
    }
  }

  private void populateInternalFields(long writeTimestamp, MemqLogMessage<K, V> logMessage) {
    logMessage.setWriteTimestamp(writeTimestamp);
    logMessage.setMessageOffsetInBatch(currentMessageOffset);
    try {
      logMessage.setNotificationPartitionId(notificationPartitionId);
      logMessage.setNotificationPartitionOffset(notificationPartitionOffset);
      logMessage.setNotificationReadTimestamp(notificationReadTimestamp);
    } catch (Exception e) {
    }
  }

  protected Map<String, byte[]> deserializeHeaders(short headerLength,
                                                   DataInputStream input) throws IOException {
    Map<String, byte[]> map = new HashMap<>();
    while (headerLength > 0) {
      short keyLength = input.readShort();
      byte[] key = new byte[keyLength];
      input.readFully(key);
      short valueLength = input.readShort();
      byte[] value = new byte[valueLength];
      input.readFully(value);
      map.put(new String(key), value);
      headerLength -= 4 + keyLength + valueLength;
    }
    return map;
  }

  @Override
  public void remove() {
    // do nothing
  }

  @Override
  public void forEachRemaining(Consumer<? super MemqLogMessage<K, V>> action) {
    while (hasNext()) {
      action.accept(next());
    }
  }

  @Override
  public void close() throws IOException {
    if (stream != null) {
      stream.close();
      stream = null;
    }
    if (uncompressedBatchInputStream != null) {
      uncompressedBatchInputStream.close();
      uncompressedBatchInputStream = null;
    }
  }
}

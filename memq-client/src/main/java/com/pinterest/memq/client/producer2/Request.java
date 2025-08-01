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
import com.pinterest.memq.client.commons.MemqMessageHeader;
import com.pinterest.memq.client.commons.audit.Auditor;
import com.pinterest.memq.client.commons2.MemoryAllocationException;
import com.pinterest.memq.client.commons2.MemqCommonClient;
import com.pinterest.memq.client.commons2.MemqPooledByteBufAllocator;
import com.pinterest.memq.client.commons2.network.ClosedConnectionException;
import com.pinterest.memq.client.commons2.retry.RetryStrategy;
import com.pinterest.memq.client.producer.MemqWriteResult;
import com.pinterest.memq.commons.protocol.RequestPacket;
import com.pinterest.memq.commons.protocol.RequestType;
import com.pinterest.memq.commons.protocol.ResponseCodes;
import com.pinterest.memq.commons.protocol.ResponsePacket;
import com.pinterest.memq.commons.protocol.WriteRequestPacket;
import com.pinterest.memq.core.utils.MemqUtils;
import com.pinterest.memq.core.utils.MiscUtils;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.CRC32;

/**
 * Request class is responsible for managing the lifecycle of a request.
 *
 * At a high level, it handles the following:
 * 1. ByteBuf allocation - it allocates a large ByteBuf of size maxPayloadSize that is sliced into 3 parts,
 * each with their own read/write indices:
 *    - RequestPacket header
 *    - WriteRequestPacket header
 *    - Payload ByteBuf
 * 2. Writing new messages to the request payload ByteBuf
 * 3. Sealing the request - this is done when the request is ready to be dispatched either via size or time threshold
 * 4. Dispatching the request - this is done by submitting a Dispatch task to the dispatcher executor
 * 5. Handling the response - this is done by the Dispatch task, which handles the response and resolves the result future
 * 6. Retrying the request - if the request fails due to a closed connection, it will retry the request based on the retry strategy
 * 7. Releasing resources - it releases the ByteBuf and request count + inflight memory permits when the request is done
 */
public class Request {
  private static final Logger logger = LoggerFactory.getLogger(Request.class);

  private final ExecutorService dispatcher;
  private final ScheduledExecutorService scheduler;
  private final MemqCommonClient client;
  private final RequestManager requestManager;
  private final String topic;
  private final int clientRequestId;
  private final int maxRequestSize;
  private final int lingerMs;
  private final long sendRequestTimeoutMs;
  private final RetryStrategy retryStrategy;
  private final boolean disableAcks;
  private final Compression compression;

  private final AtomicBoolean available = new AtomicBoolean(true);
  private final AtomicInteger activeWrites = new AtomicInteger(0);
  private final CompletableFuture<MemqWriteResult> resultFuture = new CompletableFuture<>();
  private final MetricRegistry metricRegistry;
  private volatile Future<?> timeDispatchTask;
  private volatile long startTime = System.currentTimeMillis();

  private volatile boolean dispatching = false;
  private final ByteBuf largeByteBuf;
  private final ByteBuf requestPacketHeaderByteBuf;
  private final ByteBuf writeRequestPacketHeaderByteBuf;
  private final ByteBuf payloadByteBuf;
  private OutputStream outputStream;
  private byte[] messageIdHash;
  private int messageCount;
  private MemqMessageHeader header = new MemqMessageHeader(this);

  private Counter sentBytesCounter;
  private Counter ackedBytesCounter;
  private Timer sendTimer;
  private Timer requestWriteTimer;
  private Timer dispatchTimer;
  private Counter successCounter;
  private Timer responseTimer;
  private Timer ackTimer;
  private RequestPacket requestPacket;

  public Request(ExecutorService dispatcher,
                 ScheduledExecutorService scheduler,
                 MemqCommonClient client,
                 RequestManager requestManager,
                 Semaphore requestCountPermits,
                 Semaphore inflightMemoryPermits,
                 String topic,
                 int clientRequestId,
                 int maxPayloadSize,
                 int lingerMs,
                 int maxBlockMs,
                 long sendRequestTimeoutMs,
                 RetryStrategy retryStrategy,
                 boolean disableAcks,
                 Compression compression,
                 MetricRegistry metricRegistry) throws IOException, MemoryAllocationException {
    this.dispatcher = dispatcher;
    this.scheduler = scheduler;
    this.client = client;
    this.requestManager = requestManager;
    this.topic = topic;
    this.clientRequestId = clientRequestId;
    this.maxRequestSize = maxPayloadSize;
    this.lingerMs = lingerMs;
    this.sendRequestTimeoutMs = sendRequestTimeoutMs;
    this.retryStrategy = retryStrategy;
    this.disableAcks = disableAcks;
    this.compression = compression;
    this.metricRegistry = metricRegistry;
    int bufferCapacity = getByteBufCapacity(maxRequestSize, compression);
    largeByteBuf = MemqPooledByteBufAllocator.buffer(bufferCapacity, bufferCapacity, maxBlockMs);
    requestPacketHeaderByteBuf = largeByteBuf.retainedSlice(0, RequestPacket.getHeaderSize());
    writeRequestPacketHeaderByteBuf = largeByteBuf.retainedSlice(RequestPacket.getHeaderSize(), WriteRequestPacket.getHeaderSize(RequestType.PROTOCOL_VERSION, topic));
    payloadByteBuf = largeByteBuf.retainedSlice(RequestPacket.getHeaderSize() + WriteRequestPacket.getHeaderSize(RequestType.PROTOCOL_VERSION, topic), bufferCapacity - requestPacketHeaderByteBuf.readableBytes() - writeRequestPacketHeaderByteBuf.readableBytes());
    requestPacketHeaderByteBuf.resetWriterIndex();
    writeRequestPacketHeaderByteBuf.resetWriterIndex();
    payloadByteBuf.resetWriterIndex();
    try {
      initializeOutputStream();
    } catch (IOException ioe) {
      // release bytebuf if exception happened to avoid bytebuf leaks
      largeByteBuf.release();
      throw ioe;
    }
    initializeMetrics();
    scheduleTimeBasedDispatch();
    // release request lock once the request is done
    resultFuture.handle((r, t) -> {
      requestCountPermits.release();
      inflightMemoryPermits.release(maxRequestSize);
      return null;
    });
  }

  private void initializeMetrics() {
    sentBytesCounter = metricRegistry.counter("requests.sent.bytes");
    ackedBytesCounter = metricRegistry.counter("requests.acked.bytes");
    responseTimer = MiscUtils.oneMinuteWindowTimer(metricRegistry, "requests.response.time");
    ackTimer = MiscUtils.oneMinuteWindowTimer(metricRegistry, "requests.acked.time");
    successCounter = metricRegistry.counter("requests.success.count");
    requestWriteTimer = metricRegistry.timer("requests.write.time");
    sendTimer = MiscUtils.oneMinuteWindowTimer(metricRegistry, "requests.send.time");
    dispatchTimer = MiscUtils.oneMinuteWindowTimer(metricRegistry, "requests.dispatch.time");
  }

  private void initializeOutputStream() throws IOException {
    OutputStream stream = new ByteBufOutputStream(payloadByteBuf);
    int headerLength = MemqMessageHeader.getHeaderLength();
    stream.write(new byte[headerLength]);
    if (compression != null) {
      outputStream = compression.getDecompressStream(stream);
    } else {
      outputStream = Compression.NONE.getDecompressStream(stream);
    }
  }

  private int getByteBufCapacity(int maxRequestSize, Compression compression) {
    return Math.max(maxRequestSize, compression.minBufferSize);
  }

  protected void scheduleTimeBasedDispatch() {
    if (lingerMs == 0) {
      return;
    }
    if (timeDispatchTask != null) {
      timeDispatchTask.cancel(true);
    }
    timeDispatchTask = scheduler.schedule(() -> {
      if (!Thread.interrupted()) {
        if (System.currentTimeMillis() - startTime >= lingerMs) {
          // if seal() returns true, the payload was sealed due to time threshold, so we should try to dispatch
          // if it was false, it means that a write has been initiated and sealed the payload, so the dispatching is on that write
          synchronized (this) {
            if (seal() && isReadyToUpload()) {
              tryDispatch();
            }
          }
        }
      }
    }, lingerMs, TimeUnit.MILLISECONDS);
  }

  public Future<MemqWriteResult> write(RawRecord record) throws IOException {
    int payloadSize = record.calculateEncodedLogMessageLength();
    activeWrites.getAndIncrement();
    try {
      if (!isAvailable()) {
        return null;
      }

      // synchronized to ensure bytebuf doesn't get out-of-order writes
      synchronized (payloadByteBuf) {
        if (payloadSize > payloadByteBuf.writableBytes()) {
          seal();
          return null;
        }
        try (Timer.Context ctx = requestWriteTimer.time()) {
          writeMemqLogMessage(record);
        } finally {
          record.recycle();
        }
      }

      if (lingerMs == 0) {
        seal();
      }

      return resultFuture;
    } finally {
      activeWrites.decrementAndGet();

      // In general, the last write needs to seal the request (close the door) and dispatch, unless the linger threshold was breached
      // 1. if the request is still available, it means that the dispatch criteria hasn't been met, so we don't dispatch
      // 2. if the request is not available (to write), but the batch is not ready to upload,
      //    it means that there is still an active write happening
      // 3. if the request is not available (to write) and there are no active writes after this current write,
      //    we can try to dispatch. tryDispatch might be invoked by the time dispatch task concurrently, and only one will
      //    proceed
      if (!isAvailable() && isReadyToUpload()) {
        tryDispatch();
      }
    }
  }

  // true if there are no active writes on this request
  protected boolean isReadyToUpload() {
    return activeWrites.get() == 0;
  }

  protected void tryDispatch() {
    if (!dispatching) {
      synchronized (this) {
        if (!dispatching) {
          dispatching = true;
          dispatch();
        }
      }
    }
  }

  public void dispatch() {
    try {
      outputStream.close();
    } catch (IOException e) {
      logger.warn("Failed to close output stream: ", e);
    }
    try {
      header.writeHeader(payloadByteBuf);
      int payloadSizeBytes = payloadByteBuf.readableBytes();
      if (payloadSizeBytes == 0) { // don't upload 0 byte payloads
        resultFuture.complete(new MemqWriteResult(clientRequestId, 0, 0, 0));
        return;
      }
      requestPacket = createWriteRequestPacket(payloadByteBuf.asReadOnly().retainedDuplicate());
      dispatcher.submit(new Dispatch(payloadSizeBytes));
      timeDispatchTask.cancel(true);
    } finally {
      payloadByteBuf.release();
    }
  }

  /**
   * Given the payload ByteBuf, this method returns a RequestPacket that can be sent to the broker
   * via the NetworkClient.
   *
   * The RequestPacket's payload is constructed by stitching together the 3 sliced ByteBufs which were derived from
   * a large ByteBuf during Request creation. This single CompositeByteBuf is the overall payload of the RequestPacket which
   * will be sent to the broker.
   *
   * @param payload the payloadByteBuf
   * @return RequestPacket that can be sent to the broker
   */
  public RequestPacket createWriteRequestPacket(ByteBuf payload) {
    CRC32 crc32 = new CRC32();
    crc32.update(payload.duplicate().nioBuffer());
    int checksum = (int) crc32.getValue();

    WriteRequestPacket writeRequestPacket = new WriteRequestPacket(disableAcks,
            topic.getBytes(), true, checksum, payload.duplicate());
    writeRequestPacket.writeHeader(writeRequestPacketHeaderByteBuf, RequestType.PROTOCOL_VERSION);
    RequestPacket requestPacket = new RequestPacket(RequestType.PROTOCOL_VERSION, clientRequestId, RequestType.WRITE,
            writeRequestPacket);
    requestPacket.writeHeader(requestPacketHeaderByteBuf, RequestType.PROTOCOL_VERSION);

    CompositeByteBuf finalCompositeByteBuf = PooledByteBufAllocator.DEFAULT.compositeBuffer();
    finalCompositeByteBuf.addComponent(true, requestPacketHeaderByteBuf);
    finalCompositeByteBuf.addComponent(true, writeRequestPacketHeaderByteBuf);
    finalCompositeByteBuf.addComponent(true, payloadByteBuf);
    requestPacket.setPreAllocOutBuf(finalCompositeByteBuf);
    return requestPacket;
  }

  public boolean isAvailable() {
    return available.get();
  }

  /**
   *
   * @return true if sealed by this call
   */
  public boolean seal() {
    return available.getAndSet(false);
  }

  public void flush() {
    if (this.seal() && isReadyToUpload()) { // the flush is the initiator of the dispatch
      this.tryDispatch();
    }
  }

  public void writeMemqLogMessage(RawRecord record) throws IOException {
    record.writeToOutputStream(outputStream);
    // record the messageId
    addMessageId(record.getMessageIdBytes());
    messageCount++;
  }

  protected void addMessageId(byte[] messageIdBytes) {
    if (messageIdBytes == null) {
      return;
    }
    messageIdHash = MemqUtils.calculateMessageIdHash(messageIdHash, messageIdBytes);
  }

  public short getVersion() {
    return 1_0_0;
  }

  public Compression getCompression() {
    return compression;
  }

  public int getMessageCount() {
    return messageCount;
  }

  public long getEpoch() {
    return requestManager.getProducer() != null ? requestManager.getProducer().getEpoch() : System.currentTimeMillis();
  }

  public int getClientRequestId() {
    return clientRequestId;
  }

  private void release(RequestPacket requestPacket) {
    try {
      requestPacket.release();
    } catch (IOException e) {
      logger.error("Failed to release request packet", e);
    }
  }

  protected class Dispatch implements Runnable {
    private final int payloadSizeBytes;
    private final int attempts;
    private final int redirects;
    private final long dispatchTimeoutMs;
    private final long dispatchTimestamp = System.currentTimeMillis();
    private long writeTimestamp;
    private int writeLatency;

    public Dispatch(int payloadSizeBytes) {
      this.payloadSizeBytes = payloadSizeBytes;
      this.attempts = 0;
      this.redirects = 0;
      this.dispatchTimeoutMs = sendRequestTimeoutMs;
    }

    protected Dispatch(int payloadSizeBytes, int attempts, int redirects,
                       long requestDeadline) {
      this.payloadSizeBytes = payloadSizeBytes;
      this.attempts = attempts;
      this.redirects = redirects;
      this.dispatchTimeoutMs = requestDeadline - dispatchTimestamp;
    }

    @Override
    public void run() {
      if (dispatchTimeoutMs < 0) {
        release(requestPacket);
        resolve(new TimeoutException("Request timed out before retry: " + attempts));
        return;
      }
      sentBytesCounter.inc(payloadSizeBytes);
      Timer.Context dispatchTime = dispatchTimer.time();
      try {
        writeTimestamp = System.currentTimeMillis();
        Timer.Context sendTime = sendTimer.time();
        Timer.Context responseTime = responseTimer.time();
        Timer.Context ackTime = ackTimer.time();
        requestPacket.getPreAllocOutBuf().retain();
        CompletableFuture<ResponsePacket> response = client.sendRequestPacketAndReturnResponseFuture(requestPacket, dispatchTimeoutMs);
        sendTime.stop();
        writeLatency = (int) (System.currentTimeMillis() - writeTimestamp);
        response
            .whenCompleteAsync((responsePacket, throwable) -> {
              try {
                if (throwable != null) {
                  handleException(throwable);
                } else {
                  handleResponse(responsePacket, responseTime, ackTime);
                }
              } finally {
                try {
                  requestPacket.release();
                  if (responsePacket != null) {
                    responsePacket.release();
                  }
                } catch (IOException e) {
                  logger.warn("Failed to release packets", e);
                }
              }
            }, dispatcher);
      } catch (Exception e) {
        try {
          requestPacket.release();
        } catch (IOException ioe) {
          logger.warn("Failed to release packets", ioe);
        }
        logger.error("Failed to send request " + clientRequestId, e);
        resolve(e);
      } finally {
        dispatchTime.stop();
      }
    }

    protected void handleException(Throwable throwable) {
      if (throwable instanceof ClosedConnectionException) {
        Duration nextRetryIntervalDuration = retryStrategy.calculateNextRetryInterval(attempts); // if the next interval is invalid, fail the result future
        if (nextRetryIntervalDuration == null || dispatchTimeoutMs <= nextRetryIntervalDuration.toMillis()) {
          resolve(new TimeoutException("Request timed out after " + sendRequestTimeoutMs + " ms and " + attempts + " retries : " + throwable.getMessage()));
        } else {
          logger.warn(throwable.getMessage() + ", retrying request " + clientRequestId + " after " + nextRetryIntervalDuration.toMillis() + " ms");
          requestPacket.retry(); // retain the bytebuf since the finally clause in this Dispatch will release the local refCnt
          try {
            scheduler.schedule(() -> {
              try {
                dispatcher.submit(new Dispatch(payloadSizeBytes, attempts + 1, redirects, dispatchTimeoutMs + dispatchTimestamp));
              } catch (Exception e) {
                release(requestPacket);
                resolve(e);
              }
            }, nextRetryIntervalDuration.toMillis(), TimeUnit.MILLISECONDS);
          } catch (Exception e) {
            release(requestPacket);
            resolve(e);
          }
        }
      } else if (throwable instanceof Exception) {
        Exception resultException = (Exception) throwable;
        while (resultException instanceof ExecutionException && resultException.getCause() instanceof Exception) {
          resultException = (Exception) resultException.getCause();
        }
        resolve(resultException);
      } else {
        logger.error("Failed to send request " + clientRequestId, throwable);
        resolve(throwable);
      }
    }

    protected void handleResponse(ResponsePacket responsePacket, Timer.Context responseTime, Timer.Context ackTime) {
      short responseCode = responsePacket.getResponseCode();
      responseTime.stop();
      switch (responseCode) {
        case ResponseCodes.OK:
          ackedBytesCounter.inc(payloadSizeBytes);
          sendAuditMessageIfAuditEnabled();
          int ackLatency = (int) (System.currentTimeMillis() - writeTimestamp);
          ackTime.stop();
          logger.debug("Request acked in:" + ackLatency + " " + clientRequestId);
          resolve(new MemqWriteResult(clientRequestId, writeLatency, ackLatency, payloadSizeBytes));
          break;
        case ResponseCodes.REDIRECT:
          if (redirects > 1) {
            resolve(new Exception("Write request failed after multiple attempts"));
            return;
          }
          try {
            client.reconnect(topic, false);
          } catch (Exception e) {
            resolve(e);
            return;
          }
          try {
            requestPacket.retry();
            dispatcher.submit(
                new Dispatch(
                    payloadSizeBytes,
                    attempts,
                    redirects + 1,
                    dispatchTimeoutMs + dispatchTimestamp
                )
            );
          } catch (Exception e) {
            logger.error("Error: ", e);
          }
          break;
        case ResponseCodes.BAD_REQUEST:
          resolve(new Exception("Bad request, id: " + clientRequestId));
          break;
        case ResponseCodes.NOT_FOUND:
          resolve(new Exception("Topic not found: " + topic));
          break;
        case ResponseCodes.INTERNAL_SERVER_ERROR:
          resolve(new Exception("Unknown server error: " + clientRequestId));
          break;
        case ResponseCodes.REQUEST_FAILED:
          resolve(new Exception("Request failed: " + clientRequestId));
          break;
        case ResponseCodes.SERVICE_UNAVAILABLE:
          resolve(new Exception("Server out of capacity: " + topic));
          break;
        default:
          resolve(new Exception("Unknown response code: " + responseCode));
          break;
      }
    }

    private void sendAuditMessageIfAuditEnabled() {
      Auditor auditor = requestManager.getProducer().getAuditor();
      if (auditor != null) {
        MemqProducer<?, ?> producer = requestManager.getProducer();
        try {
          auditor.auditMessage(producer.getCluster().getBytes(MemqUtils.CHARSET),
              topic.getBytes(MemqUtils.CHARSET), MemqUtils.HOST_IPV4_ADDRESS,
              getEpoch(), clientRequestId, messageIdHash, messageCount, true, "producer");
        } catch (IOException e) {
          logger.error("Failed to log audit record for topic:" + topic, e);
        }
      }
    }

    private void resolve(MemqWriteResult writeResult) {
      resultFuture.complete(writeResult);
      successCounter.inc();
      largeByteBuf.release();
    }

    private void resolve(Throwable e) {
      resultFuture.completeExceptionally(e);
      largeByteBuf.release();
    }

    public long getDeadline() {
      return dispatchTimeoutMs + dispatchTimeoutMs;
    }

  }
}

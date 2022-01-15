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
package com.pinterest.memq.client.producer.netty;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.CRC32;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.pinterest.memq.client.commons.Compression;
import com.pinterest.memq.client.commons2.MemqCommonClient;
import com.pinterest.memq.client.producer.MemqWriteResult;
import com.pinterest.memq.client.producer.TaskRequest;
import com.pinterest.memq.commons.protocol.RequestPacket;
import com.pinterest.memq.commons.protocol.RequestType;
import com.pinterest.memq.commons.protocol.ResponseCodes;
import com.pinterest.memq.commons.protocol.ResponsePacket;
import com.pinterest.memq.commons.protocol.WriteRequestPacket;
import com.pinterest.memq.core.utils.MemqUtils;

import io.netty.buffer.ByteBuf;

@SuppressWarnings("rawtypes")
public class MemqNettyRequest extends TaskRequest {

  private static int overrideDebugChecksum = 0;
  private static AtomicLong byteCounter = new AtomicLong();
  private static AtomicLong ackedByteCounter = new AtomicLong();
  private int messagePayloadSize;
  private Logger logger;
  private MemqNettyProducer producer;
  private AtomicInteger requestStatus = new AtomicInteger(-1);
  private int requestAckTimeout;
  private boolean debug;

  public MemqNettyRequest(String topicName,
                          long currentRequestId,
                          Compression compression,
                          Semaphore maxRequestLock,
                          boolean disableAcks,
                          int maxPayLoadBytes,
                          int ackCheckPollIntervalMs,
                          Map<Long, TaskRequest> requestMap,
                          MemqNettyProducer producer,
                          int requestAckTimeout,
                          boolean debug) throws IOException {
    super(topicName, currentRequestId, compression, maxRequestLock, disableAcks, maxPayLoadBytes,
        ackCheckPollIntervalMs, requestMap, requestAckTimeout);
    this.producer = producer;
    this.requestAckTimeout = requestAckTimeout;
    this.debug = debug;
    this.logger = LoggerFactory.getLogger(MemqNettyRequest.class);
  }

  public RequestPacket getWriteRequestPacket(ByteBuf payload) throws IOException {
    CRC32 crc32 = new CRC32();
    crc32.update(payload.duplicate().nioBuffer());
    int checksum = (int) crc32.getValue();
    if (debug && overrideDebugChecksum != 0) {
      checksum = overrideDebugChecksum;
    }
    messagePayloadSize = payload.readableBytes();
    byteCounter.accumulateAndGet(messagePayloadSize, (v1, v2) -> v1 + v2);

    WriteRequestPacket writeRequestPacket = new WriteRequestPacket(disableAcks,
        getTopicName().getBytes(), true, checksum, payload.retainedDuplicate());
    this.payloadOutputStream = null;
    logger.debug("Prepared request:" + clientRequestId);
    return new RequestPacket(RequestType.PROTOCOL_VERSION, getId(), RequestType.WRITE,
        writeRequestPacket);
  }

  @Override
  protected MemqWriteResult runRequest() throws Exception {
    MemqCommonClient commonClient = producer.getMemqCommonClient();
    long writeTs = System.currentTimeMillis();
    RequestPacket requestPacket = getWriteRequestPacket(buffer);
    buffer.release();
    return dispatchRequestAndReturnResponse(commonClient, writeTs, requestPacket, 0);
  }

  private MemqWriteResult dispatchRequestAndReturnResponse(MemqCommonClient networkClient,
                                                           long writeTs,
                                                           RequestPacket requestPacket,
                                                           int attempt) throws InterruptedException,
                                                                        ExecutionException,
                                                                        TimeoutException,
                                                                        Exception {
    Future<ResponsePacket> responseFuture = networkClient
        .sendRequestPacketAndReturnResponseFuture(requestPacket, requestAckTimeout);

    // not sure if the dispatch latencies matter
    // TODO add check for how long it takes between dispatch trigger and sync??
    logger.info("Dispatched:" + clientRequestId);
    int writeLatency = (int) (System.currentTimeMillis() - writeTs);
    boolean shouldRelease = true;
    try {
//      if (!disableAcks) {
      if (debug) {
        logger.warn("Waiting for ack:" + clientRequestId);
      }
      ResponsePacket responsePacket = responseFuture.get(requestAckTimeout, TimeUnit.MILLISECONDS);
      short responseCode = responsePacket.getResponseCode();
      switch (responseCode) {
      case ResponseCodes.OK:
        sendAuditMessageIfAuditEnabled();
        ackedByteCounter.accumulateAndGet(messagePayloadSize, (v1, v2) -> v1 + v2);
        int ackLatency = (int) (System.currentTimeMillis() - writeTs);
        logger.debug("Request acked in:" + ackLatency + " " + clientRequestId);
        return new MemqWriteResult(clientRequestId, writeLatency, ackLatency, messagePayloadSize);
      case ResponseCodes.REDIRECT:
        if (attempt > 1) {
          throw new Exception("Write request failed after multiple attempts");
        }
        networkClient.reconnect(getTopicName(), false);
        shouldRelease = false;
        return dispatchRequestAndReturnResponse(networkClient, writeTs, requestPacket, ++attempt);
      case ResponseCodes.BAD_REQUEST:
        throw new Exception("Bad request, id:" + clientRequestId);
      case ResponseCodes.NOT_FOUND:
        throw new Exception("Topic not found:" + getTopicName());
      case ResponseCodes.INTERNAL_SERVER_ERROR:
        throw new Exception("Unknown server error:" + clientRequestId);
      case ResponseCodes.REQUEST_FAILED:
        throw new Exception("Request failed:" + clientRequestId);
      case ResponseCodes.SERVICE_UNAVAILABLE:
        throw new Exception("Server out of capacity:" + getTopicName());
      default:
        throw new Exception("Unknown response code:" + responseCode);
      }
//      } else {
//        sendAuditMessageIfAuditEnabled();
//        return new MemqWriteResult(clientRequestId, writeLatency, -1, byteArrays.length);
//      }
    } finally {
      requestMap.remove(clientRequestId);
      if (shouldRelease) {
        requestPacket.release();
      }
    }
  }

  private void sendAuditMessageIfAuditEnabled() {
    if (producer.getAuditor() != null) {
      try {
        producer.getAuditor().auditMessage(producer.getCluster().getBytes(MemqUtils.CHARSET),
            getTopicName().getBytes(MemqUtils.CHARSET), MemqUtils.HOST_IPV4_ADDRESS,
            producer.getEpoch(), clientRequestId, messageIdHash, logmessageCount, true, "producer");
      } catch (IOException e) {
        logger.error("Failed to log audit record for topic:" + getTopicName(), e);
      }
    }
  }

  @VisibleForTesting
  public void setDebugEnabled() {
    debug = true;
  }

  public void setRequestStatus(int status) {
    requestStatus.set(status);
  }

  public int getRequestStatus() {
    return requestStatus.get();
  }

  @Override
  public long getEpoch() {
    return producer != null ? producer.getEpoch() : System.currentTimeMillis();
  }

  @VisibleForTesting
  public static long getByteCounter() {
    return byteCounter.get();
  }

  @VisibleForTesting
  public static long getAckedByteCounter() {
    return ackedByteCounter.get();
  }

  @VisibleForTesting
  public static void reset() {
    byteCounter.set(0);
    ackedByteCounter.set(0);
  }

  @VisibleForTesting
  public static void setOverrideDebugChecksum(int overrideDebugChecksum) {
    MemqNettyRequest.overrideDebugChecksum = overrideDebugChecksum;
  }
}

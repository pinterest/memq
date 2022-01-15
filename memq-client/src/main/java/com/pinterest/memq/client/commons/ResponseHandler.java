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

import java.util.Map;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.pinterest.memq.commons.protocol.ResponsePacket;

public class ResponseHandler {

  private static final Logger logger = LoggerFactory.getLogger(ResponseHandler.class);
  private Map<String, Consumer<ResponsePacket>> requestMap;

  public ResponseHandler() {
  }

  public void handle(ResponsePacket responsePacket) throws Exception {
    Consumer<ResponsePacket> consumer = requestMap
        .remove(MemqCommonClient.makeResponseKey(responsePacket));
    if (consumer != null) {
      consumer.accept(responsePacket);
    } else {
      // no handler for response skipping
      logger.error("No handler for request:" + responsePacket.getRequestType());
    }
  }

  public void setRequestMap(Map<String, Consumer<ResponsePacket>> requestMap) {
    this.requestMap = requestMap;
  }

}

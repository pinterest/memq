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
package com.pinterest.memq.core.rpc;

import com.pinterest.memq.commons.protocol.RequestType;

public abstract class Response {

  protected long clientRequestId;
  protected short responseCode;

  public Response(long clientRequestId, short responseCode) {
    this.clientRequestId = clientRequestId;
    this.responseCode = responseCode;
  }

  public abstract RequestType getRequestType();

  public long getClientRequestId() {
    return clientRequestId;
  }

  public void setClientRequestId(long clientRequestId) {
    this.clientRequestId = clientRequestId;
  }

  public short getResponseCode() {
    return responseCode;
  }

  public void setResponseCode(short responseCode) {
    this.responseCode = responseCode;
  }
  
  public abstract byte[] getSerializedBytes();

}
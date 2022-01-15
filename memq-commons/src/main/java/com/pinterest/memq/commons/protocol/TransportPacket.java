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
package com.pinterest.memq.commons.protocol;

public abstract class TransportPacket implements Packet {
  protected short protocolVersion;
  protected long clientRequestId;
  protected RequestType requestType;

  public TransportPacket() {

  }

  public TransportPacket(short protocolVersion, long clientRequestId,
                         RequestType requestType) {
    this.protocolVersion = protocolVersion;
    this.clientRequestId = clientRequestId;
    this.requestType = requestType;
  }

  public short getProtocolVersion() {
    return protocolVersion;
  }

  public void setProtocolVersion(short protocolVersion) {
    this.protocolVersion = protocolVersion;
  }

  public long getClientRequestId() {
    return clientRequestId;
  }

  public void setClientRequestId(long clientRequestId) {
    this.clientRequestId = clientRequestId;
  }

  public RequestType getRequestType() {
    return requestType;
  }

  public void setRequestType(RequestType requestType) {
    this.requestType = requestType;
  }
}

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
package com.pinterest.memq.core.utils;

import java.io.IOException;
import java.util.List;

import com.pinterest.memq.core.commons.Message;

public class CoreUtils {

  private CoreUtils() {
  }

  public static long makeAckKey(Message message) {
    return makeAckKey(message.getServerRequestId(), message.getClientRequestId());
  }

  public static long makeAckKey(long serverRequestId, long clientRequestId) {
    return serverRequestId * clientRequestId;
  }

  public static int batchSizeInBytes(List<Message> batch) {
    return batch.stream().mapToInt(b -> b.getBuf().writerIndex()).sum();
  }

  public static int batchChecksum(List<Message> messageBatchAsList) throws IOException {
    CrcProcessor proc = new CrcProcessor();
    for (Message message : messageBatchAsList) {
      message.getBuf().forEachByte(proc);
    }
    return proc.getChecksum();
  }

}
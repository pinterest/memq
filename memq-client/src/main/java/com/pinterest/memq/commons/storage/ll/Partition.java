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

import java.io.IOException;
import java.util.Properties;

import com.google.gson.JsonObject;

public interface Partition {
  
  public static final String SIZE = "size";
  public static final String OFFSET = "offset";
  public static final String TIMESTAMP = "timestamp";

  void init(Properties props) throws IOException;

  long getPartitionSizeInBytes();

  long write(JsonObject entry) throws IOException;

  JsonObject get(long offset);

  long earliestOffset();

  long latestOffset();

  int getPartitionId();

  void applyRetention(long earliestTimestamp, long maxSize);

  JsonObject next(long offset) throws OffsetOutOfRangeException;

}

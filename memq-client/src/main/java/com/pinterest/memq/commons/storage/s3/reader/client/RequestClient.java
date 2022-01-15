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
package com.pinterest.memq.commons.storage.s3.reader.client;

import software.amazon.awssdk.services.s3.model.GetObjectRequest;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import io.netty.buffer.ByteBuf;

public interface RequestClient extends Closeable {
  void initialize(Properties properties);
  InputStream tryObjectGet(GetObjectRequest request) throws IOException;
  ByteBuf tryObjectGetAsBuffer(GetObjectRequest request) throws IOException;
}

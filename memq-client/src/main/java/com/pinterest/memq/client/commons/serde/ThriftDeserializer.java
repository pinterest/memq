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
package com.pinterest.memq.client.commons.serde;

import java.util.Properties;

import org.apache.thrift.TBase;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;

import com.pinterest.memq.client.commons.Deserializer;

@SuppressWarnings("rawtypes")
public class ThriftDeserializer implements Deserializer<TBase> {

  public static final String TBASE_OBJECT_CONFIG = "tBaseObject";
  private TBase tbase;

  @Override
  public void init(Properties props) {
    if (!props.containsKey(TBASE_OBJECT_CONFIG)) {
      throw new RuntimeException("ThriftDeserializer must have TBASE_OBJECT_CONFIG");
    }
    tbase = (TBase) props.get(TBASE_OBJECT_CONFIG);
  }

  @Override
  public TBase deserialize(byte[] bytes) {
    try {
      TDeserializer tDeserializer = new TDeserializer();
      tDeserializer.deserialize(tbase, bytes);
      return tbase;
    } catch (TException e) {
      throw new RuntimeException(e);
    }
  }

}

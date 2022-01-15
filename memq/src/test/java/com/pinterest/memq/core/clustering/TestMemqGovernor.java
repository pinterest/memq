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
package com.pinterest.memq.core.clustering;

import static org.junit.Assert.assertEquals;

import java.io.FileNotFoundException;
import java.io.FileReader;

import org.junit.Test;

import com.google.gson.Gson;
import com.google.gson.JsonIOException;
import com.google.gson.JsonSyntaxException;
import com.pinterest.memq.commons.protocol.TopicConfig;

public class TestMemqGovernor {

  @Test
  public void testBackwardsCompatibility() throws JsonSyntaxException, JsonIOException,
                                           FileNotFoundException {
    Gson gson = new Gson();
    TopicConfig oldConf = gson.fromJson(new FileReader("src/test/resources/old.test_topic.json"),
        TopicConfig.class);
    TopicConfig newConf = gson.fromJson(new FileReader("src/test/resources/new.test_topic.json"),
        TopicConfig.class);
    assertEquals("customs3aync2", oldConf.getStorageHandlerName());
    assertEquals("customs3aync2", newConf.getStorageHandlerName());
  }

}

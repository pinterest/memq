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

import java.io.IOException;
import java.util.Map.Entry;
import java.util.Properties;

public class AuditorUtils {

  public static Properties extractAuditorConfig(Properties properties) throws IOException {
    Properties auditProps = new Properties();
    for (Entry<Object, Object> entry : properties.entrySet()) {
      String key = entry.getKey().toString();
      if (key.startsWith(CommonConfigs.AUDITOR_CONFIG_PREFIX)) {
        auditProps.put(key.replace(CommonConfigs.AUDITOR_CONFIG_PREFIX, ""), entry.getValue());
      }
    }
    return auditProps;
  }

}

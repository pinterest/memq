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
package com.pinterest.memq.commons.storage;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

import org.reflections.Reflections;

/**
 * Ideal inherited from:
 * https://github.com/srotya/sidewinder/blob/development/core/src/main/java/com/srotya/sidewinder/core/functions/FunctionTable.java
 *
 */
public abstract class StorageHandlerTable {

  private static final Logger logger = Logger.getLogger(StorageHandlerTable.class.getName());

  private static Map<String, Class<? extends StorageHandler>> handlerMap = new HashMap<>();

  static {
    findAndRegisterOutputHandlers(StorageHandlerTable.class.getPackage().getName());
  }

  public static void findAndRegisterOutputHandlers(String packageName) {
    Reflections reflections = new Reflections(packageName.trim());
    Set<Class<?>> annotatedClasses = reflections.getTypesAnnotatedWith(StorageHandlerName.class);
    for (Class<?> annotatedClass : annotatedClasses) {
      StorageHandlerName plugin = annotatedClass.getAnnotation(StorageHandlerName.class);
      if (plugin == null) {
        logger.severe("Plugin info null:" + plugin);
        continue;
      }
      registerStorageHandlerClassWithAlias(annotatedClass, plugin.name());
      registerStorageHandlerClassWithAlias(annotatedClass, plugin.previousName());
    }
  }
  
  @SuppressWarnings("unchecked")
  private static void registerStorageHandlerClassWithAlias(Class<?> annotatedClass, String alias) {
    if (alias == null || alias.isEmpty()) {
      logger.warning("Ignoring aggregation function:" + annotatedClass.getName());
      return;
    }
    if (handlerMap.containsKey(alias)) {
      logger.severe(
          "Output plugin alias '" + alias + "' already exists, " + annotatedClass.getName());
      System.exit(-1);
    }
    handlerMap.put(alias, (Class<? extends StorageHandler>) annotatedClass);
    logger
        .info("Registered output handler(" + annotatedClass.getName() + ") with alias:" + alias);
  }

  @SuppressWarnings("unchecked")
  public static Class<StorageHandler> getClass(String name) {
    return (Class<StorageHandler>) handlerMap.get(name);
  }

}

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

import com.pinterest.memq.commons.CloseableIterator;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SlidingTimeWindowArrayReservoir;
import com.codahale.metrics.Timer;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

public class MiscUtils {
  
  public static String getHostname() {
    String hostName;
    try {
      hostName = InetAddress.getLocalHost().getHostName();
      int firstDotPos = hostName.indexOf('.');
      if (firstDotPos > 0) {
        hostName = hostName.substring(0, firstDotPos);
      }
    } catch (Exception e) {
      // fall back to env var.
      hostName = System.getenv("HOSTNAME");
    }
    return hostName;
  }
  
  public static String getIP() throws UnknownHostException {
    return InetAddress.getLocalHost().getHostAddress();
  }

  public static Timer oneMinuteWindowTimer(MetricRegistry registry, String name) {
    return registry.timer(name, () -> new com.codahale.metrics.Timer(
        new SlidingTimeWindowArrayReservoir(1, TimeUnit.MINUTES)));
  }

  /**
   * Copied from:
   * https://github.com/srotya/sidewinder/blob/development/core/src/main/java/com/srotya/sidewinder/core/utils/MiscUtils.java
   * 
   * @param file
   * @return
   * @throws IOException
   */
  public static boolean delete(File file) throws IOException {
    if (file.isDirectory()) {
      // directory is empty, then delete it
      if (file.list().length == 0) {
        return file.delete();
      } else {
        // list all the directory contents
        String files[] = file.list();
        boolean result = false;
        for (String temp : files) {
          // construct the file structure
          File fileDelete = new File(file, temp);
          // recursive delete
          result = delete(fileDelete);
          if (!result) {
            return false;
          }
        }
        // check the directory again, if empty then delete it
        if (file.list().length == 0) {
          file.delete();
        }
        return result;
      }
    } else {
      // if file, then delete it
      return file.delete();
    }
  }
  
  public static void printAllLines(InputStream stream) throws IOException {
    BufferedReader br = new BufferedReader(new InputStreamReader(stream));
    String line = null;
    while ((line = br.readLine()) != null) {
      System.out.println(line);
    }
  }

  @SuppressWarnings("unchecked")
  public static <T> CloseableIterator<T> emptyCloseableIterator() {
    return (CloseableIterator<T>) EmptyCloseableIterator.EMPTY_CLOSEABLE_ITERATOR;
  }

  private static class EmptyCloseableIterator<T> implements CloseableIterator<T> {
    static EmptyCloseableIterator<Object> EMPTY_CLOSEABLE_ITERATOR = new EmptyCloseableIterator<>();
    @Override
    public void close() throws IOException {}
    // Copied from Collections.emptyIterator()
    public boolean hasNext() { return false; }
    public T next() { throw new NoSuchElementException(); }
    public void remove() { throw new IllegalStateException(); }
    @Override
    public void forEachRemaining(Consumer<? super T> action) {
      Objects.requireNonNull(action);
    }
  }

}
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
package com.pinterest.memq.client.examples;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.nio.file.Files;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.thrift.TBase;
import org.apache.thrift.TDeserializer;

import com.pinterest.memq.client.commons.ConsumerConfigs;
import com.pinterest.memq.client.commons.serde.ByteArrayDeserializer;
import com.pinterest.memq.client.consumer.MemqConsumer;
import com.pinterest.memq.commons.CloseableIterator;
import com.pinterest.memq.commons.MemqLogMessage;

public class ExampleMemqConsumerMultiThreaded {

  public static void main(String[] args) throws Exception {
    Properties props = new Properties();
    if (args.length < 1) {
      System.out.println("Please specify properties configuration files");
      System.exit(-1);
    }
    props.load(new FileInputStream(args[0]));

    String topic = props.getProperty("topic");
    String cluster = props.getProperty("cluster");
    String thriftClass = props.getProperty("thrift.class");
    String hostName = InetAddress.getLocalHost().getHostName();
    if (hostName.contains(".")) {
      hostName = hostName.substring(0, hostName.indexOf("."));
    }
    String tmp = "test_" + hostName + "_" + UUID.randomUUID().toString();
    String consumerId = props.getProperty("group.id", tmp);

    TBase base = Class.forName(thriftClass).asSubclass(TBase.class).newInstance();
    TDeserializer tDeserializer = new TDeserializer();

    // initialize group metrics
    String serverset = "/var/serverset/discovery.memq.prod." + cluster + ".prod";
    List<String> lines = Files.readAllLines(new File(serverset).toPath());
    String bootstrapServer = lines.get(0).split(":")[0];

    int nThreads = Integer.parseInt(props.getProperty("threads", "1"));

    ThreadPoolExecutor ex = new ThreadPoolExecutor(nThreads, nThreads, 1000, TimeUnit.SECONDS,
        new ArrayBlockingQueue<>(nThreads * 2), new ThreadPoolExecutor.CallerRunsPolicy());
    System.out.println("Running thread:" + nThreads);

    try {
      // initialize MemqConsumer using builder
      Properties properties = new Properties();
      properties.setProperty(ConsumerConfigs.CLUSTER, cluster);

      properties.setProperty(ConsumerConfigs.CLIENT_ID, consumerId);
      properties.setProperty(ConsumerConfigs.GROUP_ID, consumerId);
      properties.setProperty(
          ConsumerConfigs.NOTIFICATION_SOURCE_PROPS_PREFIX_KEY + "auto.offset.reset", "latest");
      properties.setProperty(ConsumerConfigs.DIRECT_CONSUMER, "false");
      properties.setProperty(ConsumerConfigs.BOOTSTRAP_SERVERS, bootstrapServer + ":9092");
      properties.put(ConsumerConfigs.KEY_DESERIALIZER_CLASS_KEY,
          ByteArrayDeserializer.class.getCanonicalName());
      properties.put(ConsumerConfigs.VALUE_DESERIALIZER_CLASS_KEY,
          ByteArrayDeserializer.class.getCanonicalName());
      // Use off-heap
      properties.put(ConsumerConfigs.USE_DIRECT_BUFFER_KEY, "true");

      @SuppressWarnings("resource")
      MemqConsumer<byte[], byte[]> mc = new MemqConsumer<>(properties);
      mc.subscribe(Collections.singleton(topic));
      while (true) {
        CloseableIterator<MemqLogMessage<byte[], byte[]>>it = mc.poll(Duration.ofSeconds(10));
        if (!it.hasNext()) {
          continue;
        }
        boolean submitted = false;
        while (!submitted) {
          try {
            ex.execute(() -> {
              int c = 0;
              try {
                while (it.hasNext()) {
                  MemqLogMessage<byte[], byte[]> msg = it.next();
                  byte[] value = msg.getValue();
                  c++;
                }
              } finally {
                try {
                  it.close();
                } catch (IOException ioe) {
                  ioe.printStackTrace();
                }
              }
              System.out.println("[" + Thread.currentThread().getName() + "] Read:" + c + " messages");
            });
            submitted = true;
          } catch (Exception e) {
            e.printStackTrace();
          }
        }
        mc.commitOffset();
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
    ex.shutdown();
    ex.awaitTermination(1000, TimeUnit.SECONDS);

  }

}

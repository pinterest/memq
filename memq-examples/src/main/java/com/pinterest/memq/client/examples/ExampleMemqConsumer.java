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
import java.net.InetAddress;
import java.nio.file.Files;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import org.apache.thrift.TBase;
import org.apache.thrift.TDeserializer;

import com.google.gson.Gson;
import com.pinterest.memq.client.commons.ConsumerConfigs;
import com.pinterest.memq.client.commons.serde.ByteArrayDeserializer;
import com.pinterest.memq.client.consumer.MemqConsumer;
import com.pinterest.memq.commons.CloseableIterator;
import com.pinterest.memq.commons.MemqLogMessage;

public class ExampleMemqConsumer {

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

    TDeserializer tDeserializer = null;
    TBase base = null;
    if (thriftClass != null) {
      tDeserializer = new TDeserializer();
      base = Class.forName(thriftClass).asSubclass(TBase.class).newInstance();
    }

    // initialize group metrics
    String serverset = "/tmp/testmemq";
    List<String> lines = Files.readAllLines(new File(serverset).toPath());
    String bootstrapServer = lines.get(0).split(":")[0];

    try {
      // initialize MemqConsumer using builder
      Properties properties = new Properties();
      properties.setProperty(ConsumerConfigs.CLUSTER, cluster);
      String consumerId = "test_" + hostName;
      properties.setProperty(ConsumerConfigs.CLIENT_ID, consumerId);
      properties.setProperty(ConsumerConfigs.GROUP_ID, consumerId);
      properties.setProperty(ConsumerConfigs.DIRECT_CONSUMER, "false");
      properties.setProperty(ConsumerConfigs.BOOTSTRAP_SERVERS, bootstrapServer + ":9094");
      properties.put(ConsumerConfigs.KEY_DESERIALIZER_CLASS_KEY,
          ByteArrayDeserializer.class.getCanonicalName());
      properties.put(ConsumerConfigs.VALUE_DESERIALIZER_CLASS_KEY,
          ByteArrayDeserializer.class.getCanonicalName());

      @SuppressWarnings("resource")
      MemqConsumer<byte[], byte[]> mc = new MemqConsumer<>(properties);
      mc.subscribe(Collections.singleton(topic));
      Gson gson = new Gson();
      long c = 0;
      long s = 0;
      while (true) {
        long latency = 0;
        try(CloseableIterator<MemqLogMessage<byte[], byte[]>> it = mc.poll(Duration.ofSeconds(10))) {
          while (it.hasNext()) {
            MemqLogMessage<byte[], byte[]> msg = null;
            msg = it.next();
            byte[] value = msg.getValue();
            if (base != null) {
              base.clear();
              tDeserializer.deserialize(base, value);
              System.out.println(gson.toJson(base));
            }
            if (value != null) {
              long tmp = System.nanoTime() - msg.getWriteTimestamp();
              if (tmp > latency) {
                latency = tmp;
              }
              s += value.length;
            }
            if (c % 20000 == 0) {
              System.out.println("Total:" + s / 1024 / 1024 + " Latency:" + (latency/1000_000));
            }
          }
        }
        mc.commitOffset();
      }
    } catch (Exception e) {
      e.printStackTrace();
    }

  }

}
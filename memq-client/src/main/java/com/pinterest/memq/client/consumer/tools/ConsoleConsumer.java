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
package com.pinterest.memq.client.consumer.tools;

import java.io.File;
import java.io.FileInputStream;
import java.time.Duration;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import com.pinterest.memq.client.commons.ConsumerConfigs;
import com.pinterest.memq.client.commons.serde.StringDeserializer;
import com.pinterest.memq.client.consumer.MemqConsumer;
import com.pinterest.memq.commons.MemqLogMessage;

public class ConsoleConsumer {

  public static void main(String[] args) throws Exception {
    String topicName = null;
    String configFile = null;

    Options options = new Options();
    options
        .addOption(Option.builder("config").argName("config").desc("Configuration properties file")
            .numberOfArgs(1).required().type(String.class).build());
    options.addOption(Option.builder("topic").argName("topic").desc("Memq Topic name")
        .numberOfArgs(1).required().type(String.class).build());
    CommandLineParser parser = new DefaultParser();
    try {
      // parse the command line arguments
      CommandLine line = parser.parse(options, args);
      configFile = line.getOptionValue("config");
      topicName = line.getOptionValue("topic");
    } catch (ParseException exp) {
      // oops, something went wrong
      System.err.println("Parsing failed.  Reason: " + exp.getMessage());
      System.exit(-1);
    }

    final AtomicBoolean loopControl = new AtomicBoolean(true);
    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        loopControl.set(false);
      }
    });

    Properties properties = new Properties();
    properties.load(new FileInputStream(new File(configFile)));
    properties.setProperty(ConsumerConfigs.KEY_DESERIALIZER_CLASS_KEY,
        StringDeserializer.class.getCanonicalName());
    properties.setProperty(ConsumerConfigs.VALUE_DESERIALIZER_CLASS_KEY,
        StringDeserializer.class.getCanonicalName());
    MemqConsumer<String, String> consumer = new MemqConsumer<>(properties);
    consumer.subscribe(Arrays.asList(topicName));

    while (loopControl.get()) {
      Iterator<MemqLogMessage<String, String>> records = consumer.poll(Duration.ofSeconds(10));
      while (records.hasNext() && loopControl.get()) {
        System.out.println(records.next());
      }
    }
    System.out.println("Closing memq consumer");
    consumer.close();
  }

}

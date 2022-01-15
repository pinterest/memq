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
package com.pinterest.memq.core.tools;

import java.io.File;
import java.nio.file.Files;
import java.util.concurrent.TimeUnit;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;

import com.google.gson.Gson;
import com.pinterest.memq.commons.protocol.TopicConfig;
import com.pinterest.memq.core.clustering.MemqGovernor;

public class TopicAdmin {

  private static final String ZOOKEEPER_CONNECTION = "zk";
  private static final String TOPIC_CONFIG_JSON_FILE = "tcjf";

  public static void main(String[] args) throws Exception {
    // create Options object
    Options options = new Options();

    // add t option
    
    options.addOption(Option.builder().required(true).hasArg().longOpt(ZOOKEEPER_CONNECTION)
        .argName("zookeeper connection string").build());
    options.addOption(Option.builder().required(true).hasArg().longOpt(TOPIC_CONFIG_JSON_FILE).argName("topic configuration json file").build());

    CommandLineParser parser = new DefaultParser();
    CommandLine cmd = null;

    try {
      cmd = parser.parse(options, args);
    } catch (Exception e) {
      printHelp(options);
      return;
    }

    String zookeeperConnectionString = cmd.getOptionValue(ZOOKEEPER_CONNECTION);
    String topicConfigFile = cmd.getOptionValue(TOPIC_CONFIG_JSON_FILE);
    RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
    CuratorFramework client = CuratorFrameworkFactory.newClient(zookeeperConnectionString,
        retryPolicy);
    client.start();
    client.blockUntilConnected(100, TimeUnit.SECONDS);
    Gson gson = new Gson();
    TopicConfig config = gson.fromJson(
        new String(Files.readAllBytes(new File(topicConfigFile).toPath())), TopicConfig.class);
    MemqGovernor.createTopic(client, config);
    client.close();
  }

  private static void printHelp(Options options) {
    HelpFormatter formatter = new HelpFormatter();
    formatter.printHelp("memqcli", options);
  }

}

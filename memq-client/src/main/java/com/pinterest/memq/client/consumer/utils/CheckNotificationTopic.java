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
package com.pinterest.memq.client.consumer.utils;

import java.io.FileInputStream;
import java.util.Properties;

import com.pinterest.memq.client.consumer.KafkaNotificationSource;

public class CheckNotificationTopic {

  public static void main(String[] args) throws Exception {
    Properties props = new Properties();
    props.load(new FileInputStream(args[0]));
    KafkaNotificationSource source = new KafkaNotificationSource(props);
    source.unsubscribe();
    long ts = System.currentTimeMillis();
    long[] offsetsForAllPartitions = source.getOffsetsForAllPartitions(true);
    for (int i = 0; i < offsetsForAllPartitions.length; i++) {
      long l = offsetsForAllPartitions[i];
      System.out.println("Partition:" + i + " Offset:" + l);
    }
    ts = System.currentTimeMillis() - ts;
    System.out.println("Ts:" + ts);
  }

}

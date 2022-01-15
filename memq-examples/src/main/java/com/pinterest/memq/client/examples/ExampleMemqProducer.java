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
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import com.pinterest.memq.client.commons.Compression;
import com.pinterest.memq.client.commons.serde.ByteArraySerializer;
import com.pinterest.memq.client.producer.MemqWriteResult;
import com.pinterest.memq.client.producer2.MemqProducer;

public class ExampleMemqProducer {

  public static void main(String[] args) throws IOException, InterruptedException,
                                         ExecutionException {
    int nThreads = 1;
    if (args.length > 0) {
      nThreads = Integer.parseInt(args[0]);
    }
    ExecutorService es = Executors.newFixedThreadPool(nThreads, new ThreadFactory() {

      @Override
      public Thread newThread(Runnable r) {
        Thread th = new Thread(r);
        th.setDaemon(true);
        return th;
      }
    });

    String pathname = "/tmp/memq_serverset";
    PrintWriter pr = new PrintWriter(new File(pathname));
    String s = "{\"az\": \"us-east-1a\", \"ip\": \"127.0.0.1\", \"port\": \"8080\", \"stage_name\": \"prototype\", \"version\": \"none\", \"weight\": 1}";
    pr.println(s);
    pr.close();
    for (int x = 0; x < nThreads; x++) {
      final int p = x;
      es.submit(() -> {
        try {
          String topicName = "test";
          MemqProducer<byte[], byte[]> instance = new MemqProducer.Builder<byte[], byte[]>()
              .disableAcks(false).keySerializer(new ByteArraySerializer())
              .valueSerializer(new ByteArraySerializer()).topic(topicName).cluster("local")
              .compression(Compression.ZSTD)
              .bootstrapServers("127.0.0.1:9094").build();
          StringBuilder builder = new StringBuilder();
          while (builder.length() < 1024 * 512) {
            builder.append(UUID.randomUUID().toString());
          }

          byte[] bytes = builder.toString().getBytes("utf-8");
          for (int i = 0; i < 5000; i++) {
            long ts = System.currentTimeMillis();
            List<Future<MemqWriteResult>> result = new ArrayList<>();
            for (int k = 0; k < 30; k++) {
              Future<MemqWriteResult> writeToTopic = instance.write(null, bytes, System.nanoTime());
              result.add(writeToTopic);
            }
            instance.flush();
            for (Future<MemqWriteResult> future : result) {
              future.get();
            }
            ts = System.currentTimeMillis() - ts;
            System.out.println(ts + "ms");
          }
        } catch (Exception e) {
          e.printStackTrace();
        }
      });
    }
    es.awaitTermination(1000, TimeUnit.SECONDS);
  }

}

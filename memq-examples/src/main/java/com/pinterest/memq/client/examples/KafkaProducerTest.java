package com.pinterest.memq.client.examples;

import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;

public class KafkaProducerTest {

  public static void main(String[] args) throws InterruptedException, ExecutionException {
    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    props.put(ProducerConfig.ACKS_CONFIG, "-1");
    props.put(ProducerConfig.RETRIES_CONFIG, "3");
    props.put(ProducerConfig.LINGER_MS_CONFIG, "0");
    props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "none");
    KafkaProducer<byte[], byte[]> producer = new KafkaProducer<byte[], byte[]>(props);
    StringBuilder builder = new StringBuilder();
    for (int i = 0; i < 50; i++) {
      builder.append(UUID.randomUUID().toString());
    }
    byte[] bytes = builder.toString().getBytes();
    System.out.println(bytes.length);
    while (true) {
      long ts = System.nanoTime();
      producer.send(new ProducerRecord<byte[], byte[]>("test", bytes)).get();
      System.out.println((System.nanoTime() - ts) / (1000)+"us");
    }
  }

}

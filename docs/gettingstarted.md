# Getting Started

## How to build?

Pre-requisites:
1. JDK 8+
2. Maven 3+

```
git clone https://github.com/pinterest/memq.git
cd memq
mvn clean package -DskipTests
```

## How to deploy?
Please read detailed deployment instructions [here](deploy.md)

## Running locally
We have a ready to run local example in `deploy` directory where you can run a local version of MemQ.

```
# assuming you are in the memq code base and you have already run maven build
cd deploy
# download and run zookeeper and kafka 2.4.1
./startlocalkafka.sh # non blocking once running; to stop use ./stoplocalkafka.sh
# run memq in nonclustered mode, you can change this to run in clustered mode locally but multiple instances cannot be deployed on the same host 
./run.sh
```

### Creating Topic
Topic creation in MemQ is at least a 2 step process. During the setup below we will assume the following:

1. anything in `<>` below needs to be customized / substituted
2. your_topic_name refers to the name of the topic
3. AWS S3 is the storage engine and Kafka is the notification system
3. your_bucket refers to your s3 bucket
4. input traffic is 100MB/s (you can change but please have sufficient capacity available)

Pre-requisites:
- Running MemQ cluster
- Kafka CLI
- ZK CLI (if using MemQ in clustered mode)
- AWS S3 and IAM access for broker and consumer machines

*Step 1: Create notification topic in Kafka*
```
kafka-topics.sh --create --partitions <number_of_read_partitions> --replication-factor 6 --topic <your_notification_topic_name> --bootstrap-server <kafkabroker:9092>
```
Note that you can reduce the replication factor based on your Kafka deployment. We recommend RF6 for highest availability.

**Clustered mode**

*Step 2: Create MemQ topic using ZK*
Requires creating the MemQ topic json and then creating the znode.

Below is sample topic JSON:

```
{"topicOrder":1612218199565,"bufferSize":2097152,"ringBufferSize":512,"batchMilliSeconds":20000,"batchSizeMB":15,"outputParallelism":60,"maxDispatchCount":200,"topic":"<your_topic_name>","outputHandlerConfig":{"delay.max.millis":"10000","notificationServerset":"/var/serverset/<mykafkaclusterbrokersfile>","delay.min.millis":"200","disableNotifications":"false","notificationBrokerset":"Capacity_B9_P180_0","retryTimeoutMillis":"3000","notificationTopic":"<your_notification_topic_name>","retries":"3","region":"us-east-1","bucket":"<your_s3_bucket>","path":"<your_topic_s3_prefix>"},"tickFrequencyMillis":1000,"enableBucketing2Processor":true,"outputHandler":"customs3aync2","inputTrafficMB":100.0,"enableServerHeaderValidation":true}
```

```
zkCli.sh -server <yourzk:2181> create /<memq cluster base>/topics/<topicname> "<topicjson>"
```

**Non-clustered mode**
In non-clustered mode MemQ brokers can be directly bootstrapped with the topic you are using. Non-clustered mode means each broker is running independently without any balancing features. This can be thought of analogous to a stateless APIs service.

```
logging:
  level: INFO
  appenders:
    - type: file
      currentLogFilename: /var/log/memq/memq.log
      archivedLogFilenamePattern: /var/log/memq/memq-%i.log.gz
      archivedFileCount: 10
      maxFileSize: 100MiB
defaultBufferSize: 2097152
defaultSlotTimeout: 300000
cluster: false
resetEnabled: false
openTsdbConfig:
  host: 127.0.0.1
  port: 18126
  frequencyInSeconds: 60
awsUploadRatePerStreamInMB: 10
topicConfig:
    - topic: <your_topic_name>
      ringBufferSize: 512
      outputParallelism: 30
      batchSizeMB: 15
      batchMilliSeconds: 20000
      enableBucketing2Processor: true
      outputHandler: customs3aync2
      outputHandlerConfig:
        retryTimeoutMillis: 2000
        disableNotifications: false
        bucket: <your_s3_bucket>
        path: <your_notification_topic_s3_prefix>
        region: us-east-1
        notificationServerset: /var/serverset/<mykafkaclusterbrokersfile>
        notificationTopic: <your_notification_topic_name>
```

**[If using auditing]**

*Step 3: Create auditing topic*
The auditing topic enables producers and consumers to send audit messages which enables the auditor (if deployed) to check e2e message loss and latencies.

```
kafka-topics.sh --create --partitions <number_of_read_partitions> --replication-factor 3 --topic <your_notification_topic_name> --bootstrap-server <kafkabroker>:9092
```

Note: Auditing is configured on producer and consumer. The auditing system is completely transparent to the brokers.  

## Producing Data
MemQ producer APIs can be used to send data to MemQ. Note that depending on the configuration of the Topic and the Storage system, the producer latencies can be high (>30s). Please make sure that your application is designed to handle this case else it will create cascading back pressure.

Note: specific Producer method docs can be found in `com.pinterest.memq.client.producer2.MemqProducer`

A pre-optimized MemQ producer is available in [Singer](https://github.com/pinterest/singer)

Producer ack level can be configured in 3 modes:
- none (by skipping any blocking future calls after send is invoked)
- disableAcks = false (acknowledges once data is written to broker)
- disableAcks = true (acknowledges once data is written to storage system)

The snippet below illustrates on how to do this.  

```
import com.pinterest.memq.client.commons.serde.ByteArraySerializer;
import com.pinterest.memq.client.producer2.MemqProducer;
import java.util.Arrays;
import java.util.concurrent.Future;

//  your class / method stub.....

MemqProducer.Builder<byte[], byte[]> builder = new MemqProducer.Builder<>();
    builder
        .keySerializer(new ByteArraySerializer())
        .valueSerializer(new ByteArraySerializer())
        .topic("test")
        .sendRequestTimeout(10000)
        .bootstrapServers("mymemqbroker:9092")
        .cluster("pii_dev01");

MemqProducer<byte[], byte[]> producer = builder.build(); // create producer instance
Future<?> f = producer.write("test".getBytes(), "test".getBytes()); // write message
producer.flush(); // force flush to trigger a send to broker; without this the producer will wait until it has sufficient data
f.get(); // block until the send call succeeds; note that this will depend on what level of ack is being used: disableAcks=true or false
producer.close(); // close producer once you are done writing data
```


## Consuming Data
The currently open sourced implementation of MemQ uses Kafka as a the notification source. Kafka therefore currently also provides consumer group / consumer clustering functionality in MemQ and the behaviors are identical (group rebalance, assignment etc.). For the version of Kafka used please refer to the pom.xml for the MemQ release you are using.

The following snippet should help you get started with developing a MemQ consumer. The MemQ consumer's API are very similar to Kafka with some differences when it comes to low level implementation and certain additional functionalities.

Note: The core APIs for MemQ consumer are limited to poll, seek and fetch. Specific method docs can be found in `com.pinterest.memq.client.consumer.MemqConsumer` class

```
Properties properties = new Properties();
properties.setProperty(ConsumerConfigs.CLUSTER, "clustername");// this is used for auditing
properties.setProperty(ConsumerConfigs.CLIENT_ID, "changeme"); // used for metrics
properties.setProperty(ConsumerConfigs.GROUP_ID, "changeme");  // used for consumer group (same as kafka)
properties.setProperty(ConsumerConfigs.DIRECT_CONSUMER, "false"); // advanced setting leave as is
properties.setProperty(ConsumerConfigs.BOOTSTRAP_SERVERS, "mymemqbroker:9092"); // bootstrap server (hostname or ip address of a MemQ broker)
properties.put(ConsumerConfigs.KEY_DESERIALIZER_CLASS_KEY,
    ByteArrayDeserializer.class.getCanonicalName()); // serializer for key
properties.put(ConsumerConfigs.VALUE_DESERIALIZER_CLASS_KEY,
    ByteArrayDeserializer.class.getCanonicalName()); // serializer for value
MemqConsumer<byte[], byte[]> mc = new MemqConsumer<>(properties); // initialize the consumer with these properties
mc.subscribe(Collections.singleton("my_memq_topic")); // subscribe to the topic 
while (true) {
  Iterator<MemqLogMessage<byte[], byte[]>> it = mc.poll(Duration.ofSeconds(10)); // start polling messages
  while (it.hasNext()) {
    MemqLogMessage<byte[], byte[]> msg = it.next();
    byte[] key = msg.getKey();
    byte[] value = msg.getValue();
    
    // do something with this data ......
  }
}
```
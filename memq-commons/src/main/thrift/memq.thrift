namespace java com.pinterest.memq.commons

struct LogRequest {
  1: required string kafkaClusterSignature;
  2: required list<string> brokerLists;
  3: required string acks = "1";
  4: optional string partitionerClass;
  5: optional string keySerializerClass = "org.apache.kafka.common.serialization.ByteArraySerializer";
  6: optional string valueSerializerClass = "org.apache.kafka.common.serialization.ByteArraySerializer";
  7: optional string compressionType;
  8: optional i32 maxRequestSize = 1000000;
  9: optional bool sslEnabled = 0;
 10: optional map<string, string> sslSettings;
 11: optional bool transactionEnabled = 0;
 12: optional i32 transactionTimeoutMs = 6000;
 13: optional i32 retries = 5;
 14: optional i32 bufferMemory = 33554432;
}
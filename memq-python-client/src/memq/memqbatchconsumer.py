'''
Created on Feb 3, 2021

@author: ambudsharma
'''

import boto3
from memq.memqlogmessageiterator import MemqLogMessageIterator, Deserializer, \
    MemqLogMessage
from memq.inputstream import DataInputStream


class MemqBatchConsumerV1(object):
    '''
    classdocs
    '''
    # generate static shard path combinations
    bucket_shard_paths = []
    for i in range(16):
            for j in range(16):
                bucket_shard_paths.append(format(i, 'x') + format(j, 'x'))

    def __init__(self, cluster, bucket, topic):
        '''
        Constructor
        '''
        self.cluster = cluster
        self.bucket = bucket
        self.topic = topic
        
        self.s3 = boto3.client('s3')
        
    def fetch_objects(self):
        i = '00'
        response = self.s3.list_objects(Bucket=self.bucket, Prefix=i + "/" + self.cluster + "/topics/" + self.topic)
        if response['ResponseMetadata'] == None or response['ResponseMetadata']['HTTPStatusCode'] != 200:
            raise Exception("Failed request:" + response)
        
        objects = response['Contents']
        self.keys = []
        self.index = 0
        for o in objects:
            self.keys.append(o['Key'])        
        # for i in self.bucket_shard_paths:
        #   print("s3://" + bucket + "/" + i + "/" + cluster + "/topics/" + topic)

    def has_next(self) -> bool:
        return self.index < self.keys.__len__()
    
    def poll(self) -> MemqLogMessageIterator:
        key = self.keys[self.index]
        print("Fetching:" + key)
        response = self.s3.get_object(Bucket=self.bucket, Key=key)
        self.index += 1
        stream = DataInputStream(response['Body'])
        curr_notification_obj = { 
            MemqLogMessage.INTERNAL_FIELD_TOPIC:"test",
            MemqLogMessage.INTERNAL_FIELD_OBJECT_SIZE:response['ContentLength'],
            MemqLogMessage.INTERNAL_FIELD_NOTIFICATION_PARTITION_ID:0,
            MemqLogMessage.INTERNAL_FIELD_NOTIFICATION_READ_TIMESTAMP:0,
            MemqLogMessage.INTERNAL_FIELD_NOTIFICATION_PARTITION_OFFSET:0  
        }
        return MemqLogMessageIterator(self.cluster, stream, curr_notification_obj, Deserializer(), Deserializer())

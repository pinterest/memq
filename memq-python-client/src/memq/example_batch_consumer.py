from memq.memqbatchconsumer import MemqBatchConsumerV1

class ExampleBatchConsumer:

    def run(self, cluster, bucket, topic):
        consumer = MemqBatchConsumerV1(cluster=cluster, bucket=bucket, topic=topic)
        consumer.fetch_objects()
        while consumer.has_next():
            itr = consumer.poll()
            while itr.has_next():
                message = itr.next()
                print(str(len(message.value)) + " headers:" + str(message.write_timestamp))

if __name__ == "__main__":
    import sys;sys.argv = [1:]
    ExampleBatchConsumer.run()

#!/bin/bash
DIR=$(dirname "${BASH_SOURCE[0]}") 
cd $DIR
source ./common.sh

# make target directory so the artifacts can be ignored by git
mkdir -p target
cd target

# download kafka
rm -rf kafka_2.11-2.4.1
if [[ ! -f kafka_2.11-2.4.1.tgz ]]; then
	wget https://archive.apache.org/dist/kafka/2.4.1/kafka_2.11-2.4.1.tgz
fi
tar xf kafka_2.11-2.4.1.tgz


cd kafka_2.11-2.4.1
echo "Starting zookeeper in background"
bin/zookeeper-server-start.sh -daemon config/zookeeper.properties
waitPort 2181
echo "Starting kafka in background"
bin/kafka-server-start.sh -daemon config/server.properties
waitPort 9092
echo "Creating TestTopicNotifications"
bin/kafka-topics.sh --zookeeper localhost:2181 --topic TestTopicNotifications --partitions 3 --replication-factor 1 --create
mkdir -p /tmp/serverset
echo "localhost:9092" > /tmp/serverset/notification.serverset
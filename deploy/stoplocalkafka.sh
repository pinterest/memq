#!/bin/bash
DIR=$(dirname "${BASH_SOURCE[0]}") 
cd $DIR
source ./common.sh

waitPort 2181
waitPort 9092
cd target
cd kafka_2.11-2.4.1
bin/kafka-server-stop.sh
sleep 2
bin/zookeeper-server-stop.sh
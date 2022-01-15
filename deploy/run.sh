#!/bin/bash

if [  -z $HEAP_MEMORY_GB ];then
	export HEAP_MEMORY_GB=1
fi

# Set JAVA_HOME
JAVA_HOME=${JAVA_HOME:/usr/lib/jvm/openjdk8}

#export MEM_KB=$(grep MemTotal /proc/meminfo | /usr/bin/awk '{print $2}')
export MEM_KB=$(grep MemTotal /proc/meminfo | tr ' ' '\n' | tail -n2 | head -n1) 
export MEM_GB=$((MEM_KB/1024/1024))
export LOG_DIR=/var/log/memq

mkdir p $LOG_DIR

if [ -z $MAX_DIRECT_MEMORY ]; then
	echo "Total memory $MEM_GB"
	export MAX_DIRECT_MEMORY=$((MEM_GB - HEAP_MEMORY_GB - 4))g
fi

if [ -n "$ENABLE_NMT" ]; then
  echo "Enabling Java NativeMemoryTracking=${ENABLE_NMT}"
  export ADDITIONAL_JAVA_OPTS="-XX:NativeMemoryTracking=${ENABLE_NMT}"
fi

echo "Launching Memq with HEAP=$HEAP_MEMORY_GB MaxDirectMemorySize=$MAX_DIRECT_MEMORY"
$JAVA_HOME/bin/java -Xms${HEAP_MEMORY_GB}g -Xmx${HEAP_MEMORY_GB}g -XX:MaxDirectMemorySize=${MAX_DIRECT_MEMORY} \
	 -verbosegc -Xloggc:${LOG_DIR}/gc.log ${ADDITIONAL_JAVA_OPTS} \
	-XX:+UseGCLogFileRotation -XX:NumberOfGCLogFiles=100 -XX:GCLogFileSize=2M \
	-XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+PrintGCDateStamps -XX:+PrintClassHistogram \
	-XX:+UseG1GC -XX:MaxGCPauseMillis=250 -XX:G1ReservePercent=10 -XX:ConcGCThreads=4 \
	-XX:ParallelGCThreads=4 -XX:G1HeapRegionSize=32m -XX:InitiatingHeapOccupancyPercent=70 \
	-XX:ErrorFile=${LOG_DIR}/jvm_error.log \
	-Dsun.net.spi.nameservice.provider.1=dns,sun \
	-jar target/memq.jar server configs/nonclustered.yaml
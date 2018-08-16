#!/bin/bash

cur=/data/cpc/xuxiang
SPARK_HOME=/usr/lib/spark-current
queue=root.cpc
#queue=root.develop.adhoc.cpc
#queue=root.production.biz.cpc2

jars=(
    "$cur/lib/mysql-connector-java-5.1.41-bin.jar"
    "$cur/lib/hadoop-lzo-0.4.20.jar"
    "$cur/lib/config-1.2.1.jar"
)

$SPARK_HOME/bin/spark-submit --master yarn --queue $queue \
    --conf 'spark.port.maxRetries=100' \
    --executor-memory 24g --driver-memory 8g \
    --executor-cores 8 --num-executors 30  \
    --conf 'spark.yarn.executor.memoryOverhead=4g'\
    --conf 'spark.dynamicAllocation.maxExecutors=50'\
    --jars $( IFS=$','; echo "${jars[*]}" ) \
    --class com.cpc.portrait.snapshotsample.GenDownloadTag \
    target/scala-2.11/cpc-portrait-assembly-1.0.0.jar $1




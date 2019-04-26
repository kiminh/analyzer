#!/bin/bash

cur=/data/cpc/anal
SPARK_HOME=/usr/lib/spark-current
queue=root.cpc.develop
#date=`date +"%Y-%m-%d" -d "-1day"`
day=$1
hour=$2

jars=(
    "$cur/lib/mysql-connector-java-5.1.41-bin.jar"
    "$cur/lib/mariadb-java-client-1.5.9.jar"
    "$cur/lib/config-1.2.1.jar"
    "$cur/lib/hadoop-lzo-0.4.20.jar"
    "$cur/lib/scala-redis_2.11-1.0.jar"
    "$cur/lib/spark-tensorflow-connector_2.11-1.10.0.jar"

)

$SPARK_HOME/bin/spark-submit --master yarn --queue $queue \
    --conf 'spark.port.maxRetries=100' \
    --executor-memory 12g --driver-memory 4g \
    --executor-cores 4 --num-executors 50  \
    --conf 'spark.yarn.executor.memoryOverhead=5g'\
    --conf 'spark.dynamicAllocation.maxExecutors=100'\
    --conf 'spark.sql.shuffle.partitions=2000'\
    --jars $( IFS=$','; echo "${jars[*]}" ) \
    --conf "spark.sql.shuffle.partitions=1000" \
    --class com.cpc.spark.novel.toutiao.DspTitleSample \
    /home/cpc/wy/analyzer/target/scala-2.11/cpc-anal_2.11-0.1.jar 2019-04-25
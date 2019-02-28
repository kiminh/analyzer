#!/bin/bash

#sh testOcpcForGetPb.sh model.OcpcGetPb 2019-01-02 12 hottopic_v1 hottopic 1

cur=/data/cpc/anal
SPARK_HOME=/usr/lib/spark-current
queue=root.cpc.develop

date=$1
hour=$2
version=$3
media=$4
isKnown=$5

jars=(
    "$cur/lib/mysql-connector-java-5.1.41-bin.jar"
    "$cur/lib/hadoop-lzo-0.4.20.jar"
    "$cur/lib/config-1.2.1.jar"
    "$cur/lib/mariadb-java-client-1.5.9.jar"
)

$SPARK_HOME/bin/spark-submit --master yarn --queue $queue \
    --conf 'spark.port.maxRetries=100' \
    --executor-memory 20g --driver-memory 20g \
    --executor-cores 10 --num-executors 20  \
    --conf 'spark.yarn.executor.memoryOverhead=4g'\
    --conf 'spark.dynamicAllocation.maxExecutors=50'\
    --jars $( IFS=$','; echo "${jars[*]}" ) \
    --class com.cpc.spark.ocpcV3.HotTopicOcpc.model.OcpcGetPb \
    /home/cpc/sunjianqiang/analyzer/target/scala-2.11/cpc-anal_2.11-0.1.jar $date $hour $version $media $isKnown

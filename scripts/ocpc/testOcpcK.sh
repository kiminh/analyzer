#!/bin/bash

hdfsCur=hdfs://emr-cluster/warehouse/azkaban
SPARK_HOME=/usr/lib/spark-current
localCur=/data/cpc/anal
set -e
cur_time=$1
date=`date -d"${cur_time} 3 hours ago" +"%Y-%m-%d"`
hour=`date -d"${cur_time} 3 hours ago" +"%H"`
queue=root.production.algo.cpc

jars=(
    "$localCur/lib/mysql-connector-java-5.1.41-bin.jar"
    "$localCur/lib/hadoop-lzo-0.4.20.jar"
    "$localCur/lib/config-1.2.1.jar"
)

randjar="tmp_"`date +%s%N`".jar"
hadoop fs -get $hdfsCur/lib/ocpc-data-userprofile.jar $randjar

$SPARK_HOME/bin/spark-submit --master yarn --queue $queue \
    --conf 'spark.port.maxRetries=100' \
    --executor-memory 20g --driver-memory 20g \
    --executor-cores 10 --num-executors 20  \
    --conf 'spark.yarn.executor.memoryOverhead=4g'\
    --conf 'spark.dynamicAllocation.maxExecutors=50'\
    --jars $( IFS=$','; echo "${jars[*]}" ) \
    --class com.cpc.spark.ocpcV2.OcpcK \
    $randjar $date $hour

rm $randjar

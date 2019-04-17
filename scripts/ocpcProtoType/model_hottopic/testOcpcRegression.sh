#!/bin/bash

#sh testOcpcRegression.sh 2019-03-06 12 72 1|2|3 hottopicv2 hottopic

cur=/data/cpc/anal
SPARK_HOME=/usr/lib/spark-current
queue=root.cpc.develop

date=$1
hour=$2
hourCnt=$3
conversionGoal=$4
version=$5
media=$6

jars=(
    "$cur/lib/mysql-connector-java-5.1.41-bin.jar"
    "$cur/lib/hadoop-lzo-0.4.20.jar"
    "$cur/lib/config-1.2.1.jar"
)

$SPARK_HOME/bin/spark-submit --master yarn --queue $queue \
    --conf 'spark.port.maxRetries=100' \
    --executor-memory 20g --driver-memory 20g \
    --executor-cores 10 --num-executors 20  \
    --conf 'spark.yarn.executor.memoryOverhead=4g'\
    --conf 'spark.dynamicAllocation.maxExecutors=50'\
    --jars $( IFS=$','; echo "${jars[*]}" ) \
    --class com.cpc.spark.OcpcProtoType.model_hottopic.OcpcRegression \
    /home/cpc/wangjun/analyzer/target/scala-2.11/cpc-anal_2.11-0.1.jar $date $hour $hourCnt $conversionGoal $version $media

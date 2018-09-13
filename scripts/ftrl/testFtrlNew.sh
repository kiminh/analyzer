#!/bin/bash

cur=/data/cpc/dw
SPARK_HOME=/usr/lib/spark-current
date=$1
hour=$2
typename=$3
gbdtVersion=$4
ftrlVersion=$5
upload=$6
#queue=root.cpc
queue=root.develop.adhoc.cpc
#queue=root.production.biz.cpc2


jars=(
    "$cur/lib/mysql-connector-java-5.1.41-bin.jar"
    "$cur/lib/hadoop-lzo-0.4.20.jar"
    "$cur/lib/config-1.2.1.jar"
)

$SPARK_HOME/bin/spark-submit --master yarn --queue $queue \
    --conf 'spark.port.maxRetries=100' \
    --executor-memory 20g --driver-memory 200g \
    --executor-cores 10 --num-executors 20  \
    --conf 'spark.yarn.executor.memoryOverhead=4g'\
    --conf 'spark.dynamicAllocation.maxExecutors=50'\
    --jars $( IFS=$','; echo "${jars[*]}" ) \
    --class com.cpc.spark.ml.ftrl.FtrlNewHourly \
    ../../target/scala-2.11/cpc-anal_2.11-0.1.jar $date $hour $typename $gbdtVersion $ftrlVersion $upload

#!/bin/bash

cur=/home/cpc/$1
SPARK_HOME=/home/spark/spark-2.1.0

jars=(
    "$cur/lib/mysql-connector-java-5.1.41-bin.jar"
    "$cur/lib/config-1.2.1.jar"
    "$cur/lib/hadoop-lzo-0.4.20.jar"
    "$cur/lib/scala-redis_2.11-1.0.jar"
)

$SPARK_HOME/bin/spark-submit --master yarn \
    --executor-memory 10G --executor-cores 10 --total-executor-cores 10 \
    --jars $( IFS=$','; echo "${jars[*]}" ) \
    --class com.cpc.spark.small.tool.GetAddAge \
    $cur/lib/cpc-anal_2.11-0.1.jar 1 10


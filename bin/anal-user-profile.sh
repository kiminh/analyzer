#!/bin/bash

cur=/home/cpc/$1
SPARK_HOME=/home/spark/spark-2.1.0

jars=(
    "$cur/lib/hadoop-lzo-0.4.20.jar"
    "$cur/lib/cpc-protocol_2.11-1.0.jar"
    "$cur/lib/mysql-connector-java-5.1.41-bin.jar"
)

$SPARK_HOME/bin/spark-submit --master yarn \
    --executor-memory 4G --executor-cores 2 --total-executor-cores 10 \
    --jars $( IFS=$','; echo "${jars[*]}" ) \
    --class com.cpc.spark.qukan.anal.AnalUserProfile \
    $cur/lib/cpc-anal_2.11-1.0.jar

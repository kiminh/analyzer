#!/bin/bash

cur=/home/cpc/$1
SPARK_HOME=/home/spark/spark-2.1.0

jars=(
    "$cur/lib/mysql-connector-java-5.1.41-bin.jar"
)

$SPARK_HOME/bin/spark-submit --master yarn \
    --executor-memory 4G --driver-memory 4G \
    --executor-cores 4 --total-executor-cores 16 --num-executors 4 \
    --jars $( IFS=$','; echo "${jars[*]}" ) \
    --class com.cpc.spark.log.anal.AnalUnionLog \
    $cur/lib/cpc-anal_2.11-0.1.jar "/gobblin/source/cpc" "cpc_union_log" "cpc_union_trace_log" 2


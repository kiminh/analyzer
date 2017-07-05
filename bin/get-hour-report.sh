#!/bin/bash

cur=/home/cpc/$1
SPARK_HOME=/home/spark/spark-2.1.0

jars=(
    "$cur/lib/mysql-connector-java-5.1.41-bin.jar"
    "$cur/lib/mariadb-java-client-1.5.9.jar"
    "$cur/lib/config-1.2.1.jar"
)

$SPARK_HOME/bin/spark-submit --master yarn \
    --executor-memory 2G --driver-memory 2G \
    --executor-cores 2 --total-executor-cores 8 --num-executors 4 \
    --jars $( IFS=$','; echo "${jars[*]}" ) \
    --class com.cpc.spark.log.report.GetHourReport \
    $cur/lib/cpc-anal_2.11-0.1.jar cpc_union_log 2


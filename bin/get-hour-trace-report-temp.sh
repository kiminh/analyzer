#!/bin/bash

cur=/home/cpc/$1
SPARK_HOME=/home/spark/spark-2.1.0

jars=(
    "$cur/lib/mysql-connector-java-5.1.41-bin.jar"
    "$cur/lib/cpc-protocol_2.11-1.0.jar"
)

$SPARK_HOME/bin/spark-submit --master yarn \
    --executor-memory 2G --executor-cores 2 --total-executor-cores 6 \
    --jars "$cur/lib/mysql-connector-java-5.1.41-bin.jar,$cur/lib/mariadb-java-client-1.5.9.jar" \
    --class com.cpc.spark.log.report.GetTraceReport \
    $cur/lib/cpc-trace-export_2.11-0.1.jar  10


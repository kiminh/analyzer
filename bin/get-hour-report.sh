#!/bin/bash

cur=/home/cpc/$1
SPARK_HOME=/home/spark/spark-2.1.0

$SPARK_HOME/bin/spark-submit --master yarn \
    --executor-memory 2G --executor-cores 2 --total-executor-cores 6 \
    --jars "$cur/lib/mysql-connector-java-5.1.41-bin.jar,$cur/lib/mariadb-java-client-1.5.9.jar" \
    --class com.cpc.spark.log.report.GetHourReport \
    $cur/lib/cpc-anal_2.11-1.0.jar "cpc_union_log" 3



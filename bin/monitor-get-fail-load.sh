#!/bin/bash

cur=/data/cpc/$1
SPARK_HOME=/usr/lib/spark-current
queue=root.report.biz.cpc
last_timestramp_path=/home/cpc/anal/data/monitor/last_timestramp

jars=(
    "$cur/lib/mysql-connector-java-5.1.41-bin.jar"
    "$cur/lib/config-1.2.1.jar"
    "$cur/lib/hadoop-lzo-0.4.20.jar"
    "$cur/lib/mariadb-java-client-1.5.9.jar"
)

$SPARK_HOME/bin/spark-submit --master yarn --queue $queue \
    --deploy-mode "client" \
    --executor-memory 10G --executor-cores 10 --total-executor-cores 10 \
    --jars $( IFS=$','; echo "${jars[*]}" ) \
    --class com.cpc.spark.monitor.fail.GetFailLoad \
    /home/cpc/tankaide/analyzer/target/scala-2.11/cpc-anal_2.11-0.1.jar $last_timestramp_path
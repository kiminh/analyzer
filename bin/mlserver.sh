#!/bin/bash

cur=/home/cpc/$1
SPARK_HOME=/home/spark/spark-2.1.0

jars=(
    "$cur/lib/scala-redis_2.11-1.0.jar"
    "$cur/lib/hadoop-lzo-0.4.20.jar"
    "$cur/lib/config-1.2.1.jar"
)

$SPARK_HOME/bin/spark-submit --master "spark://cpc-bj03:7077" \
    --executor-memory 2G --executor-cores 1 --total-executor-cores 1 \
    --jars $( IFS=$','; echo "${jars[*]}" ) \
    --class com.cpc.spark.ml.server.MLServer \
    $cur/lib/dev.jar 9090 "/user/cpc/model/v8_3d" /home/cpc/t/bin/uidclk.txt

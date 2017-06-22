#!/bin/bash

cur=/home/cpc/$1
SPARK_HOME=/home/spark/spark-2.1.0

jars=(
    "$cur/lib/mysql-connector-java-5.1.41-bin.jar"
    "$cur/lib/scala-redis_2.11-1.0.jar"
    "$cur/lib/config-1.2.1.jar"
    "$cur/lib/hadoop-lzo-0.4.20.jar"
)

$SPARK_HOME/bin/spark-submit --master yarn \
    --executor-memory 4G --driver-memory 4G \
    --executor-cores 2 --total-executor-cores 12 --num-executors 6 \
    --jars $( IFS=$','; echo "${jars[*]}" ) \
    --class com.cpc.spark.ml.ctrmodel.v1.CreateSvm \
    $cur/lib/dev.jar v16_full 3 4 1 "03,06,08,10,12,15,17,19,21,23"


#!/bin/bash

cur=/home/cpc/$1
SPARK_HOME=/home/spark/spark-2.1.0

jars=(
    "$cur/lib/mysql-connector-java-5.1.41-bin.jar"
    "$cur/lib/scala-redis_2.11-1.0.jar"
    "$cur/lib/config-1.2.1.jar"
)

$SPARK_HOME/bin/spark-submit --master yarn \
    --executor-memory 2G --driver-memory 2G \
    --executor-cores 2 --total-executor-cores 20 \
    --jars $( IFS=$','; echo "${jars[*]}" ) \
    --class com.cpc.spark.ml.train.AdvSvm \
    $cur/lib/dev.jar v8 2 3 uidclk.txt


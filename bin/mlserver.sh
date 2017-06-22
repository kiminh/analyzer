#!/bin/bash

cur=/home/cpc/$1
SPARK_HOME=/home/spark/spark-2.1.0

jars=(
    "$cur/lib/mysql-connector-java-5.1.41-bin.jar"
    "$cur/lib/hadoop-lzo-0.4.20.jar"
    "$cur/lib/config-1.2.1.jar"
)

$SPARK_HOME/bin/spark-submit --master "spark://10.9.125.57:7077" \
    --executor-memory 4G --driver-memory 4G \
    --executor-cores 1 --total-executor-cores 1 \
    --jars $( IFS=$','; echo "${jars[*]}" ) \
    --class com.cpc.spark.ml.ctrmodel.v1.Server \
    $cur/lib/cpc-ml_2.11-0.1.jar 9091 /user/cpc/model/v16_7d


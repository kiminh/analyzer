#!/bin/bash

cur=/home/cpc/$1
SPARK_HOME=/home/spark/spark-2.1.0

jars=(
    "$cur/lib/mysql-connector-java-5.1.41-bin.jar"
    "$cur/lib/hadoop-lzo-0.4.20.jar"
)

$SPARK_HOME/bin/spark-submit --master "spark://cpc-bj03:7077" \
    --executor-memory 2G --executor-cores 2 --total-executor-cores 8 \
    --jars $( IFS=$','; echo "${jars[*]}" ) \
    --class com.cpc.spark.ml.train.LRTrain \
    $cur/lib/dev.jar "train" \
          "/user/cpc/svmdata/v5/2017-06-{04,05,06}" \
          "/user/cpc/model/v5_3d" \
          0.99 2


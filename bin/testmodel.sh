#!/bin/bash

cur=/home/cpc/$1
SPARK_HOME=/home/spark/spark-2.1.0

jars=(
    "$cur/lib/mysql-connector-java-5.1.41-bin.jar"
    "$cur/lib/hadoop-lzo-0.4.20.jar"
)

$SPARK_HOME/bin/spark-submit --master yarn \
    --executor-memory 4G --driver-memory 4G \
    --executor-cores 4 --total-executor-cores 21 \
    --jars $( IFS=$','; echo "${jars[*]}" ) \
    --class com.cpc.spark.ml.train.LRTrain \
    $cur/lib/dev.jar "test" \
          "/user/cpc/svmdata/v8/2017-06-12" \
          "/user/cpc/model/v8_3d" \
          0.8 1



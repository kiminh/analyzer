#!/bin/bash

cur=/home/cpc/$1
SPARK_HOME=/home/spark/spark-2.1.0

jars=(
    "$cur/lib/mysql-connector-java-5.1.41-bin.jar"
    "$cur/lib/hadoop-lzo-0.4.20.jar"
)

$SPARK_HOME/bin/spark-submit --master yarn \
    --executor-memory 4G --driver-memory 4G \
    --executor-cores 4 --num-executors 10 \
    --jars $( IFS=$','; echo "${jars[*]}" ) \
    --class com.cpc.spark.ml.train.CtrModel \
    $cur/lib/dev.jar "test+ir" \
          "/user/cpc/svmdata/v2_full" 1 1 \
          "/user/cpc/model/v2/2017-07-17-20" \
          1 1 5000 "" "ir_v1.txt"


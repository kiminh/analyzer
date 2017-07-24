#!/bin/bash

cur=/home/cpc/$1
SPARK_HOME=/home/spark/spark-2.1.0
host=spark://cpc-bj03:7077

jars=(
    "$cur/lib/mysql-connector-java-5.1.41-bin.jar"
    "$cur/lib/hadoop-lzo-0.4.20.jar"
)

$SPARK_HOME/bin/spark-submit --master yarn \
    --executor-memory 8G --driver-memory 8G \
    --executor-cores 4 --num-executors 10 \
    --jars $( IFS=$','; echo "${jars[*]}" ) \
    --class com.cpc.spark.ml.train.CtrModel \
    $cur/lib/dev.jar "train+ir" \
          "/user/cpc/svmdata/v3" 10 10 \
          "/user/cpc/model/v3" \
          1 1 5000 "" ""


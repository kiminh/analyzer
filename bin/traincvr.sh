#!/bin/bash

cur=/home/cpc/$1
SPARK_HOME=/home/spark/spark-2.1.0
host=spark://cpc-bj03:7077

jars=(
    "$cur/lib/mysql-connector-java-5.1.41-bin.jar"
    "$cur/lib/hadoop-lzo-0.4.20.jar"
)

$SPARK_HOME/bin/spark-submit --master yarn \
    --executor-memory 16G --driver-memory 16G \
    --executor-cores 10 --num-executors 10 \
    --jars $( IFS=$','; echo "${jars[*]}" ) \
    --class com.cpc.spark.ml.train.CvrModel \
    $cur/lib/dev.jar "train+ir" \
          "/user/cpc/cvr_svm/v2" 50 50 \
          "/user/cpc/cvr_model/v2" \
          0.9 1 1000 "" ""


#!/bin/bash

cur=/home/cpc/$1
SPARK_HOME=/home/spark/spark-2.1.0

jars=(
    "$cur/lib/mysql-connector-java-5.1.41-bin.jar"
    "$cur/lib/hadoop-lzo-0.4.20.jar"
)

$SPARK_HOME/bin/spark-submit --master yarn \
    --executor-memory 64G --driver-memory 64G \
    --executor-cores 16 --num-executors 8 \
    --jars $( IFS=$','; echo "${jars[*]}" ) \
    --class com.cpc.spark.ml.train.LogisticRegression \
    $cur/lib/dev.jar "train" \
          "/user/cpc/svmdata/v10/2017-06-{13,14}" \
          "/user/cpc/model/v10_2d" \
          0.99 1


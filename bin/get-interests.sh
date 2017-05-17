#!/bin/bash

cur=/home/cpc/$1
SPARK_HOME=/home/spark/spark-2.1.0

jars=(
    "$cur/lib/scala-redis_2.11-1.0.jar"
    "$cur/lib/hadoop-lzo-0.4.20.jar"
    "$cur/lib/config-1.2.1.jar"
)

 

$SPARK_HOME/bin/spark-submit --master yarn \
    --executor-memory 4G --executor-cores 2 --total-executor-cores 10 \
    --jars $( IFS=$','; echo "${jars[*]}" ) \
    --class com.cpc.spark.qukan.userprofile.GetInterests \
    $cur/lib/cpc-anal_2.11-0.1.jar 1



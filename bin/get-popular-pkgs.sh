#!/bin/bash

cur=/home/cpc/$1analyzer
SPARK_HOME=/home/spark/spark-2.1.0

$SPARK_HOME/bin/spark-submit --master yarn \
    --executor-memory 4G --executor-cores 2 --total-executor-cores 10 \
    --jars "$cur/lib/scala-redis_2.11-1.0.jar, \
            $cur/lib/hadoop-lzo-0.4.20.jar, \
            $cur/lib/config-1.2.1.jar, \
            $cur/lib/cpc-protocol_2.11-1.0.jar, \
            $cur/lib/json4s-native_2.11-3.5.1.jar, \
            $cur/lib/json4s-core_2.11-3.5.1.jar, \
            $cur/lib/json4s-ast_2.11-3.5.1.jar" \
    --class com.cpc.spark.qukan.userprofile.GetPopularPkgs \
    $cur/lib/cpc-anal_2.11-1.0.jar 500



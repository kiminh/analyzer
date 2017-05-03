#!/bin/bash

cur=/home/cpc/$1analyzer
SPARK_HOME=/home/spark/spark-2.1.0

$SPARK_HOME/bin/spark-submit --master yarn \
	--executor-memory 5G --executor-cores 2 --total-executor-cores 10 \
	--jars "$cur/lib/hadoop-lzo-0.4.20.jar, \
	        $cur/lib/cpc-protocol_2.11-1.0.jar" \
	--class com.cpc.spark.qukan.anal.AnalUserProfile \
    $cur/lib/cpc-anal_2.11-1.0.jar

#!/bin/bash

cur=/home/mapred/cpc_anal
SPARK_HOME=/home/spark/spark-2.1.0

$SPARK_HOME/bin/spark-submit --master yarn \
	--executor-memory 2G --executor-cores 2 --total-executor-cores 10  \
	--jars "$cur/libs/scala-redis_2.11-1.0.jar,$cur/libs/hadoop-lzo-0.4.20.jar,$cur/libs/cpc-protocol_2.11-1.0.jar" \
	--class com.cpc.spark.qukan.userprofile.GetUserProfile \
	$cur/libs/cpc-anal_2.11-1.0.jar

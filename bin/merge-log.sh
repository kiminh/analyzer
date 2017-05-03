#!/bin/bash

cur=/home/mapred/cpc_anal
SPARK_HOME=/home/spark/spark-2.1.0

$SPARK_HOME/bin/spark-submit --master yarn \
	--executor-memory 2G --executor-cores 2 --total-executor-cores 6 \
	--jars "$cur/libs/mysql-connector-java-5.1.41-bin.jar,$cur/libs/cpc-protocol_2.11-1.0.jar" \
	--class com.cpc.spark.log.anal.AnalUnionLog \
	$cur/libs/cpc-anal_2.11-1.0.jar "/gobblin/source/cpc" "cpc_union_log" 1

$SPARK_HOME/bin/spark-submit --master yarn \
	--executor-memory 2G --executor-cores 2 --total-executor-cores 6 \
	--jars "$cur/libs/mysql-connector-java-5.1.41-bin.jar,$cur/libs/mariadb-java-client-1.5.9.jar" \
	--class com.cpc.spark.log.report.GetHourReport \
	$cur/libs/cpc-anal_2.11-1.0.jar "cpc_union_log" 2


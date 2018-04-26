#!/bin/bash

cur=/home/cpc/$1
SPARK_HOME=/home/spark/spark-2.1.0
host="spark://10.9.125.57:7077"

jars=(
    "$cur/lib/mysql-connector-java-5.1.41-bin.jar"
)

for h in  5 4 ; do
	$SPARK_HOME/bin/spark-submit --master yarn \
	    --executor-memory 4G --executor-cores 4 --num-executors 10 \
	    --jars $( IFS=$','; echo "${jars[*]}" ) \
	    --class com.cpc.spark.log.anal.AnalUnionLog \
	    $cur/lib/dev.jar "/gobblin/source/cpc" "cpc_union_log" "cpc_union_trace_log" $h
done


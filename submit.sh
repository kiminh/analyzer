#!/bin/bash

set -e

cur_time=$1
modelVersion=$2
today=`date -d "${cur_time} 1 hours ago" +%Y-%m-%d`
hour=`date -d "${cur_time} 1 hours ago" +%H`

jarLib=hdfs://emr-cluster/warehouse/azkaban/lib/fhb_start_v1.jar

queue=root.cpc.bigdata
jars=("/home/cpc/anal/lib/spark-tensorflow-connector_2.11-1.10.0.jar" )

randjar="fhb_start"`date +%s%N`".jar"
hadoop fs -get $jarLib $randjar

hdfsPath="/user/cpc/hzh/dssm/ad-output-hour-${modelVersion}/${today}/${hour}"

spark-submit --master yarn --queue ${queue} \
    --name "cpc-adlist-tf-decode" \
    --driver-memory 24g --executor-memory 8g \
    --num-executors 30 --executor-cores 8 \
    --conf "spark.yarn.executor.memoryOverhead=4g" \
    --conf "spark.sql.shuffle.partitions=500" \
    --jars $( IFS=$','; echo "${jars[*]}" ) \
    --class com.cpc.spark.ml.dnn.baseData.ReadExampleFromHdfs \
    $randjar ${hdfsPath}

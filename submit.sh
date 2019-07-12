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
#src="hdfs://emr-cluster/user/cpc/aiclk_dataflow/daily/adlist-v4/2019-06-11/part-*"
src="hdfs://emr-cluster/user/cpc/aiclk_dataflow/daily/adlist-v4/2019-06-10/part-r*"
des_dir="hdfs://emr-cluster/user/cpc/fenghuabin/adlist-v4-info"
des_date="2019-06-10"
des_map="emb-map"
partitions=1000

spark-submit --master yarn --queue ${queue} \
    --name "cpc-adlist-tf-decode" \
    --driver-memory 16g --executor-memory 16g \
    --num-executors 1000 --executor-cores 4 \
    --conf spark.hadoop.fs.defaultFS=hdfs://emr-cluster2 \
    --conf "spark.yarn.executor.memoryOverhead=4g" \
    --conf "spark.sql.shuffle.partitions=500" \
    --jars $( IFS=$','; echo "${jars[*]}" ) \
    --class com.cpc.spark.ml.dnn.baseData.ReadExampleFromHdfs \
    ${randjar} ${src} ${des_dir} ${des_date} ${des_map} ${partitions}

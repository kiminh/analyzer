#!/bin/bash

set -e

cur_date="2019-06-11-bak"
cur_date=$1
#modelVersion=$2
#today=`date -d "${cur_time} 1 hours ago" +%Y-%m-%d`
#hour=`date -d "${cur_time} 1 hours ago" +%H`

jarLib=hdfs://emr-cluster/warehouse/azkaban/lib/fhb_start_v1.jar

queue=root.cpc.basedata
jars=("/home/cpc/anal/lib/spark-tensorflow-connector_2.11-1.10.0.jar" )

randjar="fhb_start"`date +%s%N`".jar"
hadoop fs -get ${jarLib} ${randjar}

src_dir="hdfs://emr-cluster/user/cpc/fenghuabin/adlist-v4-info"
src_date="2019-05-31;2019-06-01;2019-06-02;2019-06-03;2019-06-04;2019-06-05;2019-06-06;2019-06-07;2019-06-08;2019-06-09;2019-06-10;2019-06-11"
instances_file="hdfs://emr-cluster/user/cpc/fenghuabin/adlist-v4-info/sampled-v1-instances"
partitions=1000

spark-submit --master yarn --queue ${queue} \
    --name "cpc-adlist-collect-ids" \
    --driver-memory 8g --executor-memory 8g \
    --num-executors 10 --executor-cores 4 \
    --conf spark.hadoop.fs.defaultFS=hdfs://emr-cluster2 \
    --conf "spark.yarn.executor.memoryOverhead=4g" \
    --conf "spark.sql.shuffle.partitions=500" \
    --jars $( IFS=$','; echo "${jars[*]}" ) \
    --class com.cpc.spark.ml.dnn.baseData.CollectTrainUniqSparseID \
    ${randjar} ${src_dir} ${src_date} ${instances_file} ${partitions}

hadoop fs -chmod -R 0777 ${instances_file}

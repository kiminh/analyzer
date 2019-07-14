#!/bin/bash

set -e

cur_date="2019-06-11-bak"
cur_date=$1
#modelVersion=$2
#today=`date -d "${cur_time} 1 hours ago" +%Y-%m-%d`
#hour=`date -d "${cur_time} 1 hours ago" +%H`

jarLib=hdfs://emr-cluster/warehouse/azkaban/lib/fhb_start_v1.jar

queue=root.cpc.basedata
queue=root.cpc.bigdata
jars=("/home/cpc/anal/lib/spark-tensorflow-connector_2.11-1.10.0.jar" )

randjar="fhb_start"`date +%s%N`".jar"
hadoop fs -get ${jarLib} ${randjar}

#src="hdfs://emr-cluster/user/cpc/aiclk_dataflow/daily/adlist-v4/2019-06-11/part-*"
src="hdfs://emr-cluster/user/cpc/aiclk_dataflow/daily/adlist-v4/"${cur_date}"/part*"
des_date=${cur_date}
des_map="emb-map"
partitions=1000

src_dir="hdfs://emr-cluster/user/cpc/aiclk_dataflow/daily/adlist-v4"
src_date="2019-05-26;2019-05-27;2019-05-28;2019-05-29;2019-05-30;2019-05-31;2019-06-01;2019-06-02;2019-06-03;2019-06-04;2019-06-05;2019-06-06;2019-06-07;2019-06-08;2019-06-09;2019-06-10;2019-06-11"
src_date="2019-06-10;2019-06-09;2019-06-08;2019-06-07;2019-06-11"
src_date="2019-06-10;2019-06-09;2019-06-08;2019-06-07;2019-06-11;2019-06-06;2019-06-05;2019-06-04;2019-06-03;2019-06-02;2019-06-01;2019-05-31;2019-05-30;2019-05-29;2019-05-28;2019-05-27;2019-05-26"
src_date="2019-06-07;2019-06-08;2019-06-09;2019-06-10;2019-06-11;2019-06-06;2019-06-05;2019-06-04;2019-06-03;2019-06-02;2019-06-01;2019-05-31;2019-05-30;2019-05-29;2019-05-28;2019-05-27;2019-05-26"
des_dir="hdfs://emr-cluster/user/cpc/fenghuabin/adlist-v4-base"
instances_file="instances-all"
partitions=1000

spark-submit --master yarn --queue ${queue} \
    --name "adlist-tf-make-example" \
    --driver-memory 16g --executor-memory 16g \
    --num-executors 1000 --executor-cores 4 \
    --conf spark.hadoop.fs.defaultFS=hdfs://emr-cluster2 \
    --conf "spark.yarn.executor.memoryOverhead=4g" \
    --conf "spark.sql.shuffle.partitions=500" \
    --jars $( IFS=$','; echo "${jars[*]}" ) \
    --class com.cpc.spark.ml.dnn.baseData.MakeTrainExamples \
    ${randjar} ${src_dir} ${src_date} ${des_dir} ${instances_file} ${partitions}

#chmod_des="hdfs://emr-cluster/user/cpc/fenghuabin/adlist-v4-info"${des_date}"*"
#hadoop fs -chmod -R 0777 ${chmod_des}

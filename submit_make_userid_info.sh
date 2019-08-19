#!/bin/bash

set -e

jarLib=hdfs://emr-cluster/warehouse/azkaban/lib/fhb_start_v1.jar

queue=root.cpc.bigdata
jars=("/home/cpc/anal/lib/spark-tensorflow-connector_2.11-1.10.0.jar" )

randjar="fhb_start"`date +%s%N`".jar"
hadoop fs -get ${jarLib} ${randjar}

src_dir="hdfs://emr-cluster/user/cpc/aiclk_dataflow/daily/adlist-v4"
date_begin=`date --date='20 days ago' +%Y-%m-%d`
date_end=`date --date='1 days ago' +%Y-%m-%d`
des_dir="hdfs://emr-cluster/user/cpc/fenghuabin/adlist-v4-userid-info"
sta_date_begin="2019-08-04"
sta_date_end="2019-08-18"

one_hot_feature_list="media_type,mediaid,channel,sdk_type,adslot_type,adslotid,sex,dtu_id,adtype,interaction,bid,ideaid,unitid,planid,userid,is_new_ad,adclass,site_id,os,network,phone_price,brand,province,city,city_level,uid,age,hour"

spark-submit --master yarn --queue ${queue} \
    --name "adlist-tf-make-userid-statistics" \
    --driver-memory 4g --executor-memory 4g \
    --num-executors 200 --executor-cores 4 \
    --conf spark.hadoop.fs.defaultFS=hdfs://emr-cluster2 \
    --conf "spark.yarn.executor.memoryOverhead=4g" \
    --conf "spark.sql.shuffle.partitions=500" \
    --jars $( IFS=$','; echo "${jars[*]}" ) \
    --class com.cpc.spark.ml.dnn.baseData.MakeSampling \
    ${randjar} ${src_dir} ${date_begin} ${date_end} ${des_dir}

chmod_des="hdfs://emr-cluster/user/cpc/fenghuabin/adlist-v4-sampled"
hadoop fs -chmod -R 0777 ${chmod_des}


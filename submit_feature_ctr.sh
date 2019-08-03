#!/bin/bash

#set -e

jarLib=hdfs://emr-cluster/warehouse/azkaban/lib/fhb_start_v1.jar
queue=root.cpc.bigdata
jars=("/home/cpc/anal/lib/spark-tensorflow-connector_2.11-1.10.0.jar" )

randjar="fhb_start"`date +%s%N`".jar"
hadoop fs -get ${jarLib} ${randjar}

one_hot_feature_list="media_type,mediaid,channel,sdk_type,adslot_type,adslotid,sex,dtu_id,adtype,interaction,bid,ideaid,unitid,planid,userid,is_new_ad,adclass,site_id,os,network,phone_price,brand,province,city,city_level,uid,age,hour,week"
src_dir="hdfs://emr-cluster/user/cpc/aiclk_dataflow/daily/adlist-v4"
des_dir="hdfs://emr-cluster/user/cpc/fenghuabin/adlist-v4-ctr-feature"

ctr_feature_date_begin=2019-06-26
ctr_feature_date_end=2019-07-26


partitions=1000
one_hot_cnt=29
multi_hot_cnt=15

spark-submit --master yarn --queue ${queue} \
    --name "feature_ctr" \
    --driver-memory 2g --executor-memory 2g \
    --num-executors 1000 --executor-cores 4 \
    --conf spark.hadoop.fs.defaultFS=hdfs://emr-cluster2 \
    --conf "spark.yarn.executor.memoryOverhead=4g" \
    --conf "spark.sql.shuffle.partitions=500" \
    --jars $( IFS=$','; echo "${jars[*]}" ) \
    --class com.cpc.spark.ml.dnn.baseData.FeatureCtr \
    ${randjar} ${one_hot_feature_list} ${src_dir} ${des_dir} ${ctr_feature_date_begin} ${ctr_feature_date_end} ${partitions} ${one_hot_cnt} ${multi_hot_cnt}

hadoop fs -chmod -R 0777 ${des_dir}

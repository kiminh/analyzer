#!/bin/bash

set -e

cur_date="2019-06-11-bak"
cur_date=$1
#modelVersion=$2


jarLib=hdfs://emr-cluster/warehouse/azkaban/lib/fhb_start_v1.jar

queue=root.cpc.bigdata
jars=("/home/cpc/anal/lib/spark-tensorflow-connector_2.11-1.10.0.jar" )

randjar="fhb_start"`date +%s%N`".jar"
hadoop fs -get ${jarLib} ${randjar}


one_hot_feature_list="media_type,mediaid,channel,sdk_type,adslot_type,adslotid,sex,dtu_id,adtype,interaction,bid,ideaid,unitid,planid,userid,is_new_ad,adclass,site_id,os,network,phone_price,brand,province,city,city_level,uid,age,hour"
src_dir="hdfs://emr-cluster/user/cpc/aiclk_dataflow/daily/adlist-v4"
sta_date="2019-07-21;2019-07-20;2019-07-19;2019-07-18;2019-07-17;2019-07-16;2019-07-15;2019-07-14;2019-07-13;2019-07-12;2019-07-11;2019-07-10;2019-07-09;2019-07-08;2019-07-07"
cur_date=`date --date='1 days ago' +%Y-%m-%d`
begin_date=`date --date='15 days ago' +%Y-%m-%d`
des_dir="hdfs://emr-cluster/user/cpc/fenghuabin/adlist-v4-monitor"
partitions=1000
one_hot_cnt=28
muti_hot_cnt=15

spark-submit --master yarn --queue ${queue} \
    --name "feature_monitor" \
    --driver-memory 16g --executor-memory 16g \
    --num-executors 500 --executor-cores 4 \
    --conf spark.hadoop.fs.defaultFS=hdfs://emr-cluster2 \
    --conf "spark.yarn.executor.memoryOverhead=4g" \
    --conf "spark.sql.shuffle.partitions=500" \
    --jars $( IFS=$','; echo "${jars[*]}" ) \
    --class com.cpc.spark.ml.dnn.baseData.FeatureMonitor \
    ${randjar} ${one_hot_feature_list} ${src_dir} ${sta_date} ${cur_date} ${begin_date} ${des_dir} ${partitions} ${one_hot_cnt} ${muti_hot_cnt}

#chmod_des="hdfs://emr-cluster/user/cpc/fenghuabin/adlist-v4-info"${des_date}"*"
#hadoop fs -chmod -R 0777 ${chmod_des}

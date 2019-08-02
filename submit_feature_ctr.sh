#!/bin/bash

#set -e

jarLib=hdfs://emr-cluster/warehouse/azkaban/lib/fhb_start_v1.jar
queue=root.cpc.bigdata
jars=("/home/cpc/anal/lib/spark-tensorflow-connector_2.11-1.10.0.jar" )

randjar="fhb_start"`date +%s%N`".jar"
hadoop fs -get ${jarLib} ${randjar}

one_hot_feature_list="media_type,mediaid,channel,sdk_type,adslot_type,adslotid,sex,dtu_id,adtype,interaction,bid,ideaid,unitid,planid,userid,is_new_ad,adclass,site_id,os,network,phone_price,brand,province,city,city_level,uid,age,hour"
src_dir="hdfs://emr-cluster/user/cpc/aiclk_dataflow/daily/adlist-v4"
des_dir="hdfs://emr-cluster/user/cpc/fenghuabin/adlist-v4-ctr-feature"

#20190727
collect_date_begin=`date --date='33 days ago' +%Y-%m-%d`
collect_date_end=`date --date='6 days ago' +%Y-%m-%d`
ctr_feature_date=`date --date='5 days ago' +%Y-%m-%d`
#20190726
collect_date_begin=`date --date='34 days ago' +%Y-%m-%d`
collect_date_end=`date --date='7 days ago' +%Y-%m-%d`
ctr_feature_date=`date --date='6 days ago' +%Y-%m-%d`
#20190725
collect_date_begin=`date --date='35 days ago' +%Y-%m-%d`
collect_date_end=`date --date='8 days ago' +%Y-%m-%d`
ctr_feature_date=`date --date='7 days ago' +%Y-%m-%d`

#20190724
collect_date_begin=`date --date='36 days ago' +%Y-%m-%d`
collect_date_end=`date --date='9 days ago' +%Y-%m-%d`
ctr_feature_date=`date --date='8 days ago' +%Y-%m-%d`

collect_date_begin=2019-06-14
collect_date_end=2019-06-25
ctr_feature_date=2019-07-23

echo ${ctr_feature_date}
echo ${collect_date_begin}
echo ${collect_date_end}


partitions=1000
one_hot_cnt=28
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
    ${randjar} ${one_hot_feature_list} ${src_dir} ${des_dir} ${ctr_feature_date} ${collect_date_begin} ${collect_date_end} ${partitions} ${one_hot_cnt} ${multi_hot_cnt}

hadoop fs -chmod -R 0777 ${des_dir}

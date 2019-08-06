#!/bin/bash

set -e

cur_date="2019-06-11-bak"
cur_date=$1
#modelVersion=$2
#today=`date -d "${cur_time} 1 hours ago" +%Y-%m-%d`
#hour=`date -d "${cur_time} 1 hours ago" +%H`

jarLib=hdfs://emr-cluster/warehouse/azkaban/lib/fhb_start_v1.jar

queue=root.cpc.bigdata
jars=("/home/cpc/anal/lib/spark-tensorflow-connector_2.11-1.10.0.jar" )

randjar="fhb_start"`date +%s%N`".jar"
hadoop fs -get ${jarLib} ${randjar}

one_hot_feature_list="media_type,mediaid,channel,sdk_type,adslot_type,adslotid,sex,dtu_id,adtype,interaction,bid,ideaid,unitid,planid,userid,is_new_ad,adclass,site_id,os,network,phone_price,brand,province,city,city_level,uid,age,hour"
src_dir="hdfs://emr-cluster/user/cpc/aiclk_dataflow/daily/adlist-v4"
date_begin="2019-07-20"
date_end="2019-08-03"

des_dir="hdfs://emr-cluster/user/cpc/fenghuabin/adlist-v4-sampled"
ctr_feature_dir="hdfs://emr-cluster/user/cpc/fenghuabin/adlist-v4-ctr-feature"
test_data_src="2019-08-03/part-r-000*"
test_data_des="test-2019-08-03"
test_data_week="Sat"
instances_file="instances-all"
partitions=1000
with_week=False

spark-submit --master yarn --queue ${queue} \
    --name "adlist-tf-make-example" \
    --driver-memory 16g --executor-memory 16g \
    --num-executors 400 --executor-cores 4 \
    --conf spark.hadoop.fs.defaultFS=hdfs://emr-cluster2 \
    --conf "spark.yarn.executor.memoryOverhead=4g" \
    --conf "spark.sql.shuffle.partitions=500" \
    --jars $( IFS=$','; echo "${jars[*]}" ) \
    --class com.cpc.spark.ml.dnn.baseData.MakeTrainExamples \
    ${randjar} ${one_hot_feature_list} ${ctr_feature_dir} ${src_dir} ${with_week} ${date_begin} ${date_end} ${des_dir} ${instances_file} ${test_data_src} ${test_data_des} ${test_data_week} ${partitions}

chmod_des="hdfs://emr-cluster/user/cpc/fenghuabin/adlist-v4-sampled"
hadoop fs -chmod -R 0777 ${chmod_des}


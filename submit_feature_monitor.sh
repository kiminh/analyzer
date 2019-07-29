#!/bin/bash

#set -e

src_dir="hdfs://emr-cluster/user/cpc/aiclk_dataflow/daily/adlist-v4"
cur_date=`date --date='1 days ago' +%Y-%m-%d`
echo ${cur_date}


if [[ ! -d "./feature_monitor" ]]; then
    mkdir ./feature_monitor
fi

local_dir=./feature_monitor/${cur_date}

spark_in_run=${local_dir}/spark_running
if [[ -f "$spark_in_run" ]]; then
    echo "Spark In Processing, existing......."
    exit 0
fi

sent_ok=${local_dir}/alert_sent_ok
if [[ -f "$sent_ok" ]]; then
    exit 0
fi


if [[ ! -d "$local_dir" ]]; then
    mkdir ${local_dir}
fi

success=${local_dir}/_SUCCESS
count=${local_dir}/count

if [[ ! -f "$success" ]]; then
    hadoop fs -get ${src_dir}/${cur_date}/_SUCCESS ${success}
fi

if [[ ! -f "$count" ]]; then
    hadoop fs -get ${src_dir}/${cur_date}/count ${count}
fi

if [[ ! -f "$success" ]]; then
    alert="[Valid Source Example File Not Found]no _SUCCESS file detected:"${src_dir}/${cur_date}
    echo ${alert}
    exit -1
fi
if [[ ! -f "$count" ]]; then
    alert="[Valid Source Example File Not Found]no count file detected:"${src_dir}/${cur_date}
    echo ${alert}
    exit -1
fi

sample_count=10
for line in $(cat ${count})
do
    sample_count=$((line))
done
echo ${sample_count}

if [[ "${sample_count}" -lt 10000000 ]]; then
    echo "invalid example count"
    alert="[Valid Source Example File Not Found]too less sample_count in count file:"${sample_count}
    echo ${alert}
    exit -1
fi

des_dir="hdfs://emr-cluster/user/cpc/fenghuabin/adlist-v4-monitor"

alert=${local_dir}/alerts
empty=${local_dir}/empty

if [[ ! -d "$alert" ]]; then
    alert_path=${des_dir}/${cur_date}-monitor/alerts
    hadoop fs -get ${alert_path} ${local_dir}
fi
if [[ ! -d "$empty" ]]; then
    empty_path=${des_dir}/${cur_date}-monitor/empty
    hadoop fs -get ${empty_path} ${local_dir}
fi

if [[ -d "$empty" ]]; then
    alert="[No Alerts Found]"
    echo ${alert}
    exit 0
fi

if [[ -d "$alert" ]]; then
    python kafka_writer.py ${alert}/part-00000
    touch ${sent_ok}
    exit 0
fi

touch ${spark_in_run}

jarLib=hdfs://emr-cluster/warehouse/azkaban/lib/fhb_start_v1.jar

queue=root.cpc.bigdata
jars=("/home/cpc/anal/lib/spark-tensorflow-connector_2.11-1.10.0.jar" )

randjar="fhb_start"`date +%s%N`".jar"
hadoop fs -get ${jarLib} ${randjar}


one_hot_feature_list="media_type,mediaid,channel,sdk_type,adslot_type,adslotid,sex,dtu_id,adtype,interaction,bid,ideaid,unitid,planid,userid,is_new_ad,adclass,site_id,os,network,phone_price,brand,province,city,city_level,uid,age,hour"
cur_date=`date --date='1 days ago' +%Y-%m-%d`
begin_date=`date --date='15 days ago' +%Y-%m-%d`
partitions=1000
one_hot_cnt=28
muti_hot_cnt=15

spark-submit --master yarn --queue ${queue} \
    --name "feature_monitor" \
    --driver-memory 16g --executor-memory 16g \
    --num-executors 50 --executor-cores 4 \
    --conf spark.hadoop.fs.defaultFS=hdfs://emr-cluster2 \
    --conf "spark.yarn.executor.memoryOverhead=4g" \
    --conf "spark.sql.shuffle.partitions=500" \
    --jars $( IFS=$','; echo "${jars[*]}" ) \
    --class com.cpc.spark.ml.dnn.baseData.FeatureMonitor \
    ${randjar} ${one_hot_feature_list} ${src_dir} ${cur_date} ${begin_date} ${des_dir} ${partitions} ${one_hot_cnt} ${muti_hot_cnt}

rm -rf ${spark_in_run}
#chmod_des="hdfs://emr-cluster/user/cpc/fenghuabin/adlist-v4-info"${des_date}"*"
#hadoop fs -chmod -R 0777 ${chmod_des}

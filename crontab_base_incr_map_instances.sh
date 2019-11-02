#!/usr/bin/env bash
#fenghuabin@qutoutiao.net


source /etc/profile

ml_name=adlist
ml_ver=v4refult

date_full=`date`
#printf "*****************************${date_full}********************************\n"
curr_date=`date --date='0 days ago' +%Y-%m-%d`
now_hour=$(date "+%H")
now_minutes=$(date "+%M")
now_id="00"${now_hour}
if [ ${now_minutes} -ge 30 ];then
    now_id="30"${now_hour}
fi
#printf "now id is:%s\n" ${now_id}

dir=base_incr_map_instances
if [[ ! -d "${dir}" ]]; then
    mkdir ${dir}
fi

shell_in_run=${dir}/shell_in_busy
if [[ -f "$shell_in_run" ]]; then
    #printf "shell are busy now, existing\n"
    #printf "*****************************${date_full}********************************\n"
    printf "\n\n\n"
    exit 0
fi
touch ${shell_in_run}


curr_date=`date --date='0 days ago' +%Y-%m-%d`
last_date=`date --date='1 days ago' +%Y-%m-%d`



des_dir=hdfs://emr-cluster/user/cpc/fenghuabin/adlist-v4-transformer
hdfs_path_data=hdfs://emr-cluster/user/cpc/fenghuabin/rockefeller_backup
hdfs_path_model=hdfs://emr-cluster/warehouse/dl_cpc.db/cpc_algo_models/qtt-list-dnn-rawid_${ml_ver}

des_instances_remote=${hdfs_path_model}/${curr_date}-tf-base-incr-map-instances
des_instances_local=${dir}/${curr_date}-tf-base-incr-map-instances
if [[ ! -f ${des_instances_local} ]]; then
    hadoop fs -get ${des_instances_remote}/part-00000 ${des_instances_local} &
fi
wait
if [[ -f ${des_instances_local} ]]; then
    printf "existing today's base incr instances file, existing...\n"
    rm ${shell_in_run}
    exit 0
fi


hadoop fs -mkdir ${des_dir}/${curr_date}-collect-inc

last_model_base=${last_date}-tf-base-mom-8days-6eps
last_model_incr=${last_date}-tf-base-mom-8days-6eps-inc

last_base_model_instances=${hdfs_path_model}/${last_model_base}/map_instances.data
last_incr_model_instances=${hdfs_path_model}/${last_model_incr}/map_instances.data

last_model_instances=${last_base_model_instances}
last_daily_instances=${hdfs_path_data}/${last_date}-instances/part-00000

map_tmp=${dir}/${last_date}_tmp_instances
if [[ ! -f ${map_tmp} ]]; then
    hadoop fs -get ${last_base_model_instances} ${map_tmp} &
fi
wait
if [[ ! -f ${map_tmp} ]]; then
    printf "no yesterday's base model instances file, use base incr model instances file...\n"
    last_model_instances=${last_incr_model_instances}
fi

map_base=${dir}/${last_date}_base_instances
map_incr=${dir}/${last_date}_incr_instances

if [[ ! -f ${map_base} ]]; then
    hadoop fs -get ${last_model_instances} ${map_base} &
fi
if [[ ! -f ${map_incr} ]]; then
    hadoop fs -get ${last_daily_instances} ${map_incr} &
fi
wait

if [[ ! -f ${map_base} ]]; then
    printf "no yesterday's base model map instances file, existing...\n"
    rm ${shell_in_run}
    exit 0
fi
if [[ ! -f ${map_incr} ]]; then
    printf "no yesterday's incr daily map instances file, existing...\n"
    rm ${shell_in_run}
    exit 0
fi

jarLib=hdfs://emr-cluster/warehouse/azkaban/lib/fhb_start_v1.jar
queue=root.cpc.bigdata
queue=root.cpc.bigdata
queue=root.cpc.develop
jars=("/home/cpc/anal/lib/spark-tensorflow-connector_2.11-1.10.0.jar" )

randjar="fhb_start"`date +%s%N`".jar"
hadoop fs -get ${jarLib} ${randjar}

delete_old=true

spark-submit --master yarn --queue ${queue} \
    --name "collect-inc-data" \
    --driver-memory 8g --executor-memory 4g \
    --num-executors 1000 --executor-cores 4 \
    --conf spark.hadoop.fs.defaultFS=hdfs://emr-cluster2 \
    --conf "spark.yarn.executor.memoryOverhead=4g" \
    --conf "spark.sql.shuffle.partitions=500" \
    --jars $( IFS=$','; echo "${jars[*]}" ) \
    --class com.cpc.spark.ml.dnn.baseData.CollectIncData\
    ${randjar} ${hdfs_path_model} ${last_model_instances} ${last_daily_instances} ${curr_date}

rm ${shell_in_run}

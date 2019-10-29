#!/usr/bin/env bash
#fenghuabin@qutoutiao.net

source /etc/profile

date_full=`date`
#printf "*****************************${date_full}********************************\n"

now_hour=$(date "+%H")
now_minutes=$(date "+%M")
now_id="00"${now_hour}
if [ ${now_minutes} -ge 30 ];then
    now_id="30"${now_hour}
fi
#printf "now id is:%s\n" ${now_id}

dir=base_daily_weight
if [[ ! -d "${dir}" ]]; then
    mkdir ${dir}
fi

shell_in_run=${dir}/shell_in_busy
if [[ -f "$shell_in_run" ]]; then
    #printf "shell are busy now, existing\n"
    #printf "*****************************${date_full}********************************\n"
    exit 0
fi
touch ${shell_in_run}

des_dir="hdfs://emr-cluster/user/cpc/fenghuabin/adlist-v4-transformer"
last_date=`date --date='1 days ago' +%Y-%m-%d`
des_file=${des_dir}/${last_date}-weight-aggr
#ctr_file=${des_dir}/${last_date}-23-ctr
des_file_success=${dir}/${last_date}_weight_success
des_file_count=${dir}/${last_date}_weight_count
#ctr_file_success=${dir}/${last_date}_ctr_success

#rm ${ctr_file_success}
rm ${des_file_success}
rm ${des_file_count}

#if [[ ! -f ${ctr_file_success} ]]; then
#    hadoop fs -get ${ctr_file}/_SUCCESS ${ctr_file_success} &
#fi
if [[ ! -f ${des_file_success} ]]; then
    hadoop fs -get ${des_file}/_SUCCESS ${des_file_success} &
fi
if [[ ! -f ${des_file_count} ]]; then
    hadoop fs -get ${des_file}/count ${des_file_count} &
fi
wait
run=true
#if [[ ! -f ${ctr_file_success} ]]; then
#    printf "no ${ctr_file_success}...\n"
#    run=true
#fi
if [[ ! -f ${des_file_success} ]]; then
    printf "no ${des_file_success}...\n"
    run=true
fi
if [[ ! -f ${des_file_count} ]]; then
    printf "no ${des_file_count}...\n"
    run=true
fi

if [ "${run}" = "false"  ];then
    echo "make ${last_date} weight file done, exit..."
    rm ${shell_in_run}
    exit 0
fi


aggr_path="hdfs://emr-cluster/user/cpc/aiclk_dataflow/daily/adlist-v4/${last_date}"
aggr_path="hdfs://emr-cluster/user/cpc/fenghuabin/rockefeller_backup/${last_date}-aggr"
file_success=${dir}/${last_date}_aggr_success
file_count=${dir}/${last_date}_aggr_count
file_part=${dir}/${last_date}_aggr_part_r_00999

if [[ ! -f ${file_success} ]]; then
    hadoop fs -get ${aggr_path}/_SUCCESS ${file_success} &
fi
if [[ ! -f ${file_count} ]]; then
    hadoop fs -get ${aggr_path}/count ${file_count} &
fi
if [[ ! -f ${file_part} ]]; then
    hadoop fs -get ${aggr_path}/part-r-00999 ${file_part} &
fi
wait
if [[ ! -f ${file_success} ]]; then
    printf "no ${file_success}, exiting...\n"
    rm ${shell_in_run}
    exit 0
fi
if [[ ! -f ${file_count} ]]; then
    printf "no ${file_count}, exiting...\n"
    rm ${shell_in_run}
    exit 0
fi
if [[ ! -f ${file_part} ]]; then
    printf "no ${file_part}, exiting...\n"
    rm ${shell_in_run}
    exit 0
fi

file_size=`ls -l ${file_part} | awk '{ print $5 }'`
if [ ${file_size} -lt 10000000 ]
then
    printf "invalid ${file_part} file size:${file_size}, exit...\n"
    rm ${shell_in_run}
    exit 0
fi

sample_list=(
    `date --date='1 days ago' +%Y-%m-%d`
)


collect_file=()
collect_date=()
for idx in "${!sample_list[@]}";
do
    echo "++++++++++++++++++++++++++++++++++++++++++++++++++++++++++"
    curr_date="${sample_list[$idx]}"
    echo "curr_date:${curr_date}"
    aggr_path="hdfs://emr-cluster/user/cpc/aiclk_dataflow/daily/adlist-v4/${curr_date}"
    aggr_path="hdfs://emr-cluster/user/cpc/fenghuabin/rockefeller_backup/${curr_date}-aggr"
    echo ${aggr_path}
    file_success=${dir}/${curr_date}_aggr_success
    file_count=${dir}/${curr_date}_aggr_count
    file_part=${dir}/${last_date}_aggr_part_r_00999
    if [[ ! -f ${file_success} ]]; then
        hadoop fs -get ${aggr_path}/_SUCCESS ${file_success} &
    fi
    if [[ ! -f ${file_count} ]]; then
        hadoop fs -get ${aggr_path}/count ${file_count} &
    fi
    if [[ ! -f ${file_part} ]]; then
        hadoop fs -get ${aggr_path}/part-r-00999 ${file_part} &
    fi
    wait
    if [[ ! -f ${file_success} ]]; then
        printf "no ${file_success}, continue...\n"
        continue
    fi
    if [[ ! -f ${file_count} ]]; then
        printf "no ${file_count}, continue...\n"
        continue
    fi
    if [[ ! -f ${file_part} ]]; then
        printf "no ${file_part}, exiting...\n"
        continue
    fi

    file_size=`ls -l ${file_part} | awk '{ print $5 }'`
    if [ ${file_size} -lt 10000000 ]
    then
        printf "invalid ${file_part} file size:${file_size}, exit...\n"
        continue
    fi

    collect_file+=("${aggr_path}/part-r-*")
    collect_date+=(${curr_date})
    if [[ ${#collect_file[@]} -eq 4 ]] ; then
        printf "got 4 days' aggr file, break...\n"
        break
    fi
done

if [[ ${#collect_file[@]} -lt 4 ]] ; then
    printf "not 14 days' aggr file, existing...\n"
    rm ${shell_in_run}
    exit 0
fi

file_list="$( IFS=$','; echo "${collect_file[*]}" )"
date_list="$( IFS=$';'; echo "${collect_date[*]}" )"
train_list="$( IFS=$';'; echo "${collect_file[*]}" )"

echo "++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++"
echo "${file_list}"
echo "++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++"
echo "${date_list}"
echo "++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++"
echo "${train_list}"
echo "++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++"

#rm ${shell_in_run}
#exit 0

jarLib=hdfs://emr-cluster/warehouse/azkaban/lib/fhb_start_v1.jar
queue=root.cpc.develop
queue=root.cpc.bigdata
jars=("/home/cpc/anal/lib/spark-tensorflow-connector_2.11-1.10.0.jar" )

randjar="fhb_start"`date +%s%N`".jar"
hadoop fs -get ${jarLib} ${randjar}


delete_old=true
curr_date=`date --date='0 days ago' +%Y-%m-%d`

spark-submit --master yarn --queue ${queue} \
    --name "make-base-daily-samples" \
    --driver-memory 4g --executor-memory 2g \
    --num-executors 2000 --executor-cores 4 \
    --conf spark.hadoop.fs.defaultFS=hdfs://emr-cluster2 \
    --conf "spark.yarn.executor.memoryOverhead=4g" \
    --conf "spark.sql.shuffle.partitions=500" \
    --jars $( IFS=$','; echo "${jars[*]}" ) \
    --class com.cpc.spark.ml.dnn.baseData.MakeBaseDailyWeight\
    ${randjar} ${des_dir} ${file_list} ${curr_date} ${delete_old} ${train_list} ${date_list}

#chmod_des="hdfs://emr-cluster/user/cpc/fenghuabin/adli}

#busy=${dir}/${busy_file}
#touch ${busy}
#hadoop fs -put ${busy} ${remote_busy}
#hadoop fs -chmod 0777 ${remote_busy}

rm ${shell_in_run}


#!/usr/bin/env bash
#fenghuabin@qutoutiao.net

source /etc/profile

date_full=`date`
printf "*****************************${date_full}********************************\n"

now_hour=$(date "+%H")
now_minutes=$(date "+%M")
now_id="00"${now_hour}
if [ ${now_minutes} -ge 30 ];then
    now_id="30"${now_hour}
fi
printf "now id is:%s\n" ${now_id}

dir=aggr_rock_samples
if [[ ! -d "${dir}" ]]; then
    mkdir ${dir}
fi

shell_in_run=${dir}/shell_in_busy
if [[ -f "$shell_in_run" ]]; then
    printf "shell are busy now, existing\n"
    printf "*****************************${date_full}********************************\n"
    exit 0
fi
touch ${shell_in_run}


last_date=`date --date='1 days ago' +%Y-%m-%d`
aggr_path="hdfs://emr-cluster/user/cpc/fenghuabin/rockefeller_backup/${last_date}-aggr"
file_success=${dir}/${last_date}_aggr_success
file_count=${dir}/${last_date}_aggr_count

if [[ ! -f ${file_success} ]]; then
    hadoop fs -get ${aggr_path}/_SUCCESS ${file_success} &
fi
if [[ ! -f ${file_count} ]]; then
    hadoop fs -get ${aggr_path}/count ${file_count} &
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

sample_list=(
    `date --date='1 days ago' +%Y-%m-%d`
    `date --date='2 days ago' +%Y-%m-%d`
    `date --date='3 days ago' +%Y-%m-%d`
    `date --date='4 days ago' +%Y-%m-%d`
    `date --date='5 days ago' +%Y-%m-%d`
    `date --date='6 days ago' +%Y-%m-%d`
    `date --date='7 days ago' +%Y-%m-%d`
    `date --date='8 days ago' +%Y-%m-%d`
    `date --date='9 days ago' +%Y-%m-%d`
    `date --date='10 days ago' +%Y-%m-%d`
    `date --date='11 days ago' +%Y-%m-%d`
    `date --date='12 days ago' +%Y-%m-%d`
    `date --date='13 days ago' +%Y-%m-%d`
    `date --date='14 days ago' +%Y-%m-%d`
    `date --date='15 days ago' +%Y-%m-%d`
    `date --date='16 days ago' +%Y-%m-%d`
    `date --date='17 days ago' +%Y-%m-%d`
    `date --date='18 days ago' +%Y-%m-%d`
    `date --date='19 days ago' +%Y-%m-%d`
    `date --date='20 days ago' +%Y-%m-%d`
    `date --date='21 days ago' +%Y-%m-%d`
)


collect_file=()
collect_date=()
for idx in "${!sample_list[@]}";
do
    echo "++++++++++++++++++++++++++++++++++++++++++++++++++++++++++"
    curr_date="${sample_list[$idx]}"
    echo "curr_date:${curr_date}"
    aggr_path="hdfs://emr-cluster/user/cpc/fenghuabin/rockefeller_backup/${curr_date}-aggr"
    echo ${aggr_path}
    file_success=${dir}/${curr_date}_aggr_success
    file_count=${dir}/${curr_date}_aggr_count
    if [[ ! -f ${file_success} ]]; then
        hadoop fs -get ${aggr_path}/_SUCCESS ${file_success} &
    fi
    if [[ ! -f ${file_count} ]]; then
        hadoop fs -get ${aggr_path}/count ${file_count} &
    fi
    wait
    if [[ ! -f ${file_success} ]]; then
        printf "no ${file_success}, continue...\n"
    fi
    if [[ ! -f ${file_count} ]]; then
        printf "no ${file_count}, continue...\n"
    fi
    collect_file+=("${aggr_path}/part-r-*")
    collect_date+=(${curr_date})
done

if [[ ${#collect_file[@]} -le 0 ]] ; then
    printf "no aggr training data file, existing...\n"
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
queue=root.cpc.bigdata
queue=root.cpc.develop
jars=("/home/cpc/anal/lib/spark-tensorflow-connector_2.11-1.10.0.jar" )

randjar="fhb_start"`date +%s%N`".jar"
hadoop fs -get ${jarLib} ${randjar}

des_dir="hdfs://emr-cluster/user/cpc/fenghuabin/adlist-v4-transformer"

delete_old=true
curr_date=`date --date='0 days ago' +%Y-%m-%d`

spark-submit --master yarn --queue ${queue} \
    --name "make-base-daily-samples" \
    --driver-memory 8g --executor-memory 6g \
    --num-executors 1000 --executor-cores 3 \
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


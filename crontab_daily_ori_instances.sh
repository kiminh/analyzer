#!/usr/bin/env bash
#fenghuabin@qutoutiao.net

source /etc/profile

date_full=`date`
printf "*****************************${date_full}********************************\n"

dir=daily_ori_instances
if [[ ! -d "${dir}" ]]; then
    mkdir ${dir}
fi

shell_in_run=${dir}/shell_in_busy
if [[ -f "$shell_in_run" ]]; then
    printf "shell are busy now, existing\n"
    exit 0
fi
touch ${shell_in_run}



last_date=`date --date='1 days ago' +%Y-%m-%d`
aggr_path="hdfs://emr-cluster/user/cpc/aiclk_dataflow/daily/adlist-v4/${last_date}"
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


valid_data=()
valid_date=()
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
)

for idx in "${!sample_list[@]}";
do
    curr_date="${sample_list[$idx]}"
    echo "curr_date:${curr_date}"

    prefix="hdfs://emr-cluster/user/cpc/aiclk_dataflow/daily/adlist-v4/${curr_date}"
    end=part-*

    file_count=${dir}/${curr_date}_${curr_date}_count
    file_success=${dir}/${curr_date}_${curr_date}_success
    file_part=${dir}/${curr_date}_${curr_date}_part-r-00099

    if [[ ! -f ${file_count} ]]; then
        hadoop fs -get ${prefix}/count ${file_count} &
    fi
    if [[ ! -f ${file_success} ]]; then
        hadoop fs -get ${prefix}/_SUCCESS ${file_success} &
    fi

    printf "waiting for downloading real-time data in parallel...\n"
    wait
    printf "downloaded real-time data file in parallel...\n"


    if [[ ! -f ${file_count} ]]; then
        printf "no ${file_count} file, continue...\n"
        continue
    fi
    if [[ ! -f ${file_success} ]]; then
        printf "no ${file_success} file, continue...\n"
        continue
    fi

    #file_size=`ls -l ${file_part} | awk '{ print $5 }'`
    #if [ ${file_size} -lt 1000 ]
    #then
    #    printf "invalid ${file_part} file size:${file_size}, continue...\n"
    #    continue
    #fi

    valid_data+=("${prefix}/${end}")
    valid_date+=("${curr_date}")
    rm ${file_count}
    rm ${file_part}
    if [[ ${#valid_data[@]} -eq 14 ]] ; then
        break
    fi
done

if [[ ${#valid_data[@]} -le 0 ]] ; then
    printf "no daily training data file found, existing...\n"
    rm ${shell_in_run}
    exit 0
fi

date_list="$( IFS=$';'; echo "${valid_date[*]}" )"
file_list="$( IFS=$';'; echo "${valid_data[*]}" )"

echo "${file_list}"
echo "++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++"
echo "${date_list}"

date_last=`date --date='1 days ago' +%Y-%m-%d`
instances_all_list=(
    `date --date='1 days ago' +%Y-%m-%d`
    `date --date='3 days ago' +%Y-%m-%d`
    `date --date='7 days ago' +%Y-%m-%d`
    `date --date='14 days ago' +%Y-%m-%d`
)
instances_begin_date_list="$( IFS=$';'; echo "${instances_all_list[*]}" )"

jarLib=hdfs://emr-cluster/warehouse/azkaban/lib/fhb_start_v1.jar
queue=root.cpc.bigdata
queue=root.cpc.develop
jars=("/home/cpc/anal/lib/spark-tensorflow-connector_2.11-1.10.0.jar" )

randjar="fhb_start"`date +%s%N`".jar"
hadoop fs -get ${jarLib} ${randjar}

des_dir="hdfs://emr-cluster/user/cpc/fenghuabin/adlist-v4-daily-instances"

spark-submit --master yarn --queue ${queue} \
    --name "adlist-v4-daily-ori-instances" \
    --driver-memory 4g --executor-memory 4g \
    --num-executors 1000 --executor-cores 4 \
    --conf spark.hadoop.fs.defaultFS=hdfs://emr-cluster2 \
    --conf "spark.yarn.executor.memoryOverhead=4g" \
    --conf "spark.sql.shuffle.partitions=500" \
    --jars $( IFS=$','; echo "${jars[*]}" ) \
    --class com.cpc.spark.ml.dnn.baseData.MakeDailyInstances\
    ${randjar} ${des_dir} ${date_list} ${file_list} ${date_last} ${instances_begin_date_list}


rm ${shell_in_run}

rm ${shell_in_run}


#-p GIT_NAME="git@git.qutoutiao.net:fenghuabin/dl-demo-refactor.git" \
#-p SCM_REVISION="master" \

#-p GIT_NAME="git@git.qutoutiao.net:songchengru/dl-demo.git" \
#-p SCM_REVISION="fhb_start" \


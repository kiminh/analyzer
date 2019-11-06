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

curr_date=`date --date='0 days ago' +%Y-%m-%d`
dir=aggr_rock_samples
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


sample_list=(
    `date --date='1 days ago' +%Y-%m-%d`
)


collect_file=()
collect_date=()
for idx in "${!sample_list[@]}";
do
    echo "++++++++++++++++++++++++++++++++++++++++++++++++++++++++++"
    valid_data=()
    curr_date="${sample_list[$idx]}"
    echo "curr_date:${curr_date}"

    aggr_path="hdfs://emr-cluster/user/cpc/fenghuabin/rockefeller_backup/${curr_date}-aggr"
    instances_path="hdfs://emr-cluster/user/cpc/fenghuabin/rockefeller_backup/${curr_date}-instances"

    echo ${aggr_path}
    echo ${instances_path}
    file_success_instances=${dir}/${curr_date}_instances_success
    file_success=${dir}/${curr_date}_aggr_success
    file_count=${dir}/${curr_date}_aggr_count
    #file_part=${dir}/${curr_date}_aggr_part-r-00099

    if [[ ! -f ${file_success_instances} ]]; then
        hadoop fs -get ${instances_path}/_SUCCESS ${file_success_instances} &
    fi
    if [[ ! -f ${file_success} ]]; then
        hadoop fs -get ${aggr_path}/_SUCCESS ${file_success} &
    fi
    if [[ ! -f ${file_count} ]]; then
        hadoop fs -get ${aggr_path}/count ${file_count} &
    fi
    #if [[ ! -f ${file_part} ]]; then
    #    hadoop fs -get ${aggr_path}/part-r-00099 ${file_part} &
    #fi

    wait

    done_aggr="true"
    done_instance="true"

    if [[ ! -f ${file_success_instances} ]]; then
        printf "no ${file_success_instances}, continue to instances ${curr_date}...\n"
        done_instance="false"
    fi
    if [[ ! -f ${file_success} ]]; then
        printf "no ${file_success}, continue to aggr ${curr_date}...\n"
        done_aggr="false"
    fi
    if [[ ! -f ${file_count} ]]; then
        printf "no ${file_count}, continue to aggr ${curr_date}...\n"
        done_aggr="false"
    fi

    #if [[ ! -f ${file_part} ]]; then
    #    printf "no ${file_part}, continue to aggr ${curr_date}...\n"
    #    done_aggr="false"
    #fi

    #if [[ -f ${file_part} ]]; then
    #    file_size=`ls -l ${file_part} | awk '{ print $5 }'`
    #    if [ ${file_size} -lt 1000 ]
    #    then
    #        printf "invalid ${file_part} file size:${file_size}, continue to aggr ${curr_date}...\n"
    #        done_aggr="false"
    #    fi
    #fi

    if [ "${done_aggr}" = "true" ];then
        if [ "${done_instance}" = "true" ];then
            printf "curr_date ${curr_date} has aggr file, continue...\n"
            printf "curr_date ${curr_date} has instances file, continue...\n"
            continue
        fi
    fi


    id_list=( "0000" "3000" "0001" "3001" "0002" "3002" "0003" "3003" "0004" "3004" "0005" "3005" "0006" "3006" "0007" "3007" "0008" "3008" "0009" "3009" "0010" "3010" "0011" "3011" "0012" "3012" "0013" "3013" "0014" "3014" "0015" "3015" "0016" "3016" "0017" "3017" "0018" "3018" "0019" "3019" "0020" "3020" "0021" "3021" "0022" "3022" "0023" "3023" )
    #prefix=hdfs://emr-cluster/user/cpc/fenghuabin/rockefeller_backup/${curr_date}
    prefix=hdfs://emr-cluster2ns2/user/cpc_tensorflow_example_half/${curr_date}
    end=part-*

    realtime_list=(
        ${prefix}"/00/0/"
        ${prefix}"/00/1/"
        ${prefix}"/01/0/"
        ${prefix}"/01/1/"
        ${prefix}"/02/0/"
        ${prefix}"/02/1/"
        ${prefix}"/03/0/"
        ${prefix}"/03/1/"
        ${prefix}"/04/0/"
        ${prefix}"/04/1/"
        ${prefix}"/05/0/"
        ${prefix}"/05/1/"
        ${prefix}"/06/0/"
        ${prefix}"/06/1/"
        ${prefix}"/07/0/"
        ${prefix}"/07/1/"
        ${prefix}"/08/0/"
        ${prefix}"/08/1/"
        ${prefix}"/09/0/"
        ${prefix}"/09/1/"
        ${prefix}"/10/0/"
        ${prefix}"/10/1/"
        ${prefix}"/11/0/"
        ${prefix}"/11/1/"
        ${prefix}"/12/0/"
        ${prefix}"/12/1/"
        ${prefix}"/13/0/"
        ${prefix}"/13/1/"
        ${prefix}"/14/0/"
        ${prefix}"/14/1/"
        ${prefix}"/15/0/"
        ${prefix}"/15/1/"
        ${prefix}"/16/0/"
        ${prefix}"/16/1/"
        ${prefix}"/17/0/"
        ${prefix}"/17/1/"
        ${prefix}"/18/0/"
        ${prefix}"/18/1/"
        ${prefix}"/19/0/"
        ${prefix}"/19/1/"
        ${prefix}"/20/0/"
        ${prefix}"/20/1/"
        ${prefix}"/21/0/"
        ${prefix}"/21/1/"
        ${prefix}"/22/0/"
        ${prefix}"/22/1/"
        ${prefix}"/23/0/"
        ${prefix}"/23/1/"
    )

    for idx in "${!realtime_list[@]}";
    do
        p00="${realtime_list[$idx]}"
        id="${id_list[$idx]}"

        is_new=${dir}/train_done_${curr_date}_${id}
        if [[ -f "$is_new" ]]; then
            continue
        fi

        file_part1=${dir}/${curr_date}_${id}_part-0-0
        file_part2=${dir}/${curr_date}_${id}_part-99-0

        if [[ ! -f ${file_part1} ]]; then
            hadoop fs -get ${p00}part-0-0 ${file_part1} &
        fi

        if [[ ! -f ${file_part1} ]]; then
            hadoop fs -get ${p00}part-99-0 ${file_part2} &
        fi
    done

    printf "waiting for downloading real-time data in parallel...\n"
    wait
    printf "downloaded real-time data file in parallel...\n"

    for idx in "${!realtime_list[@]}";
    do
        p00=${realtime_list[$idx]}${end}
        id="${id_list[$idx]}"

        if [ "${now_hour}" = "15" ];then
            continue
        fi
        if [ "${now_hour}" = "16" ];then
            continue
        fi
        if [ "${now_hour}" = "17" ];then
            continue
        fi
        if [ "${now_hour}" = "18" ];then
            continue
        fi

        is_new=${dir}/train_done_${curr_date}_${id}
        if [[ -f "$is_new" ]]; then
            valid_data+=(${p00})
            continue
        fi

        file_part1=${dir}/${curr_date}_${id}_part-0-0
        file_part2=${dir}/${curr_date}_${id}_part-99-0

        if [[ ! -f ${file_part1} ]]; then
            printf "no ${file_part1} file, continue...\n"
            continue
        fi

        if [[ ! -f ${file_part2} ]]; then
            printf "no ${file_part2} file, continue...\n"
            continue
        fi

        file_size=`ls -l ${file_part1} | awk '{ print $5 }'`
        if [ ${file_size} -lt 1000 ]
        then
            printf "invalid ${file_part1} file size:${file_size}, continue...\n"
            continue
        fi

        file_size=`ls -l ${file_part2} | awk '{ print $5 }'`
        if [ ${file_size} -lt 1000 ]
        then
            printf "invalid ${file_part2} file size:${file_size}, continue...\n"
            continue
        fi

        touch ${is_new}
        valid_data+=(${p00})
        rm ${file_part1}
        rm ${file_part2}
    done
    echo "++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++"
    echo "collected "${#valid_data[@]}" real-time training data file for ${curr_date}"
    train_file="$( IFS=$','; echo "${valid_data[*]}" )"
    #echo ${train_file}
    echo "++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++"

    if [ "${done_aggr}" = "true" ];then
        printf "curr_date ${curr_date} has aggr file but no instances file\n"
        if [[ ${#valid_data[@]} -eq 48 ]] ; then
            printf "original real-time files add to collect and continue...\n"
            collect_date+=(${curr_date})
            collect_file+=(${train_file})
            continue
        fi
        printf "aggr add to collect and continue...\n"
        collect_date+=(${curr_date})
        collect_file+=(${aggr_path}/part-*)
        continue
    fi

    real_curr_date=`date --date='1 days ago' +%Y-%m-%d`

    if [ "${curr_date}" != "${real_curr_date}" ];then
        printf "curr_date ${curr_date} not really today ${real_curr_date} add to collect and continue...\n"
        collect_date+=(${curr_date})
        collect_file+=(${train_file})
        continue
    fi

    if [ "${now_id}" != "0000" ];then
        printf "curr_date ${curr_date} is really today ${real_curr_date}, but now_id=${now_id}, add to collect and continue...\n"
        collect_date+=(${curr_date})
        collect_file+=(${train_file})
        continue
    fi

    if [[ ${#valid_data[@]} -eq 48 ]] ; then
        printf "curr_date ${curr_date} is really today ${real_curr_date}, and now_id=${now_id}, but valid_data.size=48, add to collect and continue...\n"
        collect_date+=(${curr_date})
        collect_file+=(${train_file})
        continue
    fi

    if [[ -f ${dir}/train_done_${curr_date}_3023 ]]; then
        printf "detected ${dir}/train_done_${curr_date}_3023, curr_date ${curr_date} is really today ${real_curr_date}, and now_id=${now_id}, but valid_data.size!=48, add to collect and continue...\n"
        collect_date+=(${curr_date})
        collect_file+=(${train_file})
        continue
    fi

    printf "curr_date ${curr_date} is really today ${real_curr_date}, and now_id=${now_id}, but valid_data.size=${#valid_data[@]}, add nothing and continue...\n"

done



if [[ ${#collect_file[@]} -le 0 ]] ; then
    printf "no real-time training data file need to be aggr, existing...\n"
    rm ${shell_in_run}
    exit 0
fi

date_list="$( IFS=$';'; echo "${collect_date[*]}" )"
file_list="$( IFS=$';'; echo "${collect_file[*]}" )"
echo "${file_list}"
echo "++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++"
echo "${date_list}"

date_last=`date --date='1 days ago' +%Y-%m-%d`
instances_all_list=(
    `date --date='14 days ago' +%Y-%m-%d`
    `date --date='7 days ago' +%Y-%m-%d`
    `date --date='5 days ago' +%Y-%m-%d`
    `date --date='3 days ago' +%Y-%m-%d`
    `date --date='1 days ago' +%Y-%m-%d`
)
instances_begin_date_list="$( IFS=$';'; echo "${instances_all_list[*]}" )"

jarLib=hdfs://emr-cluster/warehouse/azkaban/lib/fhb_start_v1.jar
queue=root.cpc.develop
queue=root.cpc.bigdata
jars=("/home/cpc/anal/lib/spark-tensorflow-connector_2.11-1.10.0.jar" )

randjar="fhb_start"`date +%s%N`".jar"
hadoop fs -get ${jarLib} ${randjar}

des_dir="hdfs://emr-cluster/user/cpc/fenghuabin/rockefeller_backup"
date_curr=`date --date='0 days ago' +%Y-%m-%d`

spark-submit --master yarn --queue ${queue} \
    --name "adlist-v4-aggr-samples" \
    --driver-memory 4g --executor-memory 4g \
    --num-executors 1000 --executor-cores 4 \
    --conf spark.hadoop.fs.defaultFS=hdfs://emr-cluster2 \
    --conf "spark.yarn.executor.memoryOverhead=4g" \
    --conf "spark.sql.shuffle.partitions=500" \
    --jars $( IFS=$','; echo "${jars[*]}" ) \
    --class com.cpc.spark.ml.dnn.baseData.AggrAdListV4Samples\
    ${randjar} ${des_dir} ${date_list} ${file_list} ${date_last} ${date_curr} ${instances_begin_date_list}


rm ${shell_in_run}

#!/usr/bin/env bash
#fenghuabin@qutoutiao.net

source /etc/profile

date_full=`date`
printf "*****************************${date_full}********************************\n"

curr_date=`date --date='0 days ago' +%Y-%m-%d`
dir=aggr_rock_samples
if [[ ! -d "${dir}" ]]; then
    mkdir ${dir}
fi

sample_list=(
    `date --date='1 days ago' +%Y-%m-%d`
)

collect_file=()
collect_date=()
for idx in "${!sample_list[@]}";
do
    valid_data=()
    curr_date="${sample_list[$idx]}"
    echo "curr_date:${curr_date}"


    id_list=( "0000" "3000" "0001" "3001" "0002" "3002" "0003" "3003" "0004" "3004" "0005" "3005" "0006" "3006" "0007" "3007" "0008" "3008" "0009" "3009" "0010" "3010" "0011" "3011" "0012" "3012" "0013" "3013" "0014" "3014" "0015" "3015" "0016" "3016" "0017" "3017" "0018" "3018" "0019" "3019" "0020" "3020" "0021" "3021" "0022" "3022" "0023" "3023" )
    #prefix=hdfs://emr-cluster2ns2/user/cpc_tensorflow_example_half/${curr_date}
    prefix=hdfs://emr-cluster/user/cpc/fenghuabin/rockefeller_backup/${curr_date}
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
    echo "collected "${#valid_data[@]}" real-time training data file for ${curr_date}"
    train_file="$( IFS=$','; echo "${valid_data[*]}" )"

    if [[ ${#valid_data[@]} -le 48 ]] ; then
        printf "too less real-time data detected, less than 48, for ${curr_date}, continue...\n"
        continue
    fi
    #printf "${train_file}\n"
    collect_date+=(${curr_date})
    collect_file+=(${train_file})
done

date_list="$( IFS=$';'; echo "${collect_date[*]}" )"
file_list="$( IFS=$';'; echo "${collect_file[*]}" )"
echo "${date_list}"
echo "${file_list}"

jarLib=hdfs://emr-cluster/warehouse/azkaban/lib/fhb_start_v1.jar
queue=root.cpc.bigdata
jars=("/home/cpc/anal/lib/spark-tensorflow-connector_2.11-1.10.0.jar" )

randjar="fhb_start"`date +%s%N`".jar"
hadoop fs -get ${jarLib} ${randjar}

des_dir="hdfs://emr-cluster/user/cpc/fenghuabin/rockefeller_backup"

spark-submit --master yarn --queue ${queue} \
    --name "adlist-v4-aggr-samples" \
    --driver-memory 4g --executor-memory 2g \
    --num-executors 1000 --executor-cores 2 \
    --conf spark.hadoop.fs.defaultFS=hdfs://emr-cluster2 \
    --conf "spark.yarn.executor.memoryOverhead=4g" \
    --conf "spark.sql.shuffle.partitions=500" \
    --jars $( IFS=$','; echo "${jars[*]}" ) \
    --class com.cpc.spark.ml.dnn.baseData.AggrAdListV4Samples\
    ${randjar} ${des_dir} ${date_list} ${file_list}



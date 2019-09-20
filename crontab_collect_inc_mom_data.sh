#!/usr/bin/env bash
#fenghuabin@qutoutiao.net


source /etc/profile

ml_name=adlist
ml_ver=v4refult

date_full=`date`
printf "*****************************${date_full}********************************\n"

curr_date=`date --date='0 days ago' +%Y-%m-%d`
printf "now curr_date is:${curr_date}\n"

now_hour=$(date "+%H")
now_minutes=$(date "+%M")
now_id="00"${now_hour}
if [ ${now_minutes} -ge 30 ];then
    now_id="30"${now_hour}
fi
printf "now id is:%s\n" ${now_id}

dir=collect_inc_mom
if [[ ! -d "${dir}" ]]; then
    mkdir ${dir}
fi
printf "now id is:%s\n" ${now_id}

shell_in_run=${dir}/shell_in_busy
if [[ -f "$shell_in_run" ]]; then
    printf "shell are busy now, existing\n"
    exit 0
fi
touch ${shell_in_run}

id_list=( "0000" "3000" "0001" "3001" "0002" "3002" "0003" "3003" "0004" "3004" "0005" "3005" "0006" "3006" "0007" "3007" "0008" "3008" "0009" "3009" "0010" "3010" "0011" "3011" "0012" "3012" "0013" "3013" "0014" "3014" "0015" "3015" "0016" "3016" "0017" "3017" "0018" "3018" "0019" "3019" "0020" "3020" "0021" "3021" "0022" "3022" "0023" "3023" )
prefix=hdfs://emr-cluster2ns2/user/cpc_tensorflow_example_half/${curr_date}
end=part-*
sample_list=(
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


now_id=3016
for idx in "${!sample_list[@]}";
do
    p00="${sample_list[$idx]}"
    id="${id_list[$idx]}"

    if [ "${id}" = "${now_id}"  ];then
        echo "id = now_id, break..."
        break
    fi

    is_new=${dir}/train_done_${curr_date}_${id}
    if [[ -f "$is_new" ]]; then
        continue
    fi

    file_count=${dir}/${curr_date}_${id}_count
    file_part=${dir}/${curr_date}_${id}_part-10-0

    if [[ ! -f ${file_count} ]]; then
        hadoop fs -get ${p00}count ${file_count} &
    fi
    if [[ ! -f ${file_part} ]]; then
        hadoop fs -get ${p00}part-10-0 ${file_part} &
    fi
done


printf "waiting for downloading real-time data in parallel...\n"
wait
printf "downloaded real-time data file in parallel...\n"


inc_data=()
done_data=()
all_data=()
for idx in "${!sample_list[@]}";
do
    p00="${sample_list[$idx]}"
    id="${id_list[$idx]}"

    if [ "${id}" = "${now_id}"  ];then
        echo "id = now_id, break..."
        break
    fi

    is_new=${dir}/train_done_${curr_date}_${id}
    if [[ -f "$is_new" ]]; then
        printf "done with ${p00}, continuing\n"
        done_data+=(${p00}${end})
        all_data+=(${p00}${end})
        continue
    fi

    file_count=${dir}/${curr_date}_${id}_count
    file_part=${dir}/${curr_date}_${id}_part-10-0

    if [[ ! -f ${file_count} ]]; then
        printf "no ${file_count} file, continue...\n"
        continue
    fi
    if [[ ! -f ${file_part} ]]; then
        printf "no ${file_part} file, continue...\n"
        continue
    fi

    file_size=`ls -l ${file_part} | awk '{ print $5 }'`
    if [ ${file_size} -lt 1000 ]
    then
        printf "invalid ${file_part} file size:${file_size}, continue...\n"
        continue
    fi

    touch ${is_new}
    rm ${file_count}
    rm ${file_part}

    printf "new inc real-time file ${p00}\n"
    inc_data+=(${p00}${end})
    all_data+=(${p00}${end})
    test_file=${p00}${end}
    last_id=${id}
done

if [[ ${#inc_data[@]} -le 0 ]] ; then
    printf "no incremental real-time training data file detected, existing...\n"
    rm ${shell_in_run}
    exit 0
fi

printf "got ${#inc_data[@]} new collect inc real-time training data file\n"

train_file="$( IFS=$','; echo "${all_data[*]}" )"

#test_file=${prefix}"/12/1/"${end}
printf "train_file:%s\n" ${train_file}
printf "test_file:%s\n" ${test_file}

jarLib=hdfs://emr-cluster/warehouse/azkaban/lib/fhb_start_v1.jar
queue=root.cpc.bigdata
jars=("/home/cpc/anal/lib/spark-tensorflow-connector_2.11-1.10.0.jar" )

randjar="fhb_start"`date +%s%N`".jar"
hadoop fs -get ${jarLib} ${randjar}

des_dir="hdfs://emr-cluster/user/cpc/fenghuabin/adlist-v4-transformer"

spark-submit --master yarn --queue ${queue} \
    --name "adlist-v4-make-samples" \
    --driver-memory 4g --executor-memory 2g \
    --num-executors 1000 --executor-cores 2 \
    --conf spark.hadoop.fs.defaultFS=hdfs://emr-cluster2 \
    --conf "spark.yarn.executor.memoryOverhead=4g" \
    --conf "spark.sql.shuffle.partitions=500" \
    --jars $( IFS=$','; echo "${jars[*]}" ) \
    --class com.cpc.spark.ml.dnn.baseData.MakeAdListV4Samples\
    ${randjar} ${des_dir} ${train_file} ${test_file} ${curr_date} ${last_id}

#chmod_des="hdfs://emr-cluster/user/cpc/fenghuabin/adlist-v4-sampled"
#hadoop fs -chmod -R 0777 ${chmod_des}



rm ${shell_in_run}

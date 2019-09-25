#!/usr/bin/env bash
#fenghuabin@qutoutiao.net


source /etc/profile

ml_name=adlist
ml_ver=v4refult

date_full=`date`
printf "*****************************${date_full}********************************\n"

dir=collect_inc_mom
if [[ ! -d "${dir}" ]]; then
    mkdir ${dir}
fi

shell_in_run=${dir}/shell_in_busy
if [[ -f "$shell_in_run" ]]; then
    printf "shell are busy now, existing\n"
    exit 0
fi
touch ${shell_in_run}


id_list=( "0000" "3000" "0001" "3001" "0002" "3002" "0003" "3003" "0004" "3004" "0005" "3005" "0006" "3006" "0007" "3007" "0008" "3008" "0009" "3009" "0010" "3010" "0011" "3011" "0012" "3012" "0013" "3013" "0014" "3014" "0015" "3015" "0016" "3016" "0017" "3017" "0018" "3018" "0019" "3019" "0020" "3020" "0021" "3021" "0022" "3022" "0023" "3023" )
end=part-*
sample_list=(
    "/00/0/"
    "/00/1/"
    "/01/0/"
    "/01/1/"
    "/02/0/"
    "/02/1/"
    "/03/0/"
    "/03/1/"
    "/04/0/"
    "/04/1/"
    "/05/0/"
    "/05/1/"
    "/06/0/"
    "/06/1/"
    "/07/0/"
    "/07/1/"
    "/08/0/"
    "/08/1/"
    "/09/0/"
    "/09/1/"
    "/10/0/"
    "/10/1/"
    "/11/0/"
    "/11/1/"
    "/12/0/"
    "/12/1/"
    "/13/0/"
    "/13/1/"
    "/14/0/"
    "/14/1/"
    "/15/0/"
    "/15/1/"
    "/16/0/"
    "/16/1/"
    "/17/0/"
    "/17/1/"
    "/18/0/"
    "/18/1/"
    "/19/0/"
    "/19/1/"
    "/20/0/"
    "/20/1/"
    "/21/0/"
    "/21/1/"
    "/22/0/"
    "/22/1/"
    "/23/0/"
    "/23/1/"
)

echo "++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++"
curr_date=`date --date='1 days ago' +%Y-%m-%d`
printf "last_date is:${curr_date}\n"
prefix=hdfs://emr-cluster2ns2/user/cpc_tensorflow_example_half/${curr_date}
for idx in "${!sample_list[@]}";
do
    p00=${prefix}"${sample_list[$idx]}"
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
all_data=()
for idx in "${!sample_list[@]}";
do
    p00=${prefix}"${sample_list[$idx]}"
    id="${id_list[$idx]}"

    is_new=${dir}/train_done_${curr_date}_${id}
    if [[ -f "$is_new" ]]; then
        printf "detected real-time file ${p00}\n"
        all_data+=(${p00}${end})
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
    rm ${file_part1}
    rm ${file_part2}

    printf "detected real-time file ${p00}\n"
    all_data+=(${p00}${end})
done

if [[ ${#all_data[@]} -le 0 ]] ; then
    printf "no history real-time training data file detected, existing...\n"
    rm ${shell_in_run}
    exit 0
fi

printf "got ${#all_data[@]} history real-time training data file\n"
train_file_last="$( IFS=$','; echo "${all_data[*]}" )"

echo "++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++"


curr_date=`date --date='0 days ago' +%Y-%m-%d`
printf "now curr_date is:${curr_date}\n"
now_hour=$(date "+%H")
now_minutes=$(date "+%M")
now_id="00"${now_hour}
if [ ${now_minutes} -ge 30 ];then
    now_id="30"${now_hour}
fi
printf "now id is:%s\n" ${now_id}

prefix=hdfs://emr-cluster2ns2/user/cpc_tensorflow_example_half/${curr_date}
now_id=0010
for idx in "${!sample_list[@]}";
do
    p00=${prefix}"${sample_list[$idx]}"
    id="${id_list[$idx]}"

    if [ "${id}" = "${now_id}"  ];then
        echo "id = now_id, break..."
        break
    fi

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

inc_data=()
done_data=()
all_data=()
for idx in "${!sample_list[@]}";
do
    p00=${prefix}"${sample_list[$idx]}"
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
        test_file=${p00}${end}
        last_id=${id}
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
    rm ${file_part1}
    rm ${file_part2}

    printf "new inc real-time file ${p00}\n"
    inc_data+=(${p00}${end})
    all_data+=(${p00}${end})
    test_file=${p00}${end}
    last_id=${id}
done

if [[ ${#all_data[@]} -le 0 ]] ; then
    printf "no today real-time training data file detected, existing...\n"
    rm ${shell_in_run}
    exit 0
fi

printf "got ${#all_data[@]} today real-time training data file\n"

train_file_curr="$( IFS=$','; echo "${all_data[*]}" )"

train_file=${train_file_last},${train_file_curr}

train_file_latest="$( IFS=$','; echo "${all_data[*]}" )"

if [[ ${#all_data[@]} -gt 8 ]] ; then
    real_data=()
    last=${#all_data[@]}
    for (( idx=last-1 ; idx>=last-8 ; idx-- ));do
        #printf "%s<------->%s\n" "${id_list[i]}" "${sample_list[i]}"
        p00="${all_data[$idx]}"
        id="${id_list[$idx]}"
        real_data+=(${p00})
        printf "add real time file ${p00}, continue...\n"
    done
    printf "got ${#real_data[@]} latest collect inc real-time training data file\n"
    train_file_latest="$( IFS=$','; echo "${real_data[*]}" )"
fi


echo "++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++"

#test_file=${prefix}"/12/1/"${end}
printf "test_file:%s\n" ${test_file}
echo "++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++"
printf "train_file:%s\n" ${train_file}
echo "++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++"
printf "train_file_latest:%s\n" ${train_file_latest}
echo "++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++"

jarLib=hdfs://emr-cluster/warehouse/azkaban/lib/fhb_start_v1.jar
queue=root.cpc.bigdata
queue=root.cpc.develop
jars=("/home/cpc/anal/lib/spark-tensorflow-connector_2.11-1.10.0.jar" )

randjar="fhb_start"`date +%s%N`".jar"
hadoop fs -get ${jarLib} ${randjar}

des_dir="hdfs://emr-cluster/user/cpc/fenghuabin/adlist-v4-transformer"


p00="hdfs://emr-cluster/user/cpc/fenghuabin/rockefeller_backup/2019-09-21-aggr/part-*"
p01="hdfs://emr-cluster/user/cpc/fenghuabin/rockefeller_backup/2019-09-20-aggr/part-*"
p02="hdfs://emr-cluster/user/cpc/fenghuabin/rockefeller_backup/2019-09-19-aggr/part-*"
p03="hdfs://emr-cluster/user/cpc/fenghuabin/rockefeller_backup/2019-09-18-aggr/part-*"
p04="hdfs://emr-cluster/user/cpc/fenghuabin/rockefeller_backup/2019-09-17-aggr/part-*"
p05="hdfs://emr-cluster/user/cpc/fenghuabin/rockefeller_backup/2019-09-16-aggr/part-*"
p06="hdfs://emr-cluster/user/cpc/fenghuabin/rockefeller_backup/2019-09-15-aggr/part-*"
history_file="${p00},${p01},${p02},${p03},${p04},${p05},${p06}"

delete_old=true

spark-submit --master yarn --queue ${queue} \
    --name "adlist-v4-make-samples" \
    --driver-memory 8g --executor-memory 4g \
    --num-executors 1000 --executor-cores 4 \
    --conf spark.hadoop.fs.defaultFS=hdfs://emr-cluster2 \
    --conf "spark.yarn.executor.memoryOverhead=4g" \
    --conf "spark.sql.shuffle.partitions=500" \
    --jars $( IFS=$','; echo "${jars[*]}" ) \
    --class com.cpc.spark.ml.dnn.baseData.MakeAdListV4Samples\
    ${randjar} ${des_dir} ${train_file} ${train_file_latest} ${test_file} ${curr_date} ${last_id} ${history_file} ${delete_old}

#chmod_des="hdfs://emr-cluster/user/cpc/fenghuabin/adli}



rm ${shell_in_run}

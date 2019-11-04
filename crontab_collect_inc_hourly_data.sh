#!/usr/bin/env bash
#fenghuabin@qutoutiao.net


source /etc/profile

ml_name=adlist
ml_ver=v4refult

date_full=`date`
#printf "*****************************${date_full}********************************\n"
curr_date=`date --date='0 days ago' +%Y-%m-%d`
#printf "now curr_date is:${curr_date}\n"
now_hour=$(date "+%H")
now_minutes=$(date "+%M")
now_id="00"${now_hour}
if [ ${now_minutes} -ge 30 ];then
    now_id="30"${now_hour}
fi
#printf "now id is:%s\n" ${now_id}

dir=collect_inc_hourly
if [[ ! -d "${dir}" ]]; then
    mkdir ${dir}
fi

last_incr_error=${dir}/last_incr_error
if [[ -f "$last_incr_error" ]]; then
    printf "detected last incr error, exiting\n"
    exit 0
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

collect_path=${des_dir}/${curr_date}-collect-inc-hourly
hadoop fs -test -e ${collect_path}
if [ $? -eq 0 ] ;then
	echo 'exist directory:'${collect_path}
else
	echo 'non exist directory:'${collect_path}
	echo 'now make dir:'${collect_path}
    hadoop fs -mkdir ${collect_path}
fi

curr_incr_base_model_instances=${hdfs_path_model}/${curr_date}-tf-base-mom-8days-6eps-inc/map_instances.data
hadoop fs -test -s ${curr_incr_base_model_instances}
if [ $? -eq 0 ] ;then
	echo 'exist and more than zero bytes:'${curr_incr_base_model_instances}
else
	echo 'non exist or less than zero bytes:'${curr_incr_base_model_instances}
	rm ${shell_in_run}
	exit 0
fi

curr_collect_base=${collect_path}/hourly_curr_map_instances.data
hadoop fs -test -s ${curr_collect_base}
if [ $? -eq 0 ] ;then
	echo 'exist and more than zero bytes:'${curr_collect_base}
else
	echo 'non exist or less than zero bytes:'${curr_collect_base}
	echo 'now copy from incr model instances:'${curr_incr_base_model_instances}
	hadoop fs -cp ${curr_incr_base_model_instances} ${curr_collect_base}
fi

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

curr_date=`date --date='0 days ago' +%Y-%m-%d`
prefix=hdfs://emr-cluster2ns2/user/cpc_tensorflow_example_half/${curr_date}
#now_id="3012"
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
    file_count=${dir}/${curr_date}_${id}_count

    if [[ ! -f ${file_part1} ]]; then
        hadoop fs -get ${p00}part-0-0 ${file_part1} &
    fi

    if [[ ! -f ${file_part1} ]]; then
        hadoop fs -get ${p00}part-99-0 ${file_part2} &
    fi

    if [[ ! -f ${file_count} ]]; then
        hadoop fs -get ${p00}count ${file_count} &
    fi
done

printf "waiting for downloading real-time data in parallel...\n"
wait
printf "downloaded real-time data file in parallel...\n"

inc_data=()
done_data=()
all_data=()
all_ids=()
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
        #printf "done with ${p00}, continuing\n"
        done_data+=(${p00}${end})
        all_data+=(${p00}${end})
        all_ids+=("${curr_date}-${id}")
        test_file=${p00}${end}
        last_id=${id}
        continue
    fi

    file_part1=${dir}/${curr_date}_${id}_part-0-0
    file_part2=${dir}/${curr_date}_${id}_part-99-0
    file_count=${dir}/${curr_date}_${id}_count

    if [[ ! -f ${file_part1} ]]; then
        printf "no ${file_part1} file, continue...\n"
        continue
    fi
    if [[ ! -f ${file_part2} ]]; then
        printf "no ${file_part2} file, continue...\n"
        continue
    fi
    #if [[ ! -f ${file_count} ]]; then
    #    printf "no ${file_count} file, continue...\n"
    #    continue
    #fi

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
    #rm ${file_count}

    printf "new inc real-time file ${p00}\n"
    inc_data+=(${p00}${end})
    all_data+=(${p00}${end})
    all_ids+=("${curr_date}-${id}")
    test_file=${p00}${end}
    last_id=${id}
done

printf "got ${#inc_data[@]} today incremental real-time training data file\n"
if [[ ${#inc_data[@]} -le 0 ]] ; then
    printf "no today incremental real-time training data file detected, existing...\n"
    rm ${shell_in_run}
    exit 0
fi

printf "got ${#all_data[@]} today total real-time training data file\n"
if [[ ${#all_data[@]} -le 0 ]] ; then
    printf "no today total real-time training data file detected, existing...\n"
    rm ${shell_in_run}
    exit 0
fi

train_file_curr="$( IFS=$','; echo "${all_data[*]}" )"
train_ids_curr="$( IFS=$','; echo "${all_ids[*]}" )"

train_file_collect_1=${train_file_curr}

if [[ ${#all_data[@]} -gt 1 ]] ; then
    real_data=()
    last=${#all_data[@]}
    for (( idx=last-1 ; idx>=last-1 ; idx-- ));do
        #printf "%s<------->%s\n" "${id_list[i]}" "${sample_list[i]}"
        p00="${all_data[$idx]}"
        id="${id_list[$idx]}"
        real_data+=(${p00})
        printf "add real time file ${p00}, continue...\n"
    done
    printf "got ${#real_data[@]} latest collect inc real-time training data file\n"
    train_file_collect_1="$( IFS=$','; echo "${real_data[*]}" )"
fi

echo "++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++"

#test_file=${prefix}"/12/1/"${end}
echo "++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++"
printf "last_id:%s\n" ${last_id}
echo "++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++"
printf "test_file:%s\n" ${test_file}
echo "++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++"
printf "train_file_collect_1:%s\n" ${train_file_collect_1}
echo "++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++"
#rm ${shell_in_run}
#exit 0


jarLib=hdfs://emr-cluster/warehouse/azkaban/lib/fhb_start_v1.jar
queue=root.cpc.bigdata
queue=root.cpc.develop
queue=root.cpc.bigdata
jars=("/home/cpc/anal/lib/spark-tensorflow-connector_2.11-1.10.0.jar" )

randjar="fhb_start"`date +%s%N`".jar"
hadoop fs -get ${jarLib} ${randjar}

delete_old=true

spark-submit --master yarn --queue ${queue} \
    --name "collect-inc-data" \
    --driver-memory 8g --executor-memory 2g \
    --num-executors 200 --executor-cores 2 \
    --conf spark.hadoop.fs.defaultFS=hdfs://emr-cluster2 \
    --conf "spark.yarn.executor.memoryOverhead=4g" \
    --conf "spark.sql.shuffle.partitions=500" \
    --jars $( IFS=$','; echo "${jars[*]}" ) \
    --class com.cpc.spark.ml.dnn.baseData.CollectIncHourlyData\
    ${randjar} ${collect_path} ${des_dir} ${train_file_collect_1} ${last_date} ${curr_date} ${last_id} ${delete_old}


curr_collect_incr=${collect_path}/${curr_date}-${last_id}-incr-instances/part-00000
hadoop fs -test -s ${curr_collect_incr}
if [ $? -eq 0 ] ;then
	echo 'exist and more than zero bytes:'${curr_collect_incr}
    map_base_local=${dir}/base_map_instances.data
    rm ${map_base_local}
    hadoop fs -get ${curr_collect_base} ${map_base_local}

    map_incr_local=${dir}/incr_map_instances.data
    rm ${map_incr_local}
    hadoop fs -get ${curr_collect_incr} ${map_incr_local}

    map_new_local=${dir}/new_map_instances.data
    scp ${map_base_local} ${map_new_local}
    cat ${map_incr_local} >> ${map_new_local}
    hadoop fs -put -f ${map_new_local} ${collect_path}/${curr_date}-${last_id}-new-instances.data
    hadoop fs -put -f ${map_new_local} ${curr_collect_base}
else
	echo 'non exist or less than zero bytes:'${curr_collect_incr}
	echo 'now touch error file:'
	touch ${last_incr_error}
fi

rm ${shell_in_run}

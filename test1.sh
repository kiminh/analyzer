#!/bin/bash
#dumper02上定时运行（依赖数据生成脚本）
branch=novel_ctr_v1
cur_time="2018-11-10 13:15:15"
user=wangyao
version=v1
replica=6
train=`date -d"${cur_time} 2 hours ago" +"%Y-%m-%d-%H"`
now=`date +"%Y-%m-%d-%H"`
hadoop fs -test -e /user/cpc/wy/dnn_novel_${version}/dnntrain-${train}
train_exist=$?

if [[ train_exist == 1 ]]
then
    echo "hourly train file is not ready"
    exit 1
fi

#last_model=`redis-cli -h 192.168.80.23 << EOF
#get last_model_${version}
#EOF`
#last_model=`cat /home/cpc/wy/tmp/last_model_v1.path | tr -d " "`

#if [[ ${#last_model} != 0 ]]
#then
#    model_path=$last_model/checkpoint/1/
#else
#    echo "no previous model for hourly trainig" | mail -s"Fatal: $train dnn ctr model error" wangyao@qutoutiao.net
#    exit 1
#fi

echo "starting training ctr dnn ${version} model $train"
ssh -l ${user} -p 3072 ci.qtt6.cn build qtt_rec_ml-start-job -s -v \
    -p ML_NAME=dnn_novel_${version} \
    -p ML_VER=v1 \
    -p ML_REPLICAS=${replica} \
    -p GIT_NAME="git@git.qutoutiao.net:zhuhaijian/dl-demo.git" \
    -p SCM_REVISION="${branch}" \
    -p TRAIN_FILE="hdfs://emr-cluster/user/cpc/wy/dnn_novel_${version}/dnntrain-${train}/part-r-000**" \
    -p TRAIN_FILE_NUM=100 \
    -p TEST_FILE="hdfs://emr-cluster/user/cpc/wy/dnn_novel_${version}/dnntest-${train}/part-r-000**" \
    -p MODEL_PATH="$model_path" \
    -p TEST_FILE_NUM=100 > /home/cpc/wy/tmp/jenkins_hourly_${version}.log
    #-p MODEL_PATH="abc" \

is_fail=`cat /home/cpc/wy/tmp/jenkins_hourly_${version}.log | grep FAILURE | wc -l`
if [[ $is_fail -gt 0 ]]
then
    echo " " | mail -s"FATAL: submit ctr dnn task to kubernetes failed!!!" wangyao@qutoutiao.net
    exit 1
fi

task_name=`cat /home/cpc/wy/tmp/jenkins_hourly_${version}.log | grep job.batch | awk -F"\"" '{print $2}'`
echo $task_name

num=0
echo "start waiting for k8s task to fininsh "

copyfile(){
    scp /home/cpc/wy/tmp/novel-ctr-dnn-rawid_${version}* cpc@192.168.80.23:/home/cpc/model_server/dnn/
}


generateMlm(){
    python ./bin/generate_DNN_mlm_all.py ${version}
    scp novel-ctr-dnn-rawid_${version}.mlm cpc@192.168.80.23:/home/cpc/model_server/dnn/
    rm novel-ctr-dnn-rawid_${version}.mlm
}

num=1
echo dnn_ctr_${version}-${now}
while true
do
    flag=`redis-cli -h 192.168.80.23 << EOF
get dnn_ctr_${version}-${now}
EOF`
echo $flag
    if [[ $flag == "ok" ]]
    then
        echo "training dnn model ${now} done ！"
        echo "stoping k8s task ${task_name}"
        #ssh -l wangyao -p 3072 172.16.60.25  build qtt_rec_ml-stop-job -s -v -p TASK_NAME=${task_name}
        copyfile
        generateMlm
        echo "模型文件已放到model server 目录下"

        echo "" | mail -s"training dnn ctr ${version} $train done" wangyao@qutoutiao.net
        cat /home/cpc/wy/tmp/jenkins_hourly_${version}.log | grep "HISTORY DIR(C)" | awk -F":" '{print $2}' > /home/cpc/wy/tmp/last_model_v1.path
        last=`cat /home/cpc/wy/tmp/jenkins_hourly_${version}.log | grep "HISTORY DIR(C)" | awk -F":" '{print $2}' | tr -d " "`
        redis-cli -h 192.168.80.23 << EOF
            set last_model_${version} $last
EOF

        exit 0
    else
        if [[ $flag == "low_auc" ]]
        then
            echo "" | mail -s"Warning: training dnn ctr ${version} $train low auc" wangyao@qutoutiao.net
            exit 0
        fi
    fi

    sleep 1m
    num=$(($num+1))
    echo "${num} min has passed"
    if [[ $num -gt 60 ]]
    then
        echo "please kill this task:${task_name} on kubernetes" | mail -s"DNN ctr ${version} training time exceeds 60 min on dumper07" wangyao@qutoutiao.net
        exit 1
    fi
done

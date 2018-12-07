#!/bin/bash
#dumper04上定时运行 dongwei账号（依赖数据生成脚本）
branch=$1
user=$2
version=$3
replica=$4

redis_lock=novel-cvr-dnn-rawid_v2_lock

t=`date +"%Y-%m-%d-%H"`
y1=`date -d"-1 days" +"%Y-%m-%d"`
y2=`date -d"-2 days" +"%Y-%m-%d"`
y3=`date -d"-3 days" +"%Y-%m-%d"`
y4=`date -d"-4 days" +"%Y-%m-%d"`
y5=`date -d"-5 days" +"%Y-%m-%d"`
y6=`date -d"-6 days" +"%Y-%m-%d"`

#redis-cli -h 192.168.80.23 << EOF
#set $redis_lock 1
#EOF

train=(
	hdfs://emr-cluster/user/cpc/wy/novel/cvr_v2/dnntrain-${y1}/part*
    hdfs://emr-cluster/user/cpc/wy/novel/cvr_v2/dnntrain-${y2}/part*
	hdfs://emr-cluster/user/cpc/wy/novel/cvr_v2/dnntrain-${y3}/part*
    hdfs://emr-cluster/user/cpc/wy/novel/cvr_v2/dnntrain-${y4}/part*
	hdfs://emr-cluster/user/cpc/wy/novel/cvr_v2/dnntrain-${y5}/part*
    hdfs://emr-cluster/user/cpc/wy/novel/cvr_v2/dnntrain-${y6}/part*
)

test=hdfs://emr-cluster/user/cpc/wy/novel/cvr_v2/dnntrain-${y1}/part-r-009**

ip=`ifconfig eth0 | grep "inet" | awk '{ print $2}'`
#ip=192.168.80.43
curdir=`pwd`

other_params=(
    CLUSTER.num_worker:1
    DATA.one_hot_slot_num:28
    DATA.multi_hot_slot_num:29
    TRAIN.test_iter:10
    TRAIN.max_steps:400
    dest_ip:$ip
    dest_dir:$curdir
)

last_model="test"

echo "starting training cvr dnn ${version} model $train"

ssh -l ${user} -p 3072 ci.qtt6.cn build qtt_rec_ml-start-job -s -v \
    -p ML_NAME=novel-cvr \
    -p ML_VER=${version} \
    -p ML_REPLICAS=6 \
    -p GIT_NAME="git@git.qutoutiao.net:songchengru/dl-demo.git" \
    -p SCM_REVISION="${branch}" \
    -p TRAIN_FILE="$( IFS=$','; echo "${train[*]}" )" \
    -p TEST_FILE="$test" \
    -p OTHER_PARAMS="$( IFS=$';'; echo "${other_params[*]}" )" > /home/cpc/wy/tmp_cvr/jenkins_daily_${version}.log

cat /home/cpc/wy/tmp_cvr/jenkins_daily_${version}.log

is_fail=`cat /home/cpc/wy/tmp_cvr/jenkins_daily_${version}.log | grep FAILURE | wc -l`
if [[ $is_fail -gt 0 ]]
then
    echo " " | mail -s"FATAL: submit cvr dnn task to kubernetes failed!!!" wangyao@qutoutiao.net
    exit 1
fi

task_name=`cat /home/cpc/wy/tmp_cvr/jenkins_daily_${version}.log | grep job.batch | awk -F"\"" '{print $2}'`
echo $task_name

num=0
echo "start waiting for k8s task to fininsh "


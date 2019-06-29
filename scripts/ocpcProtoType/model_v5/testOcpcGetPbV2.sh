#!/bin/bash

cur=/data/cpc/anal
SPARK_HOME=/usr/lib/spark-current
queue=root.cpc.develop

jars=(
    "$cur/lib/mysql-connector-java-5.1.41-bin.jar"
    "$cur/lib/hadoop-lzo-0.4.20.jar"
    "$cur/lib/config-1.2.1.jar"
)

date=${1}
hour=${2}
version=${3}
media=${4}
highBidFactor=${5}
lowBidFactor=${6}
hourInt=${7}
conversionGoal=${8}
minCV=${9}
expTag=${10}
hourInt1=${11}
hourInt2=${12}
hourInt3=${13}

$SPARK_HOME/bin/spark-submit --master yarn --queue $queue \
    --conf 'spark.port.maxRetries=100' \
    --executor-memory 20g --driver-memory 20g \
    --executor-cores 10 --num-executors 20  \
    --conf 'spark.yarn.executor.memoryOverhead=4g'\
    --conf 'spark.dynamicAllocation.maxExecutors=50'\
    --jars $( IFS=$','; echo "${jars[*]}" ) \
    --class com.cpc.spark.OcpcProtoType.model_v5.OcpcGetPbV2 \
    /home/cpc/wangjun/analyzer/target/scala-2.11/cpc-anal_2.11-0.1.jar ${date} ${hour} ${version} ${media} ${highBidFactor} ${lowBidFactor} ${hourInt} ${conversionGoal} ${minCV} ${expTag} ${hourInt1} ${hourInt2} ${hourInt3}


#val date = args(0).toString
#val hour = args(1).toString
#val version = args(2).toString
#val media = args(3).toString
#val highBidFactor = args(4).toDouble
#val lowBidFactor = args(5).toDouble
#val hourInt = args(6).toInt
#val conversionGoal = args(7).toInt
#val minCV = args(8).toInt
#val expTag = args(9).toString
#val isHidden = 0
#
#// 主校准回溯时间长度
#val hourInt1 = args(10).toInt
#// 备用校准回溯时间长度
#val hourInt2 = args(11).toInt
#// 兜底校准时长
#val hourInt3 = args(12).toInt
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
conversionGoal=${5}
sampleHour=${6}
minCV=${7}
kp=${8}
ki=${9}
kd=${10}
exptag=${11}

$SPARK_HOME/bin/spark-submit --master yarn --queue $queue \
    --conf 'spark.port.maxRetries=100' \
    --executor-memory 20g --driver-memory 20g \
    --executor-cores 10 --num-executors 20  \
    --conf 'spark.yarn.executor.memoryOverhead=4g'\
    --conf 'spark.dynamicAllocation.maxExecutors=50'\
    --jars $( IFS=$','; echo "${jars[*]}" ) \
    --class com.cpc.spark.OcpcProtoType.model_v5.OcpcPIDcontrol \
    /home/cpc/wangjun/analyzer/target/scala-2.11/cpc-anal_2.11-0.1.jar ${date} ${hour} ${version} ${media} ${conversionGoal} ${sampleHour} ${minCV} ${kp} ${ki} ${kd} ${exptag}


#val date = args(0).toString
#val hour = args(1).toString
#val version = args(2).toString
#val media = args(3).toString
#val conversionGoal = args(4).toInt
#val sampleHour = args(5).toInt
#val minCV = args(6).toInt
#val kp = args(7).toDouble
#val ki = args(8).toDouble
#val kd = args(9).toDouble
#val expTag = args(10).toString
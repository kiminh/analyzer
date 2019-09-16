#!/bin/bash

hdfsCur=hdfs://emr-cluster/warehouse/azkaban
localCur=/home/cpc/anal
SPARK_HOME=/usr/lib/spark-current
set -e
queue=root.cpc.bigdata
cur_time=$1

jars=(
    "$localCur/lib/mysql-connector-java-5.1.41-bin.jar"
    "$localCur/lib/mariadb-java-client-1.5.9.jar"
    "$localCur/lib/config-1.2.1.jar"
    "$localCur/lib/hadoop-lzo-0.4.20.jar"
    "$localCur/lib/scala-redis_2.11-1.0.jar"
)


randjar="kddcvr_calibration_"`date +%s%N`".jar"

hadoop fs -get $hdfsCur/lib/kddcvr_calibration.jar $randjar

date=`date -d "${cur_time} 3 hours ago" +%Y-%m-%d`
hour=`date -d "${cur_time} 3 hours ago" +%H`
model=qtt-ext-cvr-video-dnn-rawid-ht66v6
modelfile=rd-videocvr-dnn-rawid_ht66v6
calimodel=${model}-newcali
calibration=calibration-${calimodel}
postcalibration=post-calibration-${calimodel}
timestamp=`date +%s`

${SPARK_HOME}/bin/spark-submit --master yarn --queue $queue \
    --executor-memory 20g --driver-memory 8g \
    --executor-cores 4 --num-executors 40 \
    --conf 'spark.port.maxRetries=128' \
    --conf 'spark.default.parallelism=1000' \
    --conf 'spark.shuffle.consolidateFiles=true' \
    --conf 'spark.yarn.executor.memoryOverhead=5g' \
    --conf 'spark.dynamicAllocation.maxExecutors=100' \
    --conf 'spark.driver.maxResultSize=20g' \
    --conf 'spark.network.timeout=300' \
    --conf 'spark.locality.wait=10' \
	--conf "spark.sql.shuffle.partitions=3000" \
    --jars $( IFS=$','; echo "${jars[*]}" ) \
    --class com.cpc.spark.ml.calibration.MultiDimensionCalibOnKddCvr \
    $randjar $date $hour 24 qtt $model $calimodel 1.0

rm $randjar

hadoop fs -mkdir -p hdfs://emr-cluster/warehouse/dl_cpc.db/cpc_algo_models/${modelfile}/calibration/dnn
hadoop fs -mkdir -p hdfs://emr-cluster/warehouse/dl_cpc.db/cpc_algo_models/${modelfile}/calibration/${timestamp}

#hadoop fs -put -f /home/cpc/scheduled_job/hourly_calibration/${calibration}.mlm hdfs://emr-cluster/warehouse/dl_cpc.db/cpc_algo_models/${modelfile}/calibration/dnn/${calibration}.mlm
#md5sum /home/cpc/scheduled_job/hourly_calibration/${calibration}.mlm > /home/cpc/scheduled_job/hourly_calibration/${calibration}.mlm.md5
#hadoop fs -put -f /home/cpc/scheduled_job/hourly_calibration/${calibration}.mlm.md5 hdfs://emr-cluster/warehouse/dl_cpc.db/cpc_algo_models/${modelfile}/calibration/dnn/${calibration}.mlm.md5
#hadoop fs -put -f /home/cpc/scheduled_job/hourly_calibration/${calibration}.mlm hdfs://emr-cluster/warehouse/dl_cpc.db/cpc_algo_models/${modelfile}/calibration/${timestamp}/${calibration}.mlm

hadoop fs -put -f /home/cpc/scheduled_job/hourly_calibration/${postcalibration}.mlm hdfs://emr-cluster/warehouse/dl_cpc.db/cpc_algo_models/${modelfile}/calibration/dnn/${postcalibration}.mlm
md5sum /home/cpc/scheduled_job/hourly_calibration/${postcalibration}.mlm > /home/cpc/scheduled_job/hourly_calibration/${postcalibration}.mlm.md5
hadoop fs -put -f /home/cpc/scheduled_job/hourly_calibration/${postcalibration}.mlm.md5 hdfs://emr-cluster/warehouse/dl_cpc.db/cpc_algo_models/${modelfile}/calibration/dnn/${postcalibration}.mlm.md5
hadoop fs -put -f /home/cpc/scheduled_job/hourly_calibration/${postcalibration}.mlm hdfs://emr-cluster/warehouse/dl_cpc.db/cpc_algo_models/${modelfile}/calibration/${timestamp}/${postcalibration}.mlm

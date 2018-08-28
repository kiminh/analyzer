#!/bin/bash

spark=/usr/lib/spark-current
cur=/data/cpc/$1
#queue=root.develop.adhoc.cpc
queue=root.report.biz.cpc
#queue=root.production.biz.cpc3
#queue=root.cpc

jars=(
    "$cur/lib/mysql-connector-java-5.1.41-bin.jar"
    "$cur/lib/config-1.2.1.jar"
    "$cur/lib/hadoop-lzo-0.4.20.jar"
    "$cur/lib/mariadb-java-client-1.5.9.jar"
)

min=10
date=`date -d "now -$min min" +%Y-%m-%d`
echo "start $date"

$spark/bin/spark-submit --master yarn --queue $queue \
    --deploy-mode "client" \
    --executor-memory 15g --driver-memory 10g \
    --num-executors 20 --executor-cores 5 \
    --conf 'spark.yarn.executor.memoryOverhead=10g'\
    --jars $( IFS=$','; echo "${jars[*]}" ) \
    --class com.cpc.spark.small.tool.InsertReportQuOnlineData \
    $cur/lib/cpc-small.jar $date

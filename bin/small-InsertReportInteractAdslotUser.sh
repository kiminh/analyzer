#!/bin/bash

spark=/usr/lib/spark-current
cur=/data/cpc/$1
#queue=root.develop.adhoc.cpc
queue=root.report.biz.cpc

jars=(
    "$cur/lib/mysql-connector-java-5.1.41-bin.jar"
    "$cur/lib/config-1.2.1.jar"
    "$cur/lib/hadoop-lzo-0.4.20.jar"
    "$cur/lib/mariadb-java-client-1.5.9.jar"
)

h=24
date=`date -d "now -$h hours" +%Y-%m-%d`
echo "start $date"

$spark/bin/spark-submit --master yarn --queue $queue \
    --deploy-mode "client" \
    --executor-memory 5g --driver-memory 5g \
    --executor-cores 5 --num-executors 5 \
    --conf 'spark.yarn.executor.memoryOverhead=2g'\
    --jars $( IFS=$','; echo "${jars[*]}" ) \
    --class com.cpc.spark.small.tool.InsertReportInteractAdslotUser \
    $cur/lib/cpc-small.jar $date


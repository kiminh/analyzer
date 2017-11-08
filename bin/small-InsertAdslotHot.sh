#!/bin/bash

cur=/home/cpc/$1
SPARK_HOME=/home/spark/spark-2.1.0

queue=root.production.biz.cpc

jars=(
    "$cur/lib/mysql-connector-java-5.1.41-bin.jar"
    "$cur/lib/config-1.2.1.jar"
    "$cur/lib/hadoop-lzo-0.4.20.jar"
    "$cur/lib/mariadb-java-client-1.5.9.jar"
)

h=3
date=`date -d "now -$h hours" +%Y-%m-%d`
hour=`date -d "now -$h hours" +%H`
echo "start $date $hour"


$SPARK_HOME/bin/spark-submit --master yarn \
    --deploy-mode "client" \
    --executor-memory 10G --executor-cores 10 --total-executor-cores 10 \
    --jars $( IFS=$','; echo "${jars[*]}" ) \
    --class com.cpc.spark.small.tool.InsertAdslotHot \
    $cur/lib/cpc-small.jar $date $hour
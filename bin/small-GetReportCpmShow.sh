#!/bin/bash

cur=/data/cpc/$1
#queue=root.develop.adhoc.cpc
queue=root.report.biz.cpc
jars=(
    "$cur/lib/mysql-connector-java-5.1.41-bin.jar"
    "$cur/lib/config-1.2.1.jar"
    "$cur/lib/hadoop-lzo-0.4.20.jar"
    "$cur/lib/mariadb-java-client-1.5.9.jar"
)

/usr/bin/spark2-submit --master yarn --queue $queue \
    --executor-memory 2g --driver-memory 2g \
    --executor-cores 2 --num-executors 5 \
    --conf 'spark.yarn.executor.memoryOverhead=2g'\
    --jars $( IFS=$','; echo "${jars[*]}" ) \
    --class com.cpc.spark.small.tool.GetReportCpmShow \
    $cur/lib/cpc-small.jar 1
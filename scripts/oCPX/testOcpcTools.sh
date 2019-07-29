#!/bin/bash

cur=/data/cpc/anal
SPARK_HOME=/usr/lib/spark-current
queue=root.cpc.develop

jars=(
    "$cur/lib/mysql-connector-java-5.1.41-bin.jar"
    "$cur/lib/hadoop-lzo-0.4.20.jar"
    "$cur/lib/config-1.2.1.jar"
)

$SPARK_HOME/bin/spark-submit --master yarn --queue $queue \
    --conf 'spark.port.maxRetries=100' \
    --executor-memory 8g --driver-memory 4g \
    --executor-cores 4 --num-executors 100  \
    --conf 'spark.yarn.executor.memoryOverhead=4g'\
    --conf 'spark.dynamicAllocation.maxExecutors=100'\
    --conf 'spark.driver.maxResultSize=20g' \
    --conf 'spark.blacklist.enabled=true' \
    --conf "spark.sql.shuffle.partitions=1200" \
    --conf "spark.default.parallelism=1200" \
    --conf "spark.shuffle.file.buffer=64k" \
    --conf "spark.reducer.maxSizeInFlight=96M" \
    --conf "spark.shuffle.memoryFraction=0.4" \
    --conf "spark.shuffle.consolidateFiles=true" \
    --conf "spark.shuffle.io.maxRetries=60" \
    --conf "spark.shuffle.io.retryWait=60" \
    --conf "spark.port.maxRetries=255" \
    --jars $( IFS=$','; echo "${jars[*]}" ) \
    --class com.cpc.spark.oCPX.OcpcTools \
    /home/cpc/wangjun/analyzer/target/scala-2.11/cpc-anal_2.11-0.1.jar $1 $2

#val date = args(0).toString
#val hour = args(1).toString


#--conf 'spark.yarn.executor.memoryOverhead=4g' \
#--conf 'spark.dynamicAllocation.maxExecutors=200' \
#--conf 'spark.driver.maxResultSize=20g' \
#--conf 'spark.blacklist.enabled=true' \
#--conf "spark.sql.shuffle.partitions=1600" \
#--conf "spark.default.parallelism=1600" \
#--conf "spark.shuffle.file.buffer=64k" \
#--conf "spark.reducer.maxSizeInFlight=96M" \
#--conf "spark.shuffle.memoryFraction=0.4" \
#--conf "spark.shuffle.consolidateFiles=true" \
#--conf "spark.shuffle.io.maxRetries=60" \
#--conf "spark.shuffle.io.retryWait=60" \
#--conf "spark.port.maxRetries=255" \
#!/usr/bin/env bash
# dump kafka topic: fm_impression

cur=/data/cpc/hzh
SPARK_HOME=/usr/lib/spark-current
queue=root.report.biz.cpc

jars=(
    "$cur/lib/spark-streaming-kafka-0-8-assembly_2.11-2.1.0.jar"
    "$cur/lib/jedis-2.1.0.jar"
    "$cur/lib/config-1.2.1.jar"
)

$SPARK_HOME/bin/spark-submit --master yarn --queue $queue \
    --deploy-mode client \
    --executor-memory 10g \
    --executor-cores 1 --num-executors 20 \
    --conf 'spark.port.maxRetries=128' \
    --conf 'spark.yarn.executor.memoryOverhead=4g' \
    --conf 'spark.dynamicAllocation.maxExecutors=20' \
    --jars $( IFS=$','; echo "${jars[*]}" ) \
    --class com.cpc.spark.streaming.dump.DumpFmImpression \
    $cur/custom/dump-fm-imp.jar
#!/bin/bash
#git checkout master
git pull
rm target/scala-2.11/cpc-anal_2.11-0.1.jar
sbt assembly
echo "hadoop fs -put -f target/scala-2.11/cpc-anal_2.11-0.1.jar hdfs://emr-cluster/warehouse/azkaban/lib/fhb_start_v1.jar"
hadoop fs -put -f target/scala-2.11/cpc-anal_2.11-0.1.jar hdfs://emr-cluster/warehouse/azkaban/lib/fhb_start_v1.jar
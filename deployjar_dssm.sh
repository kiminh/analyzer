#!/bin/bash
#git checkout master
git pull
sbt assembly
echo "hadoop fs -put -f target/scala-2.11/cpc-anal_2.11-0.1.jar hdfs://emr-cluster/warehouse/azkaban/lib/dssm.jar"
hadoop fs -put -f target/scala-2.11/cpc-anal_2.11-0.1.jar hdfs://emr-cluster/warehouse/azkaban/lib/dssm.jar
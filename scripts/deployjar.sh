#!/bin/bash
git checkout master
git pull
sbt assembly
echo "hadoop fs -put -f target/scala-2.11/analyzer_2.11-0.1.jar hdfs://emr-cluster/warehouse/azkaban/lib/analyzer-deploy.jar"
hadoop fs -put -f target/scala-2.11/analyzer_2.11-0.1.jar hdfs://emr-cluster/warehouse/azkaban/lib/analyzer-deploy.jar
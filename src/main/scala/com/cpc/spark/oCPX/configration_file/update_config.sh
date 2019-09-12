#!/bin/bash

# sh update_config.sh ocpc_bid_factor.json
configFile=$1

hadoopFile=hdfs://emr-cluster/user/cpc/ocpc/conf/$configFile

echo "###########################"
hadoop fs -cat $hadoopFile
echo "###########################"
hadoop fs -put -f $configFile $hadoopFile
echo "###########################"
hadoop fs -ls $hadoopFile

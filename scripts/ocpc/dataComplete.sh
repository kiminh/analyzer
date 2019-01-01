#!/bin/bash

date=$1
hour=$2


unionlog=/user/cpc/okdir/union_done/${date}-${hour}.ok
mlfeature=/user/cpc/okdir/ml_cvr_feature_v1_done/${date}-${hour}.ok
cvr3log=/user/cpc/okdir/ml_cvr_feature_v2_done/${date}-${hour}.ok

hadoop fs -test -e $unionlog
if [[ $? == 0 ]]; then
    hadoop fs -test -e $mlfeature
    if [[ $? == 0 ]]; then
        hadoop fs -test -e $cvr3log
        if [[ $? == 0 ]]; then
            echo "start the project!"
            sh testGetOcpcLogFromUnionLog.sh $date $hour
            sh testOcpcActivationDataV1.sh $date $hour
            sh testOcpcAccPact.sh $date $hour
            sh testOcpcAccPcvr.sh $date $hour
            sh testOcpcSampleHourlyV1.sh $date $hour
            sh testOcpcMonitor.sh $date $hour

            hadoop fs -touchz /user/cpc/wangjun/okdir/ocpc/ocpc_unionlog/${date}-${hour}.ok
            exit 0
        else
            exit 0
        fi
    else
        exit 0
    fi
else
    exit 0
fi
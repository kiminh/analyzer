#!/bin/bash

date=$1
hour=$2


unionlog=/user/cpc/okdir/union_done/${date}-${hour}.ok
mlfeature=/user/cpc/okdir/ml_cvr_feature_v1_done/${date}-${hour}.ok
cvr3log=/user/cpc/okdir/ml_cvr_feature_v2_done/${date}-${hour}.ok
advSite=/user/cpc/wangjun/okdir/ocpc/ocpcSiteAdvForm/${date}-${hour}.ok

okfile=/user/cpc/wangjun/okdir/ocpc/data/ocpcProtoData/${date}-${hour}.ok


if [[ $? == 0 ]]; then
    hadoop fs -test -e $mlfeature
    if [[ $? == 0 ]]; then
        hadoop fs -test -e $cvr3log
        if [[ $? == 0 ]]; then
            hadoop fs -test -e $advSite
            if [[ $? == 0 ]]; then
                echo "start the project!"
                sh testOcpcUnionlog.sh $date $hour
                sh testOcpcConversion.sh $date $hour 1
                sh testOcpcConversion.sh $date $hour 2
                sh testOcpcConversion.sh $date $hour 3

                hadoop fs -touchz ${okfile}
                exit 0
            else
                echo "$advSite not found"
                exit 0
            fi
        else
            echo "$cvr3log not found"
            exit 0
        fi
    else
        echo "$mlfeature not found"
        exit 0
    fi
else
    echo "$unionlog not found"
    exit 0
fi
#!/bin/bash

# sh dataLoop.sh testOcpcSampleHourlyV1.sh 2018-10-27 0 23

script=$1
date1=$2
date2=$3
hour=$4
d=$date1
while [ ${d} -le $date2 ]; do
    if ((${d}<10))
    then
        currentdate="2019-02-0${d}"
    else
        currentdate="2019-02-${d}"
    fi

    echo $script
    echo $currentdate
    echo $hour

    sh $script $currentdate $hour qtt qtt_demo

    let d=d+1
done

#!/bin/bash

# sh dataLoop.sh testOcpcSampleHourlyV1.sh 2018-10-27 0 23

d=$1
while [ ${d} -le $2 ]; do
    if ((${d}<10))
    then
        day="2019-04-0${d}"
    else
        day="2019-04-${d}"
    fi

    echo $day

#    sh testOcpcChargeLoop.sh $day 23

    let d=d+1
done

#!/bin/bash

# sh dataLoop.sh testOcpcSampleHourlyV1.sh 2018-10-27 0 23

d=$2
while [ ${d} -le $3 ]; do
    if ((${d}<10))
    then
        day="2019-05-0${d}"
    else
        day="2019-05-${d}"
    fi

    echo $1
    echo $day

    sh $1 $day 23

    let d=d+1
done

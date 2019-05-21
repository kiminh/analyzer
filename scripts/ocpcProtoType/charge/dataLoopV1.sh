#!/bin/bash

# sh dataLoop.sh testOcpcChargeApp.sh 05 1 19

d=$3
while [ ${d} -le $4 ]; do
    if ((${d}<10))
    then
        day="2019-${2}-0${d}"
    else
        day="2019-${2}-${d}"
    fi

    echo $1
    echo $day

    sh $1 $day 23

    let d=d+1
done

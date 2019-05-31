#!/bin/bash

# sh dataLoop.sh 05 1 19

d=$2
while [ ${d} -le $3 ]; do
    if ((${d}<10))
    then
        day="2019-${1}-0${d}"
    else
        day="2019-${1}-${d}"
    fi

    echo $1
    echo $day

    sh testOcpcChargeV2.sh $day 6

    let d=d+1
done

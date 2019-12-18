#!/bin/bash

# sh dataLoop.sh testOcpcSampleHourlyV1.sh 2018-10-27 0 23

h=$3
while [ ${h} -le $4 ]; do
    if ((${h}<10))
    then
        hour="0${h}"
    else
        hour="${h}"
    fi

    sh $1 $2 $hour

    let h=h+1
done

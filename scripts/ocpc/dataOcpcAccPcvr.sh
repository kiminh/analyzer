#!/bin/bash

h=$2
while [ ${h} -le $3 ]; do
    if ((${h}<10))
    then
        hour="0${h}"
    else
        hour="${h}"
    fi

    sh test_ocpc_sample_hourly.sh $1 $hour

    let h=h+1
done

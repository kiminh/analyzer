#!/bin/bash

h=0
while [ ${h} -le 23 ]; do
    if ((${h}<10))
    then
        hour="0${h}"
    else
        hour="${h}"
    fi

    sh test_ocpc_sample_hourly.sh $1 $hour
    
    let h=h+1
done

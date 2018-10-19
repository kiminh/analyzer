#!/bin/bash

h=$2
while [ ${h} -le $3 ]; do
    if ((${h}<10))
    then
        hour="0${h}"
    else
        hour="${h}"
    fi

    sh testOcpcMonitor.sh $1 $hour

    let h=h+1
done

#!/bin/bash

# sh dataLoopCvrNovel.sh 2018-10-27 0 23

date=$1
h=$2
while [ ${h} -le $3 ]; do
    if ((${h}<10))
    then
        hour="0${h}"
    else
        hour="${h}"
    fi

    echo $date
    echo $hour
    sh testOcpcConversionNovel.sh $date $hour 2

    let h=h+1
done

#!/bin/bash

date=$1
hour=$2


sh testOcpcForAll.sh data.OcpcLabelCvr1 $date $hour
sh testOcpcForAll.sh data.OcpcLabelCvr2 $date $hour
sh testOcpcForAll.sh data.OcpcLabelCvr3 $date $hour
sh testOcpcForAll.sh data.OcpcProcessUnionlog $date $hour
sh testOcpcForAll.sh data.OcpcUnionlogNovel $date $hour

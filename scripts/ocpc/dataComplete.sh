#!/bin/bash

date=$1
hour=$2


sh testGetOcpcLogFromUnionLog.sh $date $hour
sh testOcpcActivationDataV1.sh $date $hour
sh testOcpcAccPact.sh $date $hour
sh testOcpcAccPcvr.sh $date $hour
sh testOcpcSampleHourlyV1.sh $date $hour
sh testOcpcMonitor.sh $date $hour

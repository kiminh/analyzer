#!/bin/bash
#git checkout master
git pull
sbt assembly
echo "cp target/scala-2.11/cpc-anal_2.11-0.1.jar /home/cpc/anal/lib/lr-train-test.jar"
cp target/scala-2.11/cpc-anal_2.11-0.1.jar /home/cpc/anal/lib/lr-train-test.jar
#!/bin/bash

cur=/home/cpc/$1
SPARK_HOME=/home/spark/spark-2.1.0

java -cp "$cur/lib/*" com.cpc.spark.ml.stub.Stub
#!/usr/bin/env python
# -*- coding:utf8-

import sys
import os
import time

os.system("ls -l")
filename = sys.argv[1]
current_time = time.strftime('%Y-%m-%d-%H-%M-%S',time.localtime(time.time()))
print current_time
print filename

print "#################################"
backup_command = "mv " + filename + " " + filename + ".bak" + current_time
print "command: " + backup_command
os.system(backup_command)

print "#################################"
hadoop_backup = "hadoop fs -get hdfs://emr-cluster/warehouse/azkaban/lib/" + filename
print "command: " + hadoop_backup
os.system(hadoop_backup)

print "#################################"
origin_jar = "/home/cpc/wangjun/analyzer/target/scala-2.11/cpc-anal_2.11-0.1.jar"
new_jar = "/home/cpc/wangjun/analyzer/target/scala-2.11/" + filename
print "command: " + "cp " + origin_jar + " " + new_jar
os.system("cp " + origin_jar + " " + new_jar)

print "#################################"
hadoop_replace = "hadoop fs -put -f " + new_jar + " hdfs://emr-cluster/warehouse/azkaban/lib/" + filename
print "command: " + hadoop_replace
os.system(hadoop_replace)
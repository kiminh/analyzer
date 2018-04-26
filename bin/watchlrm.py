#-*- coding:utf-8 -*-
import lrmodel_pb2
import sys
for line in sys.stdin:
    try:
        line = line.strip()
        pack = lrmodel_pb2.Pack()
        pack.ParseFromString(line)
        print pack.m
        #if event.location.province == 1:
        #    print event
        #    print "=" * 50
        #    print "\n"
    except Exception,e:
        print e
        continue
    sys.stdout.flush()
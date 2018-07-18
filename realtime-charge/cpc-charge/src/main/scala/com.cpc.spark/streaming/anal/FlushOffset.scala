package com.cpc.spark.streaming.anal

import com.cpc.spark.streaming.tools.OffsetRedis

object FlushOffset {
    def main(args: Array[String]) {
       println("flush offset start")
       OffsetRedis.getOffsetRedis.refreshOffsetKey()
       println("flush offset end")
    }
}
package com.cpc.spark
import org.apache.spark.Partitioner



class test_partitioner(val num: Int) extends Partitioner {
  override def numPartitions: Int = num

  override def getPartition(key: Any): Int = {
    val s = key.asInstanceOf[(String, Int)]
    s._2 % num
  }
}

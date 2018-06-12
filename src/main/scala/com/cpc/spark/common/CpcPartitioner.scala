package com.cpc.spark.common

import org.apache.spark.Partitioner

import scala.util.hashing.MurmurHash3

class CpcPartitioner(num: Int) extends Partitioner {
  override def numPartitions: Int = num

  override def getPartition(key: Any): Int = {
    val k = key.asInstanceOf[(String, Int)]
    (MurmurHash3.stringHash(k._1) + k._2) % numPartitions
  }
}

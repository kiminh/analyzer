package com.cpc.spark.common

import org.apache.spark.Partitioner

import scala.util.hashing.MurmurHash3

class CpcPartitioner(num: Int) extends Partitioner {
  override def numPartitions: Long = num.toLong

  override def getPartition(key: Any): Int = {
    val k = key.asInstanceOf[(String, Int)]
    (Murmur3Hash.stringHash(k._1) % numPartitions + numPartitions + k._2) % numPartitions
  }
}

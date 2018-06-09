package com.cpc.spark

import org.apache.spark.Partitioner

import scala.util.hashing.MurmurHash3


class test_partitioner(val num: Int) extends Partitioner {
  override def numPartitions: Int = num

  override def getPartition(key: Any): Int = {
    val s = key.asInstanceOf[(String, Int)]
    (MurmurHash3.stringHash(s._1) + s._2) % num
  }
}

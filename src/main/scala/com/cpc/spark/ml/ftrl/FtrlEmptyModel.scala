package com.cpc.spark.ml.ftrl

/**
  * author: huazhenhao
  * date: 9/18/18
  */

import com.cpc.spark.common.{Murmur3Hash, Utils}
import com.cpc.spark.ml.common.{Utils => MUtils}
import com.cpc.spark.ml.train.Ftrl
import com.typesafe.config.ConfigFactory
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object FtrlEmptyModel {

  val LOCAL_DIR = "/home/cpc/ftrl/"
  val DEST_DIR = "/home/work/mlcpp/model/"

  val DOWN_SAMPLE_RATE = 0.2


  def main(args: Array[String]): Unit = {
    val ftrl = new Ftrl(1)
    Ftrl.saveLrPbPackWithDict(ftrl, "/home/cpc/ftrl/ctr-protrait21-ftrl-id-qtt-list-redis.mlm", "ctr-ftrl-v1", "ctr-protrait21-ftrl-id-qtt-list-redis")
  }
}

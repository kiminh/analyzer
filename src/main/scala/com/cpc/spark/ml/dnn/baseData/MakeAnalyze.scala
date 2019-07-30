package com.cpc.spark.ml.dnn.baseData

import java.io.{File, PrintWriter}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import scala.sys.process._
import java.text.SimpleDateFormat
import org.apache.commons.lang3.time.DateUtils
import scala.collection.mutable.ArrayBuffer

/**
  * Created by fenghuabin on 2019/7/30.
  */

object MakeAnalyze {

  def main(args: Array[String]): Unit = {
    Logger.getRootLogger.setLevel(Level.WARN)
    val sparkConf = new SparkConf()
    sparkConf.set("spark.driver.maxResultSize", "5g")
    val spark = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
    val sc = spark.sparkContext

    var prefix = "hdfs://emr-cluster/user/cpc/aiclk_dataflow/realtime-00/adlist-v4-up/2019-07-27/"
    val tail = "/part-r-*"

    val data_list = ArrayBuffer[String]()
    for (idx <- 0 until 24) {
      data_list += prefix + "%2d".format(idx) + tail
    }

    prefix = "hdfs://emr-cluster/user/cpc/aiclk_dataflow/realtime-30/adlist-v4-up/2019-07-27/"
    for (idx <- 0 until 24) {
      data_list += prefix + "%2d".format(idx) + tail
    }

    for (part <- data_list) {
      println(part)
    }

    val importedDf: DataFrame = spark.read.format("tfrecords").option("recordType", "Example").load(data_list.mkString(","))
    println("DF file count:" + importedDf.count().toString + " of file:" + data_list.mkString(","))
    importedDf.printSchema()
    importedDf.show(3)

  }


}

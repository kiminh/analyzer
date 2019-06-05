package com.cpc.spark.ml.recall.featureSystems

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

import scala.collection.mutable

object recall_rec_sample_tfrecord {
  Logger.getRootLogger.setLevel(Level.WARN)
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("recall_rec_features").enableHiveSupport().getOrCreate()
    val curday = args(0)
    val hour = args(1)
    val cal1 = Calendar.getInstance()
    cal1.setTime(new SimpleDateFormat("yyyy-MM-dd H").parse(s"$curday" + s" " + s"$hour"))
    cal1.add(Calendar.HOUR, 1)
    val dayOneHourAgo = new SimpleDateFormat("yyyy-MM-dd").format(cal1.getTime)
    val oneHourAgo = new SimpleDateFormat("H").format(cal1.getTime)

    import spark.implicits._

    val sample = spark.sql(
      s"""
         |select * from dl_cpc.recall_rec_feature where day='$curday' and hour='$hour'
       """.stripMargin)
  }

}

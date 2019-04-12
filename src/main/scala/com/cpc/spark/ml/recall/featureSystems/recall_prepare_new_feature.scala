package com.cpc.spark.ml.recall.featureSystems

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object recall_prepare_new_feature {
  Logger.getRootLogger.setLevel(Level.WARN)
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("recall_prepare_new_feature")
      .enableHiveSupport()
      .getOrCreate()
    val featureName = args(0)
    val cal1 = Calendar.getInstance()
    cal1.add(Calendar.DATE, -1)
    val oneday = new SimpleDateFormat("yyyy-MM-dd").format(cal1.getTime)
    getFeature(spark, oneday, featureName)

    cal1.add(Calendar.DATE, -1)
    val twodays = new SimpleDateFormat("yyyy-MM-dd").format(cal1.getTime)
    getFeature(spark, twodays, featureName)

    cal1.add(Calendar.DATE, -1)
    val threedays = new SimpleDateFormat("yyyy-MM-dd").format(cal1.getTime)
    getFeature(spark, threedays, featureName)

    cal1.add(Calendar.DATE, -1)
    val fourdays = new SimpleDateFormat("yyyy-MM-dd").format(cal1.getTime)
    getFeature(spark, fourdays, featureName)


  }
  def getFeature(spark: SparkSession, date: String, featureName: String): Unit = {
    if(featureName == "activeApp"){
      val uidApp = spark.read.parquet(s"hdfs://emr-cluster/user/cpc/userInstalledApp/$date")
      uidApp.select("uid", "used_pkgs").distinct().repartition(200).createOrReplaceTempView("usedApp")
      spark.sql(
        s"""
           |insert overwrite table dl_cpc.recall_test_feature partition(dt='$date')
           |select uid, null, used_pkgs, "$featureName" from usedApp
       """.stripMargin)
    } else if (featureName == "deleteApp"){
      print("prepare delete APP")
    }
  }
}

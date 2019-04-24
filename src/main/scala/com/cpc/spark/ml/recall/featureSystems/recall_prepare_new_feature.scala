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
      uidApp.select("uid","used_pkgs").distinct().repartition(200).createOrReplaceTempView("temptable")
      spark.sql(
        s"""
           |insert overwrite table dl_cpc.recall_test_feature partition(dt='$date')
           |select uid, null, used_pkgs, "$featureName" from temptable where used_pkgs[0] is not null
       """.stripMargin)
    } else if (featureName == "deletedApp"){
      print("prepare delete APP")
      val uidApp = spark.read.parquet(s"hdfs://emr-cluster/user/cpc/userInstalledApp/$date")
      uidApp.select("uid","remove_pkgs").distinct().repartition(200).createOrReplaceTempView("temptable")
      spark.sql(
        s"""
           |insert overwrite table dl_cpc.recall_test_feature partition(dt='$date')
           |select uid, null, remove_pkgs, "$featureName" from temptable where remove_pkgs[0] is not null
       """.stripMargin)
    } else if (featureName == "addedApp"){
      print("prepare add APP")
      val uidApp = spark.read.parquet(s"hdfs://emr-cluster/user/cpc/userInstalledApp/$date")
      uidApp.select("uid","add_pkgs").distinct().repartition(200).createOrReplaceTempView("temptable")
      spark.sql(
        s"""
           |insert overwrite table dl_cpc.recall_test_feature partition(dt='$date')
           |select uid, null, add_pkgs, "$featureName" from temptable where add_pkgs[0] is not null
       """.stripMargin)
    } else if (featureName == "frequentPoi_1"){
      spark.sql(
        s"""
           |insert overwrite table dl_cpc.recall_test_feature partition(dt='$date')
           |select uid,poi_type_feature1,null,"$featureName" from dl_cpc.user_frequent_poi where dt='$date'
         """.stripMargin
      ).repartition(200)
    }
  }
}

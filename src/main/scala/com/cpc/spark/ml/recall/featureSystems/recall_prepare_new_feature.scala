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

    cal1.add(Calendar.DATE, -1)
    val fivedays = new SimpleDateFormat("yyyy-MM-dd").format(cal1.getTime)
    getFeature(spark, fivedays, featureName)


  }
  def getFeature(spark: SparkSession, date: String, featureName: String): Unit = {
    if(featureName == "app"){
      val cal = Calendar.getInstance()
      cal.setTime(new SimpleDateFormat("yyyy-MM-dd").parse(s"$date"))
      cal.add(Calendar.DATE, -1)
      val date1 = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime)
      cal.add(Calendar.DATE, -1)
      val date2 = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime)
      val uidApp = spark.read.parquet(s"hdfs://emr-cluster/user/cpc/userInstalledApp/{$date, $date1, $date2}")
      uidApp.select("uid","used_pkgs", "remove_pkgs", "add_pkgs").distinct().createOrReplaceTempView("temptable")
      spark.sql(
        s"""
           |select uid, null, null, null, if(used_pkgs[0] is null,null, used_pkgs),
           |if(remove_pkgs[0] is null, null, remove_pkgs),
           |if(add_pkgs[0] is null, null, add_pkgs)
           | from (select *,row_number() over(partition by uid order by rand() desc) as row_num from temptable where uid is not null) ta where row_num=1
       """.stripMargin).repartition(200).createOrReplaceTempView("temp_result")
      spark.sql(
        s"""
           |insert overwrite table dl_cpc.recall_test_feature partition(dt='$date', feature_name='$featureName')
           |select * from temp_result
       """.stripMargin)

    } else if (featureName == "frequent_poi_1"){
      spark.sql(
        s"""
           |insert overwrite table dl_cpc.recall_test_feature partition(dt='$date', feature_name='$featureName')
           |select uid,poi_type_feature1, poi_type_feature2, poi_type_feature3, null, null, null from dl_cpc.user_frequent_poi where dt='$date'
         """.stripMargin
      ).repartition(200)
    }
    else if (featureName == "activeapp"){
      val cal = Calendar.getInstance()
      cal.setTime(new SimpleDateFormat("yyyy-MM-dd").parse(s"$date"))
      cal.add(Calendar.DATE, -1)
      val date1 = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime)
      cal.add(Calendar.DATE, -1)
      val date2 = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime)
      val uidApp = spark.read.parquet(s"hdfs://emr-cluster/user/cpc/userInstalledApp/{$date, $date1, $date2}")
      uidApp.select("uid","used_pkgs").distinct().createOrReplaceTempView("temptable")
      spark.sql(
        s"""
           |select uid, null, null, null, if(used_pkgs[0] is null,null, used_pkgs),
           |null,
           |null
           | from (select *,row_number() over(partition by uid order by rand() desc) as row_num from temptable where uid is not null) ta where row_num=1
       """.stripMargin).repartition(200).createOrReplaceTempView("temp_result")
      spark.sql(
        s"""
           |insert overwrite table dl_cpc.recall_test_feature partition(dt='$date', feature_name='$featureName')
           |select * from temp_result
       """.stripMargin)
    }
  }
}

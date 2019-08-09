package com.cpc.spark.conversionMonitor.cvrWarning

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object v3Cvr {
  def main(args: Array[String]): Unit = {
    Logger.getRootLogger.setLevel(Level.WARN)
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()

    // 计算日期周期
    val date = args(0).toString

    val clickData = getClickData(date, spark)
    val cv1Data = getLabel1(date, spark)
    val cv2Data = getLabel2(date, spark)
    val cv3Data = getLabel3(date, spark)
    val cv4Data = getLabel4(date, spark)

    val cvData = cv1Data
      .union(cv2Data)
      .union(cv3Data)
      .union(cv4Data)

    val dataToday = clickData
      .join(cvData, Seq("searchid"), "left_outer")
      .na.fill(0, Seq("iscvr"))
      .groupBy("conversion_goal", "hour")
      .agg(
        sum(col("isclick")).alias("click"),
        sum(col("iscvr")).alias("cv")
      )
      .withColumn("cvr", col("cv") * 1.0 / col("click"))
      .select("conversion_goal", "hour", "click", "cv", "cvr")
      .withColumn("date", lit(date))
      .withColumn("version", lit("v3"))
      .select("conversion_goal", "click", "cv", "cvr", "date", "hour", "version")

    dataToday
      .repartition(1)
      .write.mode("ovewrite").insertInto("test.cv_goal_cvr_hourly")
  }

  def getLabel4(date: String, spark: SparkSession) = {
    val sqlRequest =
      s"""
         |
         |select
         |    distinct searchid
         |from dl_cpc.ml_cvr_feature_v1
         |lateral view explode(cvr_list) b as a
         |where `date` = '$date'
         |and access_channel="sdk"
         |and a = "sdk_site_wz"
       """.stripMargin
    println(sqlRequest)
    val data = spark
      .sql(sqlRequest)
      .withColumn("iscvr", lit(1))
      .withColumn("conversion_goal", lit(4))
      .distinct()

    data
  }

  def getLabel3(date: String, spark: SparkSession) = {
    val sqlRequest =
      s"""
         |
         |select
         |    distinct searchid
         |from dl_cpc.ml_cvr_feature_v1
         |lateral view explode(cvr_list) b as a
         |where `date` = '$date'
         |and access_channel="site"
         |and a in ('ctsite_form', 'site_form')
       """.stripMargin
    println(sqlRequest)
    val data = spark
      .sql(sqlRequest)
      .withColumn("iscvr", lit(1))
      .withColumn("conversion_goal", lit(3))
      .distinct()

    data
  }

  def getLabel2(date: String, spark: SparkSession) = {
    val sqlRequest =
      s"""
         |select
         |    distinct searchid
         |from dl_cpc.ml_cvr_feature_v2
         |where `date` = '$date'
         |and label = 1
       """.stripMargin
    println(sqlRequest)
    val data = spark
      .sql(sqlRequest)
      .withColumn("iscvr", lit(1))
      .withColumn("conversion_goal", lit(2))
      .distinct()

    data
  }

  def getLabel1(date: String, spark: SparkSession) = {
    val sqlRequest =
      s"""
         |select
         |    distinct searchid
         |from dl_cpc.ml_cvr_feature_v1
         |lateral view explode(cvr_list) b as a
         |where `date` = '$date'
         |and access_channel="sdk"
         |and a = "sdk_app_install"
       """.stripMargin
    println(sqlRequest)
    val data = spark
      .sql(sqlRequest)
      .withColumn("iscvr", lit(1))
      .withColumn("conversion_goal", lit(1))
      .distinct()

    data
  }


  def getClickData(date: String, spark: SparkSession) = {
    val sqlRequest =
      s"""
         |select
         |  searchid,
         |  unitid,
         |  userid,
         |  adclass,
         |  conversion_goal as cv_goal,
         |  is_ocpc,
         |  isclick,
         |  hour
         |from dl_cpc.cpc_basedata_union_events
         |where day = '$date'
         |and isclick = 1
         |and adslot_type != 7
         |and length(searchid) > 0
       """.stripMargin
    println(sqlRequest)
    val data = spark
      .sql(sqlRequest)
      .na.fill(0, Seq("cv_goal"))
      .withColumn("conversion_goal", udfDetermineCvGoal()(col("adclass"), col("is_ocpc"), col("cv_goal")))
      .filter(s"conversion_goal > 0")
      .select("searchid", "isclick", "conversion_goal", "hour")

    data
  }

  def udfDetermineCvGoal() = udf((adclass: Int, is_ocpc: Int, cv_goal: Int) => {
    var result = 0
    if (is_ocpc == 1) {
      result = cv_goal
    } else{
      if (adclass == 110110100) {
        result = 4
      } else {
        result = 0
      }
    }
    result
  })

}
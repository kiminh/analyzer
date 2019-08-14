package com.cpc.spark.oCPX.monitor

import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.oCPX.OcpcTools.{getTimeRangeSqlDate, udfConcatStringInt, udfDetermineMedia}
import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object OcpcUnitSurvivalMonitor {
  def main(args: Array[String]): Unit = {
    Logger.getRootLogger.setLevel(Level.WARN)
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()

    // 计算日期周期
    val date = args(0).toString
    val hour = args(1).toString
    val hourInt = args(2).toInt

    // 抽取基础表
    val baseData = getData(date, hour, hourInt, spark)

    // 分小时计算cvr
    val data = calculateCvr(baseData, date, hour, spark).cache()

    data.show(10)
    data
      .select("unitid", "userid", "conversion_goal", "media", "click", "click_cv", "show_cv", "date", "hour", "hour_int")
      .repartition(5)
//      .write.mode("overwrite").insertInto("test.ocpc_cvr_delay_hourly")
      .write.mode("overwrite").insertInto("dl_cpc.ocpc_cvr_delay_hourly")

  }

  def getData(date: String, hour: String, hourInt: Int, spark: SparkSession) = {
    // 取历史数据
    val dateConverter = new SimpleDateFormat("yyyy-MM-dd HH")
    val newDate = date + " " + hour
    val today = dateConverter.parse(newDate)
    val calendar = Calendar.getInstance
    calendar.setTime(today)
    calendar.add(Calendar.HOUR, -hourInt)
    val yesterday = calendar.getTime
    val tmpDate = dateConverter.format(yesterday)
    val tmpDateValue = tmpDate.split(" ")
    val date1 = tmpDateValue(0)
    val hour1 = tmpDateValue(1)
    val selectCondition = getTimeRangeSqlDate(date1, hour1, date, hour)

    val conf = ConfigFactory.load("ocpc")
    val conf_key = "medias.total.media_selection"
    val mediaSelection = conf.getString(conf_key)
    // 取数据: score数据
    val sqlRequest =
      s"""
         |select
         |    searchid,
         |    unitid,
         |    userid,
         |    conversion_goal,
         |    media_appsid,
         |    isclick,
         |    date,
         |    hour
         |from dl_cpc.ocpc_base_unionlog
         |where $selectCondition
         |and isshow = 1
         |and $mediaSelection
         |and ideaid > 0 and adsrc = 1
         |and userid > 0
         |and conversion_goal > 0
       """.stripMargin
    println(sqlRequest)
    val clickData = spark
      .sql(sqlRequest)
      .withColumn("cvr_goal", udfConcatStringInt("cvr")(col("conversion_goal")))
      .withColumn("media", udfDetermineMedia()(col("media_appsid")))


    val sqlRequest2 =
      s"""
         |SELECT
         |  searchid,
         |  label,
         |  cvr_goal
         |FROM
         |  dl_cpc.ocpc_label_cvr_hourly
         |WHERE
         |  $selectCondition
       """.stripMargin
    println(sqlRequest2)
    val cvData = spark.sql(sqlRequest2)


    // 关联数据
    val resultDF = clickData
      .join(cvData, Seq("searchid", "cvr_goal"), "left_outer")
      .na.fill(0, Seq("label"))

    resultDF
  }

  def getConversionGoal(date: String, hour: String, spark: SparkSession) = {
    val conf = ConfigFactory.load("ocpc")

    val url = conf.getString("adv_read_mysql.new_deploy.url")
    val user = conf.getString("adv_read_mysql.new_deploy.user")
    val passwd = conf.getString("adv_read_mysql.new_deploy.password")
    val driver = conf.getString("adv_read_mysql.new_deploy.driver")
    val table = "(select id, user_id, ocpc_bid, cast(conversion_goal as char) as conversion_goal, is_ocpc, ocpc_status from adv.unit where ideas is not null) as tmp"

    val data = spark.read.format("jdbc")
      .option("url", url)
      .option("driver", driver)
      .option("user", user)
      .option("password", passwd)
      .option("dbtable", table)
      .load()

    val resultDF = data
      .withColumn("unitid", col("id"))
      .withColumn("userid", col("user_id"))
      .withColumn("cpagiven", col("ocpc_bid"))
      .selectExpr("unitid",  "userid", "cpagiven", "cast(conversion_goal as int) conversion_goal", "is_ocpc", "ocpc_status")
      .distinct()

    resultDF.show(10)
    resultDF
  }



}




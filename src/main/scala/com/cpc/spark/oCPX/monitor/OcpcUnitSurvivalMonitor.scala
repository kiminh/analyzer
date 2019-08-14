package com.cpc.spark.oCPX.monitor

import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.oCPX.OcpcTools.{getTimeRangeSqlDate, udfConcatStringInt, udfDetermineMedia}
import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object OcpcUnitSurvivalMonitor {
  /*
  分媒体统计每个媒体下各个单元的日耗
  标记每个媒体下的单元数、近7天内新增单元数
   */
  def main(args: Array[String]): Unit = {
    Logger.getRootLogger.setLevel(Level.WARN)
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()

    // 计算日期周期
    val date = args(0).toString

    // 抽取基础表
    val baseData = getData(date, spark)
    val newUnits = getNewUnits(date, spark)

    // 存储基础数据
    baseData
      .withColumn("date", lit(date))
      .select("unitid", "userid", "media", "is_ocpc", "show", "click", "cost", "date")
      .repartition(5)
      .write.mode("overwrite").insertInto("test.unit_cost_by_media_daily")

//    // 分小时计算cvr
//    val data = calculateCvr(baseData, date, spark).cache()
//
//    data.show(10)
//    data
//      .select("unitid", "userid", "conversion_goal", "media", "click", "click_cv", "show_cv", "date", "hour", "hour_int")
//      .repartition(5)
////      .write.mode("overwrite").insertInto("test.ocpc_cvr_delay_hourly")
//      .write.mode("overwrite").insertInto("dl_cpc.ocpc_cvr_delay_hourly")

  }

  def getData(date: String, spark: SparkSession) = {
    // 取历史数据
    val selectCondition = s"`date` = '$date'"

    val conf = ConfigFactory.load("ocpc")
    val conf_key = "medias.total.media_selection"
    val mediaSelection = conf.getString(conf_key)
    // 取数据: score数据
    val sqlRequest =
      s"""
         |SELECT
         |  searchid,
         |  unitid,
         |  userid,
         |  media_appsid,
         |  is_ocpc,
         |  isshow,
         |  isclick,
         |  (case when isclick=1 then price else 0 end) as price
         |FROM
         |  dl_cpc.ocpc_base_unionlog
         |WHERE
         |  $selectCondition
         |AND
         |  $mediaSelection
       """.stripMargin
    println(sqlRequest)
    val clickData = spark
      .sql(sqlRequest)
      .withColumn("media", udfDetermineMedia()(col("media_appsid")))

    val resultDF = clickData
        .groupBy("unitid", "userid", "media", "is_ocpc")
        .agg(
          sum(col("isshow")).alias("show"),
          sum(col("isclick")).alias("click"),
          sum(col("price")).alias("cost")
        )
        .select("unitid", "userid", "media", "is_ocpc", "show", "click", "cost")
        .distinct()

    resultDF
  }

  def getNewUnits(date: String, spark: SparkSession) = {
    // 取历史数据
    val dateConverter = new SimpleDateFormat("yyyy-MM-dd")
    val today = dateConverter.parse(date)
    val calendar = Calendar.getInstance
    calendar.setTime(today)
    calendar.add(Calendar.DATE, -7)
    val yesterday = calendar.getTime
    val date1 = dateConverter.format(yesterday)

    val sqlRequest =
      s"""
         |SELECT
         |    unit_id as unitid,
         |    aduser as userid,
         |    target_medias,
         |    a as media_appsid,
         |    is_ocpc,
         |    create_time,
         |    date(create_time) as create_date
         |from
         |    qttdw.dim_unit_ds
         |lateral view explode(split(target_medias, ',')) b as a
         |WHERE
         |    dt = '$date'
         |and
         |    target_medias != ''
       """.stripMargin
    println(sqlRequest)
    val data = spark.sql(sqlRequest)
      .select("unitid",  "userid", "media_appsid", "is_ocpc", "create_time", "create_date")
      .withColumn("media", udfDetermineMedia()(col("media_appsid")))

    data
      .repartition(5)
      .write.mode("overwrite").saveAsTable("test.check_ocpc_report20190814")

    val resultDF = data
        .filter(s"create_date > '$date1'")
        .withColumn("flag", lit(1))
        .select("unitid",  "userid", "media", "is_ocpc", "flag")
        .distinct()


    resultDF.show(10)
    resultDF
  }



}




package com.cpc.spark.oCPX.oCPC

import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.oCPX.OcpcTools._
import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}


object OcpcBaseJoin {
  def main(args: Array[String]): Unit = {
    /*
    动态计算alpha平滑系数
    1. 基于原始pcoc，计算预测cvr的量纲系数
    2. 二分搜索查找到合适的平滑系数
     */
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()
    Logger.getRootLogger.setLevel(Level.WARN)

    val date = args(0).toString
    val hour = args(1).toString
    val dayInt = args(2).toInt
    println("parameters:")
    println(s"date=$date, hour=$hour, dayInt:$dayInt")

    val slowUnion = getSlowUnion(date, hour, dayInt, spark)
    slowUnion
      .repartition(500)
      .write.mode("overwrite").insertInto("test.ocpc_union_slow_log")



//    val quickUnion = getQuickUnion(date, hour, dayInt, spark)
  }

  def getQuickUnion(date: String, hour: String, dayInt: Int, spark: SparkSession) = {
    // 抽取媒体id
    val conf = ConfigFactory.load("ocpc")
    val conf_key = "medias.total.media_selection"
    val mediaSelection = conf.getString(conf_key)

    // 取历史数据
    val dateConverter = new SimpleDateFormat("yyyy-MM-dd HH")
    val newDate = date + " " + hour
    val today = dateConverter.parse(newDate)
    val calendar = Calendar.getInstance
    calendar.setTime(today)
    calendar.add(Calendar.DATE, -dayInt)
    val yesterday = calendar.getTime
    val tmpDate = dateConverter.format(yesterday)
    val tmpDateValue = tmpDate.split(" ")
    val date1 = tmpDateValue(0)
    val hour1 = tmpDateValue(1)
    val selectCondition = getTimeRangeSqlDay(date1, hour1, date, hour)

    val sqlRequest =
      s"""
         |SELECT
         |  searchid,
         |  unitid,
         |  isclick,
         |  cast(exp_cvr * 1.0 / 1000000 as double) as exp_cvr,
         |  media_appsid,
         |  (case
         |      when (cast(adclass as string) like '134%' or cast(adclass as string) like '107%') then "elds"
         |      when (adslot_type<>7 and cast(adclass as string) like '100%') then "feedapp"
         |      when (adslot_type=7 and cast(adclass as string) like '100%') then "yysc"
         |      when adclass in (110110100, 125100100) then "wzcp"
         |      else "others"
         |  end) as industry,
         |  conversion_goal
         |FROM
         |  dl_cpc.cpc_basedata_click_event
         |WHERE
         |  $selectCondition
         |AND
         |  $mediaSelection
         |AND
         |  ocpc_step in (1, 2)
         |AND
         |  adslot_type != 7
         |AND
         |  isclick = 1
       """.stripMargin
    println(sqlRequest)
    val clickData = spark
      .sql(sqlRequest)
      .withColumn("media", udfDetermineMedia()(col("media_appsid")))

    // 抽取cv数据
    spark.udf.register("getConversionGoal", (traceType: String, traceOp1: String, traceOp2: String) => {
      var result = 0
      if (traceOp1 == "REPORT_DOWNLOAD_PKGADDED") {
        result = 1
      } else if (traceType == "active_third") {
        result = 2
      } else if (traceType == "active15" || traceType == "ctsite_active15") {
        result = 3
      } else if (traceOp1 == "REPORT_USER_STAYINWX") {
        result = 4
      } else {
        result = 0
      }
      result
    })

    val sqlRequest2 =
      s"""
         |SELECT
         |  searchid,
         |  getConversionGoal(trace_type, trace_op1, trace_op2) as conversion_goal
         |FROM
         |  dl_cpc.cpc_basedata_trace_event
         |WHERE
         |  $selectCondition
       """.stripMargin
    println(sqlRequest2)
    val cvDataRaw = spark
      .sql(sqlRequest2)
      //      .withColumn("conversion_goal", udfDetermineConversionGoal()(col("trace_type"), col("trace_op1"), col("trace_op2")))
      .select("searchid", "conversion_goal")
      .filter(s"conversion_goal > 0")
      .withColumn("iscvr", lit(1))
      .distinct()
    val cvData3 = cvDataRaw
      .filter(s"conversion_goal = 2")
      .withColumn("conversion_goal", lit(3))
      .distinct()
    val cvData = cvDataRaw
      .union(cvData3)
      .select("searchid", "conversion_goal", "iscvr")
      .distinct()



    // 数据关联
    val resultDF = clickData
      .join(cvData, Seq("searchid", "conversion_goal"), "left_outer")
      .na.fill(0, Seq("iscvr"))
      .select("searchid", "unitid", "media", "conversion_goal", "industry", "exp_cvr", "isclick", "iscvr", "date", "hour")

    resultDF
  }

  def getSlowUnion(date: String, hour: String, dayInt: Int, spark: SparkSession) = {
    // 抽取媒体id
    val conf = ConfigFactory.load("ocpc")
    val conf_key = "medias.total.media_selection"
    val mediaSelection = conf.getString(conf_key)

    // 取历史数据
    val dateConverter = new SimpleDateFormat("yyyy-MM-dd HH")
    val newDate = date + " " + hour
    val today = dateConverter.parse(newDate)
    val calendar = Calendar.getInstance
    calendar.setTime(today)
    calendar.add(Calendar.DATE, -dayInt)
    val yesterday = calendar.getTime
    val tmpDate = dateConverter.format(yesterday)
    val tmpDateValue = tmpDate.split(" ")
    val date1 = tmpDateValue(0)
    val hour1 = tmpDateValue(1)
    val selectCondition = getTimeRangeSqlDate(date1, hour1, date, hour)

    val sqlRequest =
      s"""
         |SELECT
         |  searchid,
         |  unitid,
         |  isclick,
         |  bid_discounted_by_ad_slot as bid,
         |  price,
         |  cast(exp_cvr as double) as exp_cvr,
         |  media_appsid,
         |  (case
         |      when (cast(adclass as string) like '134%' or cast(adclass as string) like '107%') then "elds"
         |      when (adslot_type<>7 and cast(adclass as string) like '100%') then "feedapp"
         |      when (adslot_type=7 and cast(adclass as string) like '100%') then "yysc"
         |      when adclass in (110110100, 125100100) then "wzcp"
         |      else "others"
         |  end) as industry,
         |  conversion_goal,
         |  date,
         |  hour
         |FROM
         |  dl_cpc.ocpc_base_unionlog
         |WHERE
         |  $selectCondition
         |AND
         |  $mediaSelection
         |AND
         |  is_ocpc = 1
         |AND
         |  isclick = 1
       """.stripMargin
    println(sqlRequest)
    val clickData = spark
      .sql(sqlRequest)
      .withColumn("cvr_goal", udfConcatStringInt("cvr")(col("conversion_goal")))
      .withColumn("media", udfDetermineMedia()(col("media_appsid")))

    // 抽取cv数据
    val sqlRequest2 =
      s"""
         |SELECT
         |  searchid,
         |  label as iscvr,
         |  cvr_goal
         |FROM
         |  dl_cpc.ocpc_label_cvr_hourly
         |WHERE
         |  $selectCondition
       """.stripMargin
    println(sqlRequest2)
    val cvData = spark.sql(sqlRequest2)


    // 数据关联
    val resultDF = clickData
      .join(cvData, Seq("searchid", "cvr_goal"), "left_outer")
      .na.fill(0, Seq("iscvr"))
      .select("searchid", "unitid", "media", "conversion_goal", "industry", "bid", "price", "exp_cvr", "isclick", "iscvr", "date", "hour")

    resultDF
  }



}
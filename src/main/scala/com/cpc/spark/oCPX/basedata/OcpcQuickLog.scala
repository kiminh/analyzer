package com.cpc.spark.oCPX.basedata

import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.oCPX.OcpcTools._
import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._


object OcpcQuickLog {
  def main(args: Array[String]): Unit = {
    /*
    代码测试
     */
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()
    Logger.getRootLogger.setLevel(Level.WARN)

    // bash: 2019-01-02 12
    val date = args(0).toString
    val hour = args(1).toString

    // 点击数据
    val clickData = getClickLog(date, hour, spark)
    clickData
      .withColumn("date", lit(date))
      .withColumn("hour", lit(hour))
      .repartition(10)
//      .write.mode("overwrite").insertInto("test.ocpc_quick_click_log")
      .write.mode("overwrite").insertInto("dl_cpc.ocpc_quick_click_log")

    // 转化数据
    val cvData = getCvLog(date, hour, spark)
    cvData
      .withColumn("date", lit(date))
      .withColumn("hour", lit(hour))
      .repartition(10)
//      .write.mode("overwrite").insertInto("test.ocpc_quick_cv_log")
      .write.mode("overwrite").insertInto("dl_cpc.ocpc_quick_cv_log")


  }

  def getClickLog(date: String, hour: String, spark: SparkSession) = {
    // 抽取媒体id
    val conf = ConfigFactory.load("ocpc")
    val conf_key = "medias.total.media_selection"
    val mediaSelection = conf.getString(conf_key)
    val selectCondition1 = s"day = '$date' and hour = '$hour'"

    val sqlRequest1 =
      s"""
         |SELECT
         |  searchid,
         |  unitid,
         |  userid,
         |  adslot_type,
         |  adclass,
         |  isclick,
         |  cast(exp_cvr * 1.0 / 1000000 as double) as exp_cvr,
         |  media_appsid,
         |  conversion_goal,
         |  ocpc_step,
         |  adclass,
         |  price,
         |  adtype,
         |  bid_ocpc
         |FROM
         |  dl_cpc.cpc_basedata_click_event
         |WHERE
         |  $selectCondition1
         |AND
         |  $mediaSelection
         |AND
         |  ocpc_step >= 1
         |AND
         |  adslot_type != 7
         |AND
         |  isclick = 1
         |AND
         |  antispam_score = 10000
       """.stripMargin

    println(sqlRequest1)
    val data1 = spark
      .sql(sqlRequest1)
      .withColumn("media", udfDetermineMedia()(col("media_appsid")))
      .withColumn("industry", udfDetermineIndustry()(col("adslot_type"), col("adclass")))
      .select("searchid", "unitid", "userid", "adslot_type", "conversion_goal", "media", "industry", "isclick", "exp_cvr", "ocpc_step", "adclass", "price", "adtype", "media_appsid", "bid_ocpc")

    // 取历史数据
    val dateConverter = new SimpleDateFormat("yyyy-MM-dd HH")
    val today = dateConverter.parse(date + " " + hour)
    val calendar = Calendar.getInstance
    calendar.setTime(today)
    calendar.add(Calendar.HOUR, -3)
    val yesterday = calendar.getTime
    val tmpData = dateConverter.format(yesterday)
    val tmpDate = tmpData.split(" ")
    val date1 = tmpDate(0)
    val hour1 = tmpDate(1)
    val selectCondition2 = getTimeRangeSqlDay(date1, hour1, date, hour)


    val sqlRequest2 =
      s"""
         |SELECT
         |  searchid,
         |  ocpc_log
         |FROM
         |  dl_cpc.cpc_basedata_adx_event
         |WHERE
         |  $selectCondition2
         |AND
         |  $mediaSelection
         |AND
         |  is_ocpc = 1
         |AND
         |  adslot_type != 7
         |""".stripMargin
    println(sqlRequest2)
    val data2 = spark.sql(sqlRequest2)

    val clickData = data1
      .join(data2, Seq("searchid"), "left_outer")
      .select("searchid", "unitid", "userid", "adslot_type", "conversion_goal", "media", "industry", "isclick", "exp_cvr", "ocpc_step", "adclass", "price", "adtype", "media_appsid", "ocpc_log", "bid_ocpc")

    clickData
  }

  def getCvLog(date: String, hour: String, spark: SparkSession) = {
    // 抽取cv数据
//    spark.udf.register("getConversionGoal", (traceType: String, traceOp1: String, traceOp2: String) => {
//      var result = -1
//      if (traceOp1 == "REPORT_DOWNLOAD_PKGADDED") {
//        result = 1
//      } else if (traceType == "active_third" && traceOp2 == "") {
//        result = 0 // result = 2
//      } else if (traceType == "active_third" && traceOp2 == "0") {
//        result = 2
//      } else if (traceType == "active_third" && traceOp2 == "1") {
//        result = 5
//      } else if (traceType == "active_third" && traceOp2 == "2") {
//        result = 7
//      } else if (traceType == "active_third" && traceOp2 == "5") {
//        result = 11
//      } else if (traceType == "active_third" && traceOp2 == "6") {
//        result = 6
//      } else if (traceType == "active_third" && traceOp2 == "26") {
//        result = 3
//      } else if (traceType == "active_third" && traceOp2 == "27") {
//        result = 12
//      } else if (traceType == "active15" && traceOp2 == "site_form") {
//        result = 3
//      } else if (traceType == "ctsite_active15" && traceOp2 == "ct_site_form") {
//        result = 3
//      } else if (traceType == "js_active" && traceOp2 == "js_form") {
//        result = 3
//      } else if (traceOp1 == "REPORT_USER_STAYINWX") {
//        result = 4
//      } else if (traceType == "js_active" && traceOp2 == "active_copywx") {
//        result = 4
//      } else if (traceOp1 == "REPORT_ICON_STAYINWX" && traceOp2 == "ON_BANNER") {
//        result = 4
//      } else if (traceOp1 == "REPORT_ICON_STAYINWX" && traceOp2 == "CLICK_POPUPWINDOW_ADDWX") {
//        result = 4
//      } else {
//        result = -1
//      }
//      result
//    })

    spark.udf.register("getConversionGoal", (traceType: String, traceOp1: String, traceOp2: String) => {
      var result = (traceType, traceOp1, traceOp2) match {
        case (_, "REPORT_DOWNLOAD_PKGADDED", _) => 1
        case ("active_third", _, "") | ("active_third", _, "0") => 2
        case ("active_third", _, "26") | ("active15", _, "site_form") | ("ctsite_active15", _, "ct_site_form") | ("js_active", _, "js_form") => 3
        case (_, "REPORT_USER_STAYINWX", _) | ("js_active", _, "active_copywx") | (_, "REPORT_ICON_STAYINWX", "ON_BANNER") | (_, "REPORT_ICON_STAYINWX", "CLICK_POPUPWINDOW_ADDWX") => 4
        case ("active_third", _, "1") => 5
        case (_, _, _) => 0
      }
      result
    })
    val selectCondition = s"day = '$date' and hour = '$hour'"

    val sqlRequest =
      s"""
         |SELECT
         |  searchid,
         |  trace_type,
         |  trace_op1,
         |  trace_op2,
         |  getConversionGoal(trace_type, trace_op1, trace_op2) as conversion_goal
         |FROM
         |  dl_cpc.cpc_basedata_trace_event
         |WHERE
         |  $selectCondition
       """.stripMargin
    println(sqlRequest)
    val cvData = spark
      .sql(sqlRequest)
      .filter(s"conversion_goal > 0")
      .select("searchid", "conversion_goal")
      .distinct()

    cvData
  }


}
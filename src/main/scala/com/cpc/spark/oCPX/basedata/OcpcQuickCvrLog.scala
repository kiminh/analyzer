package com.cpc.spark.oCPX.basedata

import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.oCPX.OcpcTools._
import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object OcpcQuickCvrLog {
  def main(args: Array[String]): Unit = {
    /*
    代码测试
     */
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()
    Logger.getRootLogger.setLevel(Level.WARN)

    // bash: 2019-01-02 12
    val date = args(0).toString
    val hour = args(1).toString


    // 转化数据
    val cvData = getCvLog(date, hour, spark)
    cvData
      .withColumn("date", lit(date))
      .withColumn("hour", lit(hour))
      .repartition(10)
      .write.mode("overwrite").insertInto("test.ocpc_quick_cv_log")
//      .write.mode("overwrite").insertInto("dl_cpc.ocpc_quick_cv_log")


  }



  def getCvLog(date: String, hour: String, spark: SparkSession) = {

    spark.udf.register("getConversionGoal", (traceType: String, traceOp1: String, traceOp2: String) => {
      /*
      生成conversion_goal和conversion_from的Array
       */
      var result = (traceType, traceOp1, traceOp2) match {
        case (_, "REPORT_DOWNLOAD_PKGADDED", _) => Array(1, 3)
        case ("active_third", _, "") | ("active_third", _, "0") => Array(2, 1)
        case ("active_third", _, "26") => Array(3, 1)
        case ("active15", _, _) | ("ctsite_active15", _, _) => Array(3, 2)
        case ("js_active", _, "js_form") => Array(3, 4)
        case (_, "REPORT_USER_STAYINWX", _) | (_, "REPORT_ICON_STAYINWX", "ON_BANNER") | (_, "REPORT_ICON_STAYINWX", "CLICK_POPUPWINDOW_ADDWX") => Array(4, 3)
        case ("js_active", _, "active_copywx") => Array(4, 4)
        case ("active_third", _, "1") => Array(5, 1)
        case (_, _, _) => Array(-1, 0)
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
         |  getConversionGoal(trace_type, trace_op1, trace_op2) as conversion_type
         |FROM
         |  dl_cpc.cpc_basedata_trace_event
         |WHERE
         |  $selectCondition
       """.stripMargin
    println(sqlRequest)
    val cvData = spark
      .sql(sqlRequest)
      .withColumn("conversion_goal", col("conversion_type").getItem(0))
      .withColumn("conversion_from", col("conversion_type").getItem(1))
      .filter(s"conversion_goal > 0 and conversion_type > 0")
      .select("searchid", "conversion_goal", "conversion_from")
      .distinct()

    cvData
  }


}
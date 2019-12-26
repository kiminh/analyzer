package com.cpc.spark.oCPX.deepOcpc.calibration_v6

import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.oCPX.OcpcTools._
import com.typesafe.config.ConfigFactory
//import com.cpc.spark.oCPX.deepOcpc.calibration_v2.OcpcRetentionFactor._
//import com.cpc.spark.oCPX.deepOcpc.calibration_v2.OcpcShallowFactor._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}


object OcpcGetPb_retention {
  /*
  采用基于后验激活率的复合校准策略
  jfb_factor：正常计算
  cvr_factor：
  cvr_factor = (deep_cvr * post_cvr1) / pre_cvr1
  smooth_factor = 0.3
   */
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()
    Logger.getRootLogger.setLevel(Level.WARN)

    val date = args(0).toString
    val hour = args(1).toString
    val version = args(2).toString
    val expTag = args(3).toString
    val hourInt = args(4).toInt
    val minCV1 = args(5).toInt
    val minCV2 = args(6).toInt

    println("parameters:")
    println(s"date=$date, hour=$hour, version:$version, expTag:$expTag, hourInt:$hourInt")

    /*
    jfb_factor calculation
     */

    /*
    cvr factor calculation
     */



  }

  def calculateCvrFactor(date: String, hour: String, spark: SparkSession) = {
    /*
    method to calculate cvr_factor: predict the retention cv

    cvr_factor = post_cvr1 * deep_cvr / pre_cvr2

    deep_cvr: calculate by natural day
    post_cvr1, pre_cvr2: calculate by sliding time window
     */

    // calculate deep_cvr
    val deepCvr = calculateDeepCvr(date, 3, spark)


  }

  def calculateDeepCvr(date: String, dayInt: Int, spark: SparkSession) = {
    // 取历史数据
    val dateConverter = new SimpleDateFormat("yyyy-MM-dd")
    val today = dateConverter.parse(date)
    val calendar = Calendar.getInstance
    calendar.setTime(today)
    calendar.add(Calendar.DATE, -1)
    val date1String = calendar.getTime
    val date1 = dateConverter.format(date1String)
    calendar.add(Calendar.DATE, -1)
    val date2String = calendar.getTime
    val date2 = dateConverter.format(date2String)
    calendar.add(Calendar.DATE, -dayInt)
    val date3String = calendar.getTime
    val date3 = dateConverter.format(date3String)

    // 激活数据
    val sqlRequest1 =
      s"""
         |SELECT
         |  searchid,
         |  unitid,
         |  userid,
         |  conversion_goal,
         |  media_appsid,
         |  1 as iscvr1
         |FROM
         |  dl_cpc.cpc_conversion
         |WHERE
         |  day between '$date3' and '$date2'
         |AND
         |  array_contains(conversion_target, 'api_app_active')
         |""".stripMargin
    println(sqlRequest1)
    val data1Raw = spark
      .sql(sqlRequest1)
      .distinct()

    val data1 = mapMediaName(data1Raw, spark)

    // 次留数据
    val sqlRequest2 =
      s"""
         |SELECT
         |  searchid,
         |  1 as iscvr2
         |FROM
         |  dl_cpc.cpc_conversion
         |WHERE
         |  day >= '$date3'
         |AND
         |  array_contains(conversion_target, 'api_app_retention')
         |""".stripMargin
    println(sqlRequest2)
    val data2 = spark
      .sql(sqlRequest2)
      .distinct()

    val data = data1
      .join(data2, Seq("searchid"), "left_outer")
      .groupBy("unitid", "media")
      .agg(
        sum(col("iscvr1")).alias("cv1"),
        sum(col("iscvr2")).alias("cv2")
      )
      .select("unitid", "media", "cv1", "cv2")
      .withColumn("deep_cvr", col("cv2") * 1.0 / col("cv1"))

    data
  }


}

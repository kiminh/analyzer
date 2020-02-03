package com.cpc.spark.oCPX.cv_recall.deep_cv

import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.oCPX.OcpcTools.getTimeRangeSqlDate
import com.cpc.spark.oCPX.cv_recall.shallow_cv.OcpcShallowCVrecall_predict.cvRecallPredict
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object OcpcDeepCVrecall_assessment {
  def main(args: Array[String]): Unit = {
    // 计算日期周期
    val date = args(0).toString
    val hourInt = args(1).toInt
    val dbName = args(2).toString
    println("parameters:")
    println(s"date=$date")

    // spark app name
    val spark = SparkSession.builder().appName(s"OcpcShallowCVrecall_predict: $date").enableHiveSupport().getOrCreate()

//    val data = cvRecallAssessment(date, hourInt, spark)

    val data = calculateCV(date, spark)


  }

  def calculateCV(date: String, spark: SparkSession) = {
    val sqlRequest1 =
      s"""
         |SELECT
         |    searchid,
         |    unitid,
         |    userid,
         |    deep_conversion_goal,
         |    price,
         |    date as click_date
         |FROM
         |    dl_cpc.ocpc_base_unionlog
         |WHERE
         |    `date` = '$date'
         |AND
         |    is_deep_ocpc = 1
         |AND
         |    isclick = 1
         |""".stripMargin
    println(sqlRequest1)
    val clickData = spark.sql(sqlRequest1)

    val sqlRequest2 =
      s"""
         |SELECT
         |    searchid,
         |    deep_conversion_goal,
         |    date,
         |    1 as iscvr,
         |    row_number() over(partition by searchid, deep_conversion_goal order by date) as seq
         |FROM
         |    dl_cpc.ocpc_label_deep_cvr_hourly
         |WHERE
         |    date >= '$date'
         |""".stripMargin
    println(sqlRequest2)
    val cvData = spark
      .sql(sqlRequest2)
      .filter(s"seq = 1")
      .withColumn("cv_date", col("date"))
      .select("searchid", "deep_conversion_goal", "cv_date")

    val baseData = clickData
      .join(cvData, Seq("searchid", "deep_conversion_goal"), "inner")
      .select("searchid", "unitid", "userid", "deep_conversion_goal", "click_date", "cv_date")
      .withColumn("click_date_diff", udfCalculateDateDiff(date)(col("click_date")))
      .withColumn("cv_date_diff", udfCalculateDateDiff(date)(col("cv_date")))

    baseData.createOrReplaceTempView("base_data")

    val sqlRequest3 =
      s"""
         |SELECT
         |  unitid,
         |  userid,
         |  deep_conversion_goal,
         |  click_date_diff,
         |  cv_date_diff,
         |  count(distinct searchid) as cv
         |FROM
         |  base_data
         |GROUP BY unitid, userid, deep_conversion_goal, click_date_diff, cv_date_diff
         |""".stripMargin
    println(sqlRequest3)
    val data = spark.sql(sqlRequest3).cache()

    data.show(10)

    data
  }

  def udfCalculateDateDiff(date: String) = udf((date1: String) => {
    // 取历史数据
    val dateConverter = new SimpleDateFormat("yyyy-MM-dd")

    val nowTime = dateConverter.parse(date1)
    val ocpcTime = dateConverter.parse(date)
    val dateDiff = (nowTime.getTime() - ocpcTime.getTime()) / (1000 * 60 * 60 * 24)

    dateDiff
  })


}
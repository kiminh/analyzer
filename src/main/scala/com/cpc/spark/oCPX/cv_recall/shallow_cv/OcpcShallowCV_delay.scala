package com.cpc.spark.oCPX.cv_recall.shallow_cv

import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.oCPX.OcpcTools.getTimeRangeSqlDate
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object OcpcShallowCV_delay {
  def main(args: Array[String]): Unit = {
    // 计算日期周期
    val date = args(0).toString
    println("parameters:")
    println(s"date=$date")

    // spark app name
    val spark = SparkSession.builder().appName(s"OcpcShallowCVrecall_predict: $date").enableHiveSupport().getOrCreate()

    val data = getUserDelay(date, spark)

  }

  def getUserDelay(date: String, spark: SparkSession) = {
    val dateConverter = new SimpleDateFormat("yyyy-MM-dd HH")
    val today = dateConverter.parse(date)
    val calendar = Calendar.getInstance
    calendar.setTime(today)
    calendar.add(Calendar.DATE, -1)
    val yesterday = calendar.getTime
    val date1 = dateConverter.format(yesterday)

    val costData = calculateCost(date1, spark)
    val hourDiffData = calculateMinHourDiff(date1, spark)

    val data = hourDiffData
      .join(costData, Seq("userid"), "inner")

    data
  }

  def calculateCost(date: String, spark: SparkSession) = {
    val sqlRequest =
      s"""
         |SELECT
         |  userid,
         |  conversion_goal,
         |  sum(price) * 0.01 as cost
         |FROM
         |  dl_cpc.ocpc_base_unionlog
         |WHERE
         |  date = '$date'
         |AND
         |  isclick = 1
         |AND
         |  conversion_goal in (2, 5)
         |AND
         |  is_ocpc = 1
         |GROUP BY userid, conversion_goal
         |""".stripMargin
    println(sqlRequest)
    val data = spark.sql(sqlRequest)

    data
  }

  def calculateMinHourDiff(date: String, spark: SparkSession) = {
    val sqlRequest =
      s"""
         |SELECT
         |    tt.userid,
         |    min(tt.hour_diff) as hour_diff
         |FROM
         |    (SELECT
         |        t.searchid,
         |        t.userid,
         |        (t.cv_hour-t.click_hour)+(datediff(t.cv_date, t.click_date)*24) as hour_diff,
         |        t.click_date,
         |        t.click_hour,
         |        t.cv_date,
         |        t.cv_hour
         |    FROM
         |        (SELECT
         |            searchid,
         |            userid,
         |            click_timestamp,
         |            from_unixtime(click_timestamp,'yyyy-MM-dd') as click_date,
         |            from_unixtime(click_timestamp,'HH') as click_hour,
         |            day as cv_date,
         |            hour as cv_hour
         |        FROM
         |            dl_cpc.cpc_conversion
         |        WHERE
         |            day = '$date'
         |        AND
         |            conversion_goal in (2, 5)
         |        AND
         |            array_contains(conversion_target, 'api')) as t) as tt
         |GROUP BY tt.userid
         |""".stripMargin
    println(sqlRequest)
    val data = spark.sql(sqlRequest)

    data
  }




}
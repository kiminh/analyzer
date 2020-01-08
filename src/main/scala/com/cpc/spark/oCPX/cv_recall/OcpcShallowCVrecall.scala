package com.cpc.spark.oCPX.cv_recall

import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.ocpcV3.utils
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object OcpcShallowCVrecall {
  def main(args: Array[String]): Unit = {
    // 计算日期周期
    val date = args(0).toString
    println("parameters:")
    println(s"date=$date")

    // spark app name
    val spark = SparkSession.builder().appName(s"OcpcShallowCVrecall: $date").enableHiveSupport().getOrCreate()

    val data = getData(date, spark)

    data
      .repartition(1)
      .write.mode("overwrite").insertInto("test.cv_delay_distribution_daily")


  }

  def getData(date: String, spark: SparkSession) = {
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
         |    tttt.unitid,
         |    tttt.userid,
         |    tttt.conversion_goal,
         |    tttt.hour_diff,
         |    count(distinct tttt.searchid) as cv
         |FROM
         |    (SELECT
         |        a.searchid,
         |        a.unitid,
         |        a.userid,
         |        a.conversion_goal,
         |        a.conversion_from,
         |        a.date as click_date,
         |        a.hour as click_hour,
         |        b.date as cv_date,
         |        b.hour as cv_hour,
         |        (b.hour-a.hour)+(datediff(b.date, a.date)*24) as hour_diff
         |    FROM
         |        (SELECT
         |            *
         |        FROM
         |            dl_cpc.ocpc_base_unionlog
         |        WHERE
         |            date = '$date1'
         |        AND
         |            isclick=1
         |        AND
         |            is_ocpc = 1) as a
         |    INNER JOIN
         |        (SELECT
         |            ttt.searchid,
         |            ttt.conversion_goal,
         |            ttt.conversion_from,
         |            ttt.iscvr,
         |            ttt.date,
         |            ttt.hour
         |        FROM
         |            (SELECT
         |                tt.*,
         |                row_number() over(partition by tt.searchid, tt.conversion_goal, tt.conversion_from order by tt.date, tt.hour asc) as seq
         |            FROM
         |                (SELECT
         |                    searchid,
         |                    conversion_goal,
         |                    conversion_from,
         |                    1 as iscvr,
         |                    date,
         |                    hour
         |                FROM
         |                    dl_cpc.ocpc_cvr_log_hourly
         |                WHERE
         |                    date >= '$date1'
         |                GROUP BY searchid, conversion_goal, conversion_from, date, hour) as tt) as ttt
         |        WHERE
         |            ttt.seq = 1) as b
         |    ON
         |        a.searchid = b.searchid
         |    AND
         |        a.conversion_goal = b.conversion_goal
         |    AND
         |        a.conversion_from = b.conversion_from) as tttt
         |GROUP BY tttt.unitid, tttt.userid, tttt.conversion_goal, tttt.hour_diff
         |""".stripMargin
    println(sqlRequest)
    val data = spark
        .sql(sqlRequest)
        .selectExpr("unitid", "userid", "conversion_goal", "cast(hour_diff as int) hour_diff", "cast(cv as int) cv")
        .withColumn("date", lit(date1))

    data
  }
}
package com.cpc.spark.oCPX.cv_recall.shallow_cv

import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.oCPX.OcpcTools.getTimeRangeSqlDate
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object OcpcShallowCVrecall_predict {
  def main(args: Array[String]): Unit = {
    // 计算日期周期
    val date = args(0).toString
    println("parameters:")
    println(s"date=$date")

    // spark app name
    val spark = SparkSession.builder().appName(s"OcpcShallowCVrecall_predict: $date").enableHiveSupport().getOrCreate()


  }

  def cvRecallPredict(date: String, hourDiff: Int, spark: SparkSession) = {
    val hourList = Array("01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12", "13", "14", "15", "16", "17", "18", "19", "20", "21", "22", "23")
    var data = calculateCV(date, "00", hourDiff, spark)
    for (hour <- hourList) {
      val singleData = calculateCV(date, hour, hourDiff, spark)
      data = data.union(singleData)
    }
    data
  }

  def calculateCV(date: String, hour: String, hourInt: Int, spark: SparkSession) = {
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

    val sqlRequest =
      s"""
         |SELECT
         |    tt.userid,
         |    tt.conversion_goal,
         |    min(tt.recall_value) as recall_value
         |FROM
         |    (SELECT
         |        aa.unitid,
         |        aa.userid,
         |        aa.conversion_goal,
         |        aa.click,
         |        aa.cv as cv1,
         |        bb.click,
         |        bb.cv as cv2,
         |        bb.cv * 1.0 / aa.cv as recall_value
         |    FROM
         |        (SELECT
         |            a.unitid,
         |            a.userid,
         |            a.conversion_goal,
         |            count(a.isclick) as click,
         |            count(b.iscvr) as cv
         |        FROM
         |            (SELECT
         |                *
         |            FROM
         |                dl_cpc.ocpc_base_unionlog
         |            WHERE
         |                $selectCondition
         |            AND
         |                is_ocpc = 1
         |            AND
         |                conversion_goal in (2, 5)
         |            AND
         |                isclick = 1) as a
         |        LEFT JOIN
         |            (SELECT
         |                searchid,
         |                conversion_goal,
         |                conversion_from,
         |                1 as iscvr
         |            FROM
         |                dl_cpc.ocpc_cvr_log_hourly
         |            WHERE
         |                $selectCondition
         |            GROUP BY searchid, conversion_goal, conversion_from) as b
         |        ON
         |            a.searchid = b.searchid
         |        AND
         |            a.conversion_goal = b.conversion_goal
         |        AND
         |            a.conversion_from = b.conversion_from
         |        GROUP BY a.unitid, a.userid, a.conversion_goal) as aa
         |    LEFT JOIN
         |        (SELECT
         |            a.unitid,
         |            a.userid,
         |            a.conversion_goal,
         |            count(a.isclick) as click,
         |            count(b.iscvr) as cv
         |        FROM
         |            (SELECT
         |                *
         |            FROM
         |                dl_cpc.ocpc_base_unionlog
         |            WHERE
         |                $selectCondition
         |            AND
         |                is_ocpc = 1
         |            AND
         |                conversion_goal in (2, 5)
         |            AND
         |                isclick = 1) as a
         |        LEFT JOIN
         |            (SELECT
         |                searchid,
         |                conversion_goal,
         |                conversion_from,
         |                1 as iscvr
         |            FROM
         |                dl_cpc.ocpc_cvr_log_hourly
         |            WHERE
         |                date >= '$date1'
         |            GROUP BY searchid, conversion_goal, conversion_from) as b
         |        ON
         |            a.searchid = b.searchid
         |        AND
         |            a.conversion_goal = b.conversion_goal
         |        AND
         |            a.conversion_from = b.conversion_from
         |        GROUP BY a.unitid, a.userid, a.conversion_goal) as bb
         |    ON
         |        aa.unitid = bb.unitid
         |    AND
         |        aa.userid = bb.userid
         |    AND
         |        aa.conversion_goal = bb.conversion_goal) as tt
         |WHERE
         |    tt.cv1 >= 80
         |GROUP BY tt.userid, tt.conversion_goal
         |""".stripMargin
    println(sqlRequest)
    val data = spark
      .sql(sqlRequest)
      .withColumn("date", lit(date))
      .withColumn("hour", lit(hour))
      .withColumn("hour_diff", lit(hourInt))

    data
  }


}
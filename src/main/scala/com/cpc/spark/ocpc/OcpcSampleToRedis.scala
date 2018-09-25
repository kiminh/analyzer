package com.cpc.spark.ocpc

import java.text.SimpleDateFormat
import java.util.Date
import java.util.Calendar

import com.redis.RedisClient
import org.apache.spark.sql.SparkSession

import com.cpc.spark.qukan.utils.RedisUtil

object OcpcSampleToRedis {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()

    // calculate time period for historical data
    val end_date = args(0)
    val hour = args(1)
    val sdf = new SimpleDateFormat("yyyy-MM-dd")
    val date = sdf.parse(end_date)
    val calendar = Calendar.getInstance
    calendar.setTime(date)
    calendar.add(Calendar.DATE, -1)
    val dt1 = calendar.getTime
    val date1 = sdf.format(dt1)
    calendar.add(Calendar.DATE, -1)
    val dt2 = calendar.getTime
    val date2 = sdf.format(dt2)
    val selectCondition1 = s"`date`='$date2' and hour > '$hour'"
    val selectCondition2 = s"`date`='$date1'"
    val selectCondition3 = s"`date`='$end_date' and hour <= '$hour'"

    // read data and set redis configuration
    val sqlRequest =
      s"""
         |SELECT
         |  userid,
         |  adslotid,
         |  uid,
         |  SUM(cost) as cost,
         |  SUM(cvr_cnt) as cvr_cnt,
         |  SUM(total_cnt) as total_cnt
         |FROM
         |  test.temperate_roi_track
         |WHERE $selectCondition1 OR
         |$selectCondition2 OR
         |$selectCondition3
         |GROUP BY userid, adslotid, uid
       """.stripMargin

    val base = spark.sql(sqlRequest)
    base.createOrReplaceTempView("baseTable")
    val redis = new RedisClient("192.168.80.20", 6390)

    // calculation for ratio: adslotid, uid
    val sqlRequest1 =
      s"""
         |SELECT
         |  adslotid,
         |  uid,
         |  concat(adslotid, "-", uid) id,
         |  SUM(cvr_cnt) / SUM(total_cnt) historical_cvr
         |FROM
         |  baseTable
         |GROUP BY adslotid, uid
       """.stripMargin

    val adslotData = spark.sql(sqlRequest1)

    // calculation for bid and ROI: userid
    val sqlRequest2 =
      s"""
         |SELECT
         |  userid,
         |  SUM(cvr_cnt) / SUM(total_cnt) as historical_cvr,
         |  SUM(cvr_cnt) * 1000 / SUM(cost) as historical_roi
         |FROM
         |  baseTable
         |GROUP BY userid
       """.stripMargin

    val userData = spark.sql(sqlRequest2)

    // save into redis
    RedisUtil.toRedis(adslotData, "id", "historical_cvr", "adslotid-uid-hcvr-")

    RedisUtil.toRedis(userData, "userid", "historical_cvr", "userid-hcvr-")
    RedisUtil.toRedis(userData, "userid", "historical_roi", "userid-hroi-")
  }
}

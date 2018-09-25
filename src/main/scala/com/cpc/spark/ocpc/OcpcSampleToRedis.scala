package com.cpc.spark.ocpc

import java.text.SimpleDateFormat
import java.util.Date
import java.util.Calendar

import com.redis.RedisClient
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

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

    // calculation for ratio: adslotid, uid
    val adslotData = base
      .withColumn("id", concat_ws("-", col("adslotid"), col("uid")))
      .groupBy("id")
      .agg((sum("cvr_cnt")/sum("total_cnt")).alias("historical_cvr"))


    // calculation for bid and ROI: userid
    val userData = base
      .groupBy("userid")
      .agg((sum("cvr_cnt")/sum("total_cnt")).alias("historical_cvr"), (sum("cvr_cnt")*1000/sum("cost")).alias("historical_roi"))

    // save into redis
    RedisUtil.toRedis(adslotData, "id", "historical_cvr", "adslotid-uid-hcvr-")
    RedisUtil.toRedis(userData, "userid", "historical_cvr", "userid-hcvr-")
    RedisUtil.toRedis(userData, "userid", "historical_roi", "userid-hroi-")
  }
}

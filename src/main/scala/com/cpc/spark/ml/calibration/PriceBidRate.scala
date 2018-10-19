package com.cpc.spark.ml.calibration

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import com.cpc.spark.common.Utils
import com.cpc.spark.qukan.utils.RedisUtil

/**
  * author: huazhenhao
  * date: 10/19/18
  */
object PriceBidRate {
  def main(args: Array[String]): Unit = {
    val endDt = args(0)
    val endHour = args(1)
    val range = args(2).toInt
    val upload = args(3).toBoolean

    val endTime = LocalDateTime.parse(s"$endDt-$endHour", DateTimeFormatter.ofPattern("yyyy-MM-dd-HH"))
    val startTime = endTime.minusHours(Math.max(range - 1, 0))

    val startDate = startTime.format(DateTimeFormatter.ofPattern("yyyy-MM-dd"))
    val startHour = startTime.format(DateTimeFormatter.ofPattern("HH"))

    println(s"endDt=$endDt")
    println(s"endHour=$endHour")
    println(s"range=$range")
    println(s"startDt=$startDate")
    println(s"startHour=$startHour")

    val session = Utils.buildSparkSession("calibration_check")
    val timeRangeSql = Utils.getTimeRangeSql(startDate, startHour, endDt, endHour)

    // get union log
    val sql = s"""
                 | select
                 |  ideaid,
                 |  count(1),
                 |  avg(price/bid)
                 |  variance(price/bid)
                 | from dl_cpc.cpc_union_log
                 | where $timeRangeSql
                 |  and isclick=1
                 |  and ext['antispam'].int_value = 0
                 |  and ideaid > 0
                 |  and adsrc = 1
                 |  and adslot_type in 1
                 |  and userid > 0
                 |  and (ext["charge_type"] IS NULL
                 |    OR ext["charge_type"].int_value = 1)
                 |  group by ideaid
       """.stripMargin

    println(s"sql:\n$sql")
    val log = session.sql(sql)
    val redis = RedisUtil.getRedisClient(1)
    var total = 0
    var passed = 0
    log.rdd.collect().map( x => {
      total += 1
      val ideaid = x.getAs[Number](0)
      val count = x.getAs[Number](1)
      val avg = x.getAs[Double](2)
      if (count.intValue() > 100) {
        passed += 1
        if (upload) {
          redis.set("i" + ideaid.toString, avg)
        }
      }
    })
    redis.disconnect
    println(s"total: $total")
    println(s"passed: $passed")
  }
}

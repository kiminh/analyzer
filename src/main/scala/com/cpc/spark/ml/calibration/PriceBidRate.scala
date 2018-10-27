package com.cpc.spark.ml.calibration

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import com.cpc.spark.common.Utils
import com.cpc.spark.ml.calibration.HourlyCalibration.newDestDir
import com.cpc.spark.ml.common.{Utils => MUtils}
import com.typesafe.config.ConfigFactory
import userocpc.userocpc.BidAdjustmentConfig

import scala.collection.mutable

/**
  * author: huazhenhao
  * date: 10/19/18
  */
object PriceBidRate {
  def main(args: Array[String]): Unit = {

    val LOCAL_PATH = "/home/cpc/price_rate/rate.pb"
    val DEST_PATH = "/home/work/mlcpp/data/rate.pb"
    val NEW_DEST_PATH = "/home/cpc/model_server/data/rate.pb"


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
    println(s"upload=$upload")

    val session = Utils.buildSparkSession("calibration_check")
    val timeRangeSql = Utils.getTimeRangeSql(startDate, startHour, endDt, endHour)


    // get union log
    val sql = s"""
                 | select
                 |  ideaid,
                 |  count(*),
                 |  avg(price/bid),
                 |  variance(price/bid)
                 | from dl_cpc.cpc_union_log
                 | where $timeRangeSql
                 |  and isclick=1
                 |  and ext['antispam'].int_value = 0
                 |  and ideaid > 0
                 |  and adsrc = 1
                 |  and adslot_type = 1
                 |  and userid > 0
                 |  and media_appsid in ('80000001', '80000002')
                 |  and (ext["charge_type"] IS NULL
                 |    OR ext["charge_type"].int_value = 1)
                 |  group by ideaid
       """.stripMargin
    println(s"sql:\n$sql")

    val log = session.sql(sql)
    var total = 0
    var passed = 0
    val ideaMap = mutable.Map[Int, Double]()
    log.rdd.collect().map( x => {
      total += 1
      val ideaid = x.getAs[Number](0)
      val count = x.getAs[Number](1)
      val avg = x.getAs[Double](2)
      if (count.intValue() > 100) {
        passed += 1
        ideaMap.put(ideaid.intValue(), avg)
      }
    })
    println(s"idea id total: $total")
    println(s"idea id passed: $passed")

    val slotSQL =
      s"""
         |select
         |  adslotid,
         |  count(*),
         |  avg(price/bid),
         |  variance(price/bid)
         | from dl_cpc.cpc_union_log
         | where $timeRangeSql
         |  and isclick=1
         |  and ext['antispam'].int_value = 0
         |  and ideaid > 0
         |  and adsrc = 1
         |  and adslot_type = 1
         |  and userid > 0
         |  and media_appsid in ('80000001', '80000002')
         |  and (ext["charge_type"] IS NULL
         |    OR ext["charge_type"].int_value = 1)
         |  group by adslotid
       """.stripMargin
    total = 0
    passed = 0
    val slotIDMap = mutable.Map[String, Double]()
    session.sql(slotSQL).rdd.collect().map( x => {
      total += 1
      val slotid = x.getAs[String](0)
      val count = x.getAs[Number](1)
      val avg = x.getAs[Double](2)
      if (count.intValue() > 100) {
        passed += 1
        slotIDMap.put(slotid, avg)
      }
    })
    println(s"slot id total: $total")
    println(s"slot id passed: $passed")

    val globalSQL =
      s"""
         |select
         |  avg(price/bid)
         | from dl_cpc.cpc_union_log
         | where $timeRangeSql
         |  and isclick=1
         |  and ext['antispam'].int_value = 0
         |  and ideaid > 0
         |  and adsrc = 1
         |  and adslot_type = 1
         |  and userid > 0
         |  and (ext["charge_type"] IS NULL
         |    OR ext["charge_type"].int_value = 1)
         |  and media_appsid in ('80000001', '80000002')
       """.stripMargin
    val globalAvg = session.sql(globalSQL).rdd.collect().head.getAs[Number](0).doubleValue()

    println(s"global avg: $globalAvg")
    val config = BidAdjustmentConfig(
      ideaRate = ideaMap.toMap,
      adslotidRate = slotIDMap.toMap,
      globalRate = globalAvg
    )

    Utils.saveProtoToFile(config, LOCAL_PATH)
    if (upload) {
      val conf = ConfigFactory.load()
      println(MUtils.updateMlcppOnlineData(LOCAL_PATH, DEST_PATH, conf))
      println(MUtils.updateMlcppModelData(LOCAL_PATH, NEW_DEST_PATH, conf))
    }

  }
}

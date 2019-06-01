package com.cpc.spark.ml.calibration

import java.io.{File, FileOutputStream, PrintWriter}
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import com.cpc.spark.OcpcProtoType.model_novel_v3.OcpcSuggestCPAV3.matchcvr
import com.cpc.spark.common.Utils
import com.cpc.spark.ml.calibration.HourlyCalibration._
import com.cpc.spark.ml.common.{Utils => MUtils}
import com.cpc.spark.ocpc.OcpcUtils.getTimeRangeSql4
import com.typesafe.config.ConfigFactory
import com.cpc.spark.ml.calibration.MultiDimensionCalibOnQttV2._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}


object MultiDimensionCalibOnQttCvr {

  def main(args: Array[String]): Unit = {

    // new calibration
    val endDate = args(0)
    val endHour = args(1)
    val hourRange = args(2).toInt
    val softMode = args(3).toInt
    val media = args(4)
    val model = args(5)
    val conf = ConfigFactory.load("ocpc")
    val conf_key = "medias." + media + ".media_selection"
    val mediaSelection = conf.getString(conf_key)


    val endTime = LocalDateTime.parse(s"$endDate-$endHour", DateTimeFormatter.ofPattern("yyyy-MM-dd-HH"))
    val startTime = endTime.minusHours(Math.max(hourRange - 1, 0))

    val startDate = startTime.format(DateTimeFormatter.ofPattern("yyyy-MM-dd"))
    val startHour = startTime.format(DateTimeFormatter.ofPattern("HH"))

    println(s"endDate=$endDate")
    println(s"endHour=$endHour")
    println(s"hourRange=$hourRange")
    println(s"startDate=$startDate")
    println(s"startHour=$startHour")
    println(s"softMode=$softMode")

    // build spark session
    val session = Utils.buildSparkSession("hourlyCalibration")
    val timeRangeSql = Utils.getTimeRangeSql_3(startDate, startHour, endDate, endHour)
    val selectCondition2 = getTimeRangeSql4(startDate, startHour, endDate, endHour)
    val selectCondition3 = s"day between '$startDate' and '$endDate'"

    // get union log
    val clicksql = s"""
                 |select a.searchid, cast(a.raw_cvr as bigint) as ectr, substring(a.adclass,1,6) as adclass,
                 |a.cvr_model_name as model, a.adslotid, a.ideaid,
                 |case when user_req_ad_num = 1 then '1'
                 |  when user_req_ad_num = 2 then '2'
                 |  when user_req_ad_num in (3,4) then '4'
                 |  when user_req_ad_num in (5,6,7) then '7'
                 |  else '8' end as user_req_ad_num
                 |  from dl_cpc.slim_union_log a
                 |  join dl_cpc.dw_unitid_detail b
                 |    on a.unitid = b.unitid
                 |    and a.dt  = b.day
                 |    and $selectCondition3
                 |    and b.conversion_target[0] not in ('none','site_uncertain')
                 |  where $timeRangeSql
                 |  and a.$mediaSelection and isclick = 1
                 |  and a.cvr_model_name = '$model'
                 |  and a.ideaid > 0 and a.adsrc = 1 AND a.userid > 0
                 |  AND (charge_type IS NULL OR charge_type = 1)
       """.stripMargin
    println(s"sql:\n$clicksql")
    val clickData = session.sql(clicksql)
    val cvrsql =s"""
                 |select distinct a.searchid,
                 |       a.conversion_target as unit_target,
                 |       b.conversion_target[0] as real_target
                 |from
                 |   (select *
                 |    from dl_cpc.dm_conversions_for_model
                 |   where $selectCondition2
                 |and size(conversion_target)>0) a
                 |join dl_cpc.dw_unitid_detail b
                 |    on a.unitid=b.unitid
                 |    and b.$selectCondition3
       """.stripMargin
    val cvrData = session.sql(cvrsql)
      .withColumn("iscvr",matchcvr(col("unit_target"),col("real_target")))
      .filter("iscvr = 1")
      .select("searchid", "iscvr")
    val log = clickData.join(cvrData,Seq("searchid"),"left")
        .withColumn("isclick",col("iscvr"))
    log.show(10)

    LogToPb(log, session, model, softMode)

  }
}
package com.cpc.spark.ml.calibration

import java.io.{File, FileOutputStream, PrintWriter}
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import com.cpc.spark.common.Utils
import com.cpc.spark.ml.common.{Utils => MUtils}
import com.typesafe.config.ConfigFactory
import mlmodel.mlmodel.{CalibrationConfig, IRModel, PostCalibrations}
import org.apache.spark.sql.functions._
import com.cpc.spark.ml.calibration.MultiDimensionCalibOnQtt._


object MultiDimensionCalibOnMiduV2 {

  val localDir = "/home/cpc/scheduled_job/hourly_calibration/"
  val destDir = "/home/work/mlcpp/calibration/"
  val MAX_BIN_COUNT = 10
  val MIN_BIN_SIZE = 100000

  def main(args: Array[String]): Unit = {

    // parse and process input
    val endDate = args(0)
    val endHour = args(1)
    val hourRange = args(2).toInt
    val softMode = args(3).toInt
    val model = "novel-ctr-dnn-rawid-v8"
    val calimodel ="novel-ctr-dnn-rawid-v8-postcali"


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

    // get union log
    val sql = s"""
                 |select isclick, cast(raw_ctr as bigint) as ectr, ctr_model_name, adslotid as adslot_id, cast(ideaid as string) ideaid,
                 |case when user_req_ad_num = 1 then '1'
                 |  when user_req_ad_num = 2 then '2'
                 |  when user_req_ad_num in (3,4) then '4'
                 |  when user_req_ad_num in (5,6,7) then '7'
                 |  else '8' end as user_req_ad_num
                 | from dl_cpc.slim_union_log
                 | where $timeRangeSql
                 | and media_appsid in ('80001098', '80001292') and isshow = 1
                 | and ctr_model_name in ('$model','$calimodel')
                 | and ideaid > 0 and adsrc = 1 AND userid > 0
                 | AND (charge_type IS NULL OR charge_type = 1)
       """.stripMargin
    println(s"sql:\n$sql")
    val log = session.sql(sql)

    val group1 = log.groupBy("ideaid","user_req_ad_num","adslot_id").count().withColumn("count1",col("count"))
      .withColumn("group",concat_ws("_",col("ideaid"),col("user_req_ad_num"),col("adslot_id")))
      .filter("count1>100000")
      .select("ideaid","user_req_ad_num","adslot_id","group")
    val group2 = log.groupBy("ideaid","user_req_ad_num").count().withColumn("count2",col("count"))
      .withColumn("group",concat_ws("_",col("ideaid"),col("user_req_ad_num")))
      .filter("count2>100000")
      .select("ideaid","user_req_ad_num","group")
    val group3 = log.groupBy("ideaid").count().withColumn("count3",col("count"))
      .filter("count3>10000")
      .withColumn("group",col("ideaid"))
      .select("ideaid","group")

    val data1 = log.join(group1,Seq("user_req_ad_num","adslot_id","ideaid"),"inner")
    val data2 = log.join(group2,Seq("ideaid","user_req_ad_num"),"inner")
    val data3 = log.join(group3,Seq("ideaid"),"inner")

    //create cali pb
    val calimap1 = GroupToConfig(data1, session,calimodel)
    val calimap2 = GroupToConfig(data2, session,calimodel)
    val calimap3 = GroupToConfig(data3, session,calimodel)
    val calimap = calimap1 ++ calimap2 ++ calimap3
    val califile = PostCalibrations(calimap.toMap)
    val localPath = saveProtoToLocal(model, califile)
    saveFlatTextFileForDebug(model, califile)
    if (softMode == 0) {
      val conf = ConfigFactory.load()
      println(MUtils.updateMlcppOnlineData(localPath, destDir + s"calibration-$calimodel.mlm", conf))
      println(MUtils.updateMlcppModelData(localPath, newDestDir + s"calibration-$calimodel.mlm", conf))
    }
  }
}
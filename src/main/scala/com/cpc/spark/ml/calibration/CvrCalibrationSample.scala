package com.cpc.spark.ml.calibration

import java.io.{File, FileOutputStream, PrintWriter}
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import com.cpc.spark.OcpcProtoType.model_novel_v3.OcpcSuggestCPAV3.matchcvr
import com.cpc.spark.common.Utils
import com.cpc.spark.ml.common.{Utils => MUtils}
import com.typesafe.config.ConfigFactory
import mlmodel.mlmodel.{CalibrationConfig, IRModel}
import org.apache.spark.SparkContext
import org.apache.spark.mllib.regression.IsotonicRegression
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions.col


object CvrCalibrationSample {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()
    // parse and process input
    val date = args(0)

    // get union log
    val sql = s"""
                 |select searchid, raw_cvr, cvr_model_name, adslotid, ideaid, user_req_ad_num,dt , hour
                 | from dl_cpc.slim_union_log
                 | where dt = '$date'
                 | and media_appsid in ('80000001', '80000002') and isclick = 1
                 | and cvr_model_name = 'qtt-cvr-dnn-rawid-v1-180'
                 | and ideaid > 0 and adsrc = 1 AND userid > 0
                 | AND (charge_type IS NULL OR charge_type = 1)
       """.stripMargin
    println(s"sql:\n$sql")
    val ctrdata = spark.sql(sql)

    val sqlRequest2 =
      s"""
         |select distinct a.searchid,
         |        a.conversion_target as real_target,
         |        b.conversion_target[0] as unit_target
         |from dl_cpc.dm_conversions_for_model a
         |join dl_cpc.dw_unitid_detail b
         |    on a.unitid=b.unitid
         |    and a.day = b.day
         |    and b.day = '$date'
         |where a.day = '$date'
         |and size(a.conversion_target)>0
       """.stripMargin
    println(sqlRequest2)
    val cvrData = spark.sql(sqlRequest2)
      .withColumn("iscvr",matchcvr(col("real_target"),col("unit_target")))
      .filter("iscvr = 1")

    val sample = ctrdata.join(cvrData,Seq("searchid"),"left")
      .select("searchid","raw_cvr","cvr_model_name","adslotid","ideaid","user_req_ad_num","iscvr","dt","hour")

    sample.repartition(1).write.mode("overwrite").insertInto("dl_cpc.qtt_cvr_calibration_sample")
  }
}
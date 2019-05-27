//package com.cpc.spark.ml.calibration
//
//import java.io.{File, FileOutputStream, PrintWriter}
//import java.time.LocalDateTime
//import java.time.format.DateTimeFormatter
//
//import com.cpc.spark.OcpcProtoType.model_novel_v3.OcpcSuggestCPAV3.matchcvr
//import com.cpc.spark.common.Utils
//import com.cpc.spark.ml.common.{Utils => MUtils}
//import com.typesafe.config.ConfigFactory
//import mlmodel.mlmodel.{CalibrationConfig, IRModel}
//import org.apache.spark.SparkContext
//import org.apache.spark.mllib.regression.IsotonicRegression
//import org.apache.spark.rdd.RDD
//import org.apache.spark.sql.Row
//import org.apache.spark.sql.functions.col
//
//
//object CvrCalibrationSample {
//
//  def main(args: Array[String]): Unit = {
//
//    // parse and process input
//    val date = args(0)
//    val hour = args(1)
//
//    // get union log
//    val sql = s"""
//                 |select raw_cvr, cvr_model_name, adslotid, ideaid, user_req_ad_num,date , hour
//                 | from dl_cpc.slim_union_log
//                 | where dt = '$date'
//                 | and media_appsid in ('80000001', '80000002') and isclick = 1
//                 | and cvr_model_name = 'qtt-cvr-dnn-rawid-v1-180'
//                 | and ideaid > 0 and adsrc = 1 AND userid > 0
//                 | AND (charge_type IS NULL OR charge_type = 1))
//       """.stripMargin
//    println(s"sql:\n$sql")
//    val log = session.sql(sql)
//
//    val sqlRequest2 =
//      s"""
//         |select distinct a.searchid,
//         |        a.conversion_target as real_target,
//         |        b.conversion_target[0] as unit_target
//         |from dl_cpc.dm_conversions_for_model a
//         |join dl_cpc.dw_unitid_detail b
//         |    on a.unitid=b.unitid
//         |    and a.day = b.day
//         |    and b.$selectCondition2
//         |where a.$selectCondition2
//         |and size(a.conversion_target)>0
//       """.stripMargin
//    println(sqlRequest2)
//    val cvrData = spark.sql(sqlRequest2)
//      .withColumn("iscvr",matchcvr(col("real_target"),col("unit_target")))
//      .filter("iscvr = 1")
//  }
//}
package com.cpc.spark.ocpcV3.ocpc.toolkit

import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.ocpc.OcpcUtils._
import com.cpc.spark.ocpc.utils.OcpcUtils.getIdeaUpdates
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType
import com.cpc.spark.udfs.Udfs_wj._
import com.typesafe.config.ConfigFactory


object OcpcCheckByAdslotid {
  def main(args: Array[String]): Unit = {
    /*
    根据时间和adslotid分析暗投ocpc的广告数据
     */
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()

    // 计算日期周期
    val date = args(0).toString
    val hour = args(1).toString
    val adslotid = args(2).toString

    // 根据日期和小时还有adslotid收集过滤数据
    val rawData = filterData(date, hour, adslotid, spark)
    val summaryData = calculateOcpcSummary(date, hour, spark)
  }

  def filterData(date: String, hour: String, adslotid: String, spark: SparkSession) = {
    /*
    分别搜集ctr、cvr1和cvr2的data
     */

    // ctr数据
    val sqlRequest1 =
      s"""
         |select
         |    uid,
         |    timestamp,
         |    searchid,
         |    userid,
         |    ext['exp_ctr'].int_value * 1.0 / 1000000 as exp_ctr,
         |    ext['exp_cvr'].int_value * 1.0 / 1000000 as exp_cvr,
         |    isclick,
         |    isshow,
         |    ideaid,
         |    adslotid,
         |    exptags,
         |    price,
         |    ext_int['bid_ocpc'] as bid_ocpc,
         |    ext_int['is_ocpc'] as is_ocpc,
         |    ext_string['ocpc_log'] as ocpc_log
         |from
         |    dl_cpc.cpc_union_log
         |WHERE
         |    `date` = '$date'
         |and
         |    `hour` <= '$hour'
         |and
         |    media_appsid  in ("80000001", "80000002")
         |and
         |    ext['antispam'].int_value = 0
         |and adsrc = 1
         |and isshow=1
         |and adslot_type in (1,2,3)
         |and round(ext["adclass"].int_value/1000) != 132101  --去掉互动导流
         |and length(ocpc_log)>0
         |and adslotid='$adslotid'
       """.stripMargin

    println(sqlRequest1)
    val ctrData = spark
      .sql(sqlRequest1)
      .withColumn("ocpc_log_dict", udfStringToMap()(col("ocpc_log")))

    // cvr1
    val cvr1Data = spark
      .table("dl_cpc.ml_cvr_feature_v1")
      .where(s"`date`='$date' and `hour`<='$hour'")
      .withColumn("iscvr1", col("label2"))
      .filter("iscvr1=1")
      .select("searchid", "iscvr1")
      .distinct()

    // cvr2
    val cvr2Data = spark
      .table("dl_cpc.ml_cvr_feature_v2")
      .where(s"`date`='$date' and `hour`<='$hour'")
      .withColumn("iscvr2", col("label"))
      .filter("iscvr2=1")
      .select("searchid", "iscvr2")
      .distinct()

    // 数据关联
    val data = ctrData
      .join(cvr1Data, Seq("searchid"), "left_outer")
      .join(cvr2Data, Seq("searchid"), "left_outer")

    data.createOrReplaceTempView("base_table")

    val sqlRequest2 =
      s"""
         |SELECT
         |  searchid,
         |  unitid,
         |  userid,
         |  isclick,
         |  price,
         |  exp_ctr,
         |  exp_cvr,
         |  is_ocpc,
         |  ocpc_log_dict['cpagiven'] as cpagiven,
         |  ocpc_log_dict['kvalue'] as kvalue,
         |  ocpc_log_dict['dynamicbid'] as dynamicbid,
         |  ocpc_log_dict['ocpcstep'] as ocpcstep,
         |  iscvr1,
         |  iscvr2
         |FROM
         |  base_table
       """.stripMargin
    println(sqlRequest2)

    val resultDF = spark.table(sqlRequest2)
    resultDF

  }

  def calculateOcpcSummary(date: String, hour: String, spark: SparkSession) = {

  }

}


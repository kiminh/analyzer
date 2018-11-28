package com.cpc.spark.ocpcV3.ocpcNovel.data

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import com.cpc.spark.ocpc.utils.OcpcUtils._

object OcpcCompleteUnionlog {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()

    // 计算日期周期
    val date = args(0).toString
    val hour = args(1).toString

    // TODO 测试
  }

  def getBaseData(date: String, hour: String, spark: SparkSession) = {
    var selectWhere = s"`date`='$date' and hour = '$hour'"

    var sqlRequest =
      s"""
         |select
         |    searchid,
         |    uid,
         |    ideaid,
         |    unitid,
         |    price,
         |    bid,
         |    userid,
         |    media_appsid,
         |    ext['adclass'].int_value as adclass,
         |    ext['exp_cvr'].int_value * 1.0 / 1000000 as exp_cvr,
         |    isclick,
         |    isshow,
         |    ext_int['is_api_callback'] as is_api_callback
         |from dl_cpc.cpc_union_log
         |where $selectWhere
         |and isclick is not null
         |and media_appsid in ("80001098","80001292","80000001", "80000002")
         |and isshow = 1
         |and ext['antispam'].int_value = 0
         |and ideaid > 0
         |and adsrc = 1
         |and adslot_type in (1,2,3)
      """.stripMargin
    println(sqlRequest)
    val resultDF = spark.sql(sqlRequest)

    resultDF.show(10)
    resultDF
  }


  def getCVR1(date: String, hour: String, spark: SparkSession) = {
    var selectWhere = s"`date`='$date' and hour = '$hour'"
    val sqlRequest =
      s"""
         |SELECT
         |  searchid,
         |  label2 as iscvr1
         |FROM
         |  dl_cpc.ml_cvr_feature_v1
         |WHERE
         |  where $selectWhere
         |AND
         |  label2=1
       """.stripMargin
    println(sqlRequest)
    val resultDF = spark.sql(sqlRequest).distinct()
    resultDF.show(10)
    resultDF
  }

  def getCVR2(date: String, hour: String, spark: SparkSession) = {
    var selectWhere = s"`date`='$date' and hour = '$hour'"

    val sqlRequest =
      s"""
         |SELECT
         |  searchid,
         |  label as iscvr2
         |FROM
         |  dl_cpc.ml_cvr_feature_v2
         |WHERE
         |  where $selectWhere
         |AND
         |  label=1
       """.stripMargin
    println(sqlRequest)
    val resultDF = spark.sql(sqlRequest).distinct()
    resultDF.show(10)
    resultDF
  }

  def getCVR3(date: String, hour: String, spark: SparkSession) = {
    var selectWhere = s"`date`='$date' and hour = '$hour'"
    val sqlRequest =
      s"""
         |SELECT
         |  searchid,
         |  label2 as iscvr3
         |FROM
         |  dl_cpc.ml_cvr_feature_v1
         |WHERE
         |  where $selectWhere
         |AND
         |  label2=1
       """.stripMargin
    println(sqlRequest)
    val resultDF = spark.sql(sqlRequest).distinct()
    resultDF.show(10)
    resultDF
  }

  def getGroupbyData(ctrData: DataFrame, cvr1: DataFrame, cvr2: DataFrame, cvr3: DataFrame, spark: SparkSession) = {
    val rawData = ctrData
      .join(cvr1, Seq("searchid"), "left_outer")
      .join(cvr2, Seq("searchid"), "left_outer")
      .join(cvr3, Seq("searchid"), "left_outer")
    rawData.show(10)
    rawData.createOrReplaceTempView("raw_table")

    val sqlRequest =
      s"""
         |SELECT
         |  ideaid,
         |  unitid,
         |  adclass,
         |  media_appsid,
         |  SUM(case when isclick=1 then price else 0 end) as total_price,
         |  SUM(isshow) as show_cnt,
         |  SUM(isclick) as ctr_cnt,
         |  SUM(case when isclick=1 then bid else 0 end) as total_bid,
         |  SUM(case when is_api_callback!=1 then isclick else 0 end) as noapi_click,
         |  SUM(case when is_api_callback!=1 and isclick=1 then exp_cvr else 0 end) as noapi_pcvr_total,
         |  SUM(case when is_api_callback=1 then isclick else 0 end) as api_click,
         |  SUM(case when is_api_callback=1 and isclick=1 then exp_cvr else 0 end) as api_pcvr_total,
         |  SUM(iscvr1) as cvr1cnt,
         |  SUM(iscvr2) as cvr2cnt,
         |  SUM(iscvr3) as cvr3cnt
         |FROM
         |  raw_table
         |GROUP BY ideaid, unitid, adclass, media_appsid
       """.stripMargin
    println(sqlRequest)

    val resultDF = spark.sql(sqlRequest)
    resultDF.show(10)
    resultDF

  }

}
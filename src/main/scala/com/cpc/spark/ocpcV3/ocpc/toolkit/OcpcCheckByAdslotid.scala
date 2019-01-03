package com.cpc.spark.ocpcV3.ocpc.toolkit

import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.ocpc.OcpcUtils._
import com.cpc.spark.ocpc.utils.OcpcUtils.getIdeaUpdates
import com.cpc.spark.ocpcV3.ocpc.OcpcUtils.getTimeRangeSql2
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
    val adslotid = args(0).toString

    // 根据日期和小时还有adslotid收集过滤数据
    val rawData = filterData(adslotid, spark)
//    val detailData = calculateOcpcDetail(rawData, spark)
//    val summaryData = calculateOcpcSummary(rawData, spark)

//    detailData.write.mode("overwrite").saveAsTable("test.ocpc_check_adslotid20181228detail")
//    summaryData.write.mode("overwrite").saveAsTable("test.ocpc_check_adslotid20181228summary")
  }

  def filterData(adslotid: String, spark: SparkSession) = {
    /*
    分别搜集ctr、cvr1和cvr2的data
     */
    val selectCondition = s"`date`='2018-12-31'"
    // ctr数据
    val sqlRequest1 =
      s"""
         |select
         |    uid,
         |    timestamp,
         |    searchid,
         |    userid,
         |    unitid,
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
         |    ext_string['ocpc_log'] as ocpc_log,
         |    date,
         |    hour
         |from
         |    dl_cpc.cpc_union_log
         |WHERE
         |    $selectCondition
         |and
         |    media_appsid  in ("80000001", "80000002")
         |and
         |    ext['antispam'].int_value = 0
         |and adsrc = 1
         |and isshow=1
         |and adslot_type in (1,2,3)
         |and round(ext["adclass"].int_value/1000) != 132101  --去掉互动导流
         |and adslotid='$adslotid'
       """.stripMargin

    println(sqlRequest1)
    val ctrData = spark
      .sql(sqlRequest1)
      .withColumn("ocpc_log_dict", udfStringToMap()(col("ocpc_log")))

    // cvr1
    val cvr1Data = spark
      .table("dl_cpc.ml_cvr_feature_v1")
      .where(selectCondition)
      .filter(s"label_type!=12")
      .withColumn("iscvr1", col("label2"))
      .filter("iscvr1=1")
      .select("searchid", "iscvr1")
      .distinct()

    // cvr2
    val cvr2Data = spark
      .table("dl_cpc.ml_cvr_feature_v2")
      .where(selectCondition)
      .withColumn("iscvr2", col("label"))
      .filter("iscvr2=1")
      .select("searchid", "iscvr2")
      .distinct()

    // 数据关联
    val data = ctrData
      .join(cvr1Data, Seq("searchid"), "left_outer")
      .join(cvr2Data, Seq("searchid"), "left_outer")

    val tableName = "test.ocpc_check_adslotid20181231"
    data.write.mode("overwrite").saveAsTable(tableName)
    data.show(10)
    val resultDF = data
      .selectExpr("searchid", "unitid", "userid", "isclick", "price", "exp_ctr", "exp_cvr", "is_ocpc", "cast(ocpc_log_dict['cpagiven'] as double) cpagiven", "cast(ocpc_log_dict['kvalue'] as double) kvalue", "cast(ocpc_log_dict['dynamicbid'] as double) dynamicbid", "cast(ocpc_log_dict['ocpcstep'] as int) ocpcstep", "iscvr1", "iscvr2", "date", "hour")
//
//    val sqlRequest2 =
//      s"""
//         |SELECT
//         |  searchid,
//         |  unitid,
//         |  userid,
//         |  isclick,
//         |  price,
//         |  exp_ctr,
//         |  exp_cvr,
//         |  is_ocpc,
//         |  ocpc_log_dict['cpagiven'] as cpagiven,
//         |  ocpc_log_dict['kvalue'] as kvalue,
//         |  ocpc_log_dict['dynamicbid'] as dynamicbid,
//         |  ocpc_log_dict['ocpcstep'] as ocpcstep,
//         |  (case when length(ocpc_log) > 0 then 1 else 0 end) as ocpc_flag,
//         |  iscvr1,
//         |  iscvr2,
//         |  date,
//         |  hour
//         |FROM
//         |  $tableName
//       """.stripMargin
//    println(sqlRequest2)
//
//    val resultDF = spark.table(sqlRequest2)
    resultDF

  }

  def calculateOcpcDetail(data: DataFrame, date: String, hour: String, spark: SparkSession) = {
    /*
    统计cost，acp，以及cpa等等，分两张表，一张详情表，一张汇总表
    详情表：
    unitid,cost0,cost1,acp0,acp1,cpm0,cpm1,ctr0,ctr1,cpa_cpc0,cpa_cpc1,cpa_ocpc0,cpa_ocpc1,cpa_given,cvr0,cvr1
     */

    val dataCPC = data.filter("is_ocpc=0 and ocpc_flag=0")
    val dataOCPC = data.filter("is_ocpc=0 and ocpc_flag=1")

    dataCPC.createOrReplaceTempView("cpc_data")
    val sqlRequest1 =
      s"""
         |SELECT
         |  unitid,
         |  sum(case when isclick=1 then price else 0 end) as cost_cpc,
         |  sum(case when isclick=1 then price else 0 end) * 1.0 / sum(isclick) as acp_cpc,
         |  sum(case when isclick=1 then price else 0 end) * 10.0 / sum(isshow) as cpm_cpc,
         |  sum(isclick) * 1.0 / sum(isshow) as ctr_cpc,
         |  sum(case when isclick=1 then price else 0 end) * 1.0 / sum(iscvr1) as cpa_cpc1,
         |  sum(case when isclick=1 then price else 0 end) * 1.0 / sum(iscvr2) as cpa_cpc2,
         |  sum(iscvr1) * 1.0 / sum(isclick) as cvr_cpc1,
         |  sum(iscvr2) * 1.0 / sum(isclick) as cvr_cpc2
         |FROM
         |  cpc_date
         |GROUP BY unitid
       """.stripMargin
    println(sqlRequest1)
    val dataCPCdetail = spark.sql(sqlRequest1)

    dataOCPC.createOrReplaceTempView("ocpc_data")
    val sqlRequest2 =
      s"""
         |SELECT
         |  unitid,
         |  sum(case when isclick=1 then price else 0 end) as cost_ocpc,
         |  sum(case when isclick=1 then price else 0 end) * 1.0 / sum(isclick) as acp_ocpc,
         |  sum(case when isclick=1 then price else 0 end) * 10.0 / sum(isshow) as cpm_ocpc,
         |  sum(isclick) * 1.0 / sum(isshow) as ctr_ocpc,
         |  sum(case when isclick=1 then price else 0 end) * 1.0 / sum(iscvr1) as cpa_ocpc1,
         |  sum(case when isclick=1 then price else 0 end) * 1.0 / sum(iscvr2) as cpa_ocpc2,
         |  sum(iscvr1) * 1.0 / sum(isclick) as cvr_ocpc1,
         |  sum(iscvr2) * 1.0 / sum(isclick) as cvr_ocpc2
         |FROM
         |  ocpc_date
         |GROUP BY unitid
       """.stripMargin
    println(sqlRequest2)
    val dataOCPCdetail = spark.sql(sqlRequest2)

    // 关联数据表
    val dataDetail = dataCPCdetail
      .join(dataOCPCdetail, Seq("unitid"), "outer")
      .select("unitid", "cost_cpc", "cost_ocpc", "acp_cpc", "acp_ocpc", "cpm_cpc", "cpm_ocpc", "ctr_cpc", "ctr_ocpc", "cpa_cpc1", "cpa_cpc2", "cpa_ocpc1", "cpa_ocpc2", "cvr_cpc1", "cvr_cpc2", "cvr_ocpc1", "cvr_ocpc2")
    dataDetail

  }

  def calculateOcpcSummary(data: DataFrame, date: String, hour: String, spark: SparkSession) = {
    /*
    统计cost，acp，以及cpa等等，分两张表，一张详情表，一张汇总表
    汇总表：
    is_ocpc,cost,acp,cpm,ctr,cpa1,cpa2,cvr1,cvr2
     */
    val base = data.filter("is_ocpc=0")
    base.createOrReplaceTempView("base_data")
    val sqlRequest =
      s"""
         |SELECT
         |  ocpc_flag,
         |  sum(case when isclick=1 then price else 0 end) as cost,
         |  sum(case when isclick=1 then price else 0 end) * 1.0 / sum(isclick) as acp,
         |  sum(case when isclick=1 then price else 0 end) * 10.0 / sum(isshow) as cpm,
         |  sum(isclick) * 1.0 / sum(isshow) as ctr,
         |  sum(case when isclick=1 then price else 0 end) * 1.0 / sum(iscvr1) as cpa1,
         |  sum(case when isclick=1 then price else 0 end) * 1.0 / sum(iscvr2) as cpa2,
         |  sum(iscvr1) * 1.0 / sum(isclick) as cvr1,
         |  sum(iscvr2) * 1.0 / sum(isclick) as cvr2
         |FROM
         |  base_data
         |GROUP BY ocpc_flag
       """.stripMargin
    println(sqlRequest)
    val resultDF = spark.sql(sqlRequest)
    resultDF
  }

}


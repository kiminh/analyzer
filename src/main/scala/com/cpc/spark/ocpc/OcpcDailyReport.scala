package com.cpc.spark.ocpc

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.{Column, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._

object OcpcDailyReport {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("OcpcDailyReport")
      .enableHiveSupport()
      .getOrCreate()

    val date = args(0).toString

    val rawCompleteData = getCompleteRawTable(date, spark)
    getDailyReport(rawCompleteData, date, spark)

  }

  def getCompleteRawTable(date: String, spark: SparkSession): DataFrame = {
    val sqlRequest =
      s"""
         |SELECT
         |    a.*,
         |    b.iscvr
         |FROM
         |    (select
         |        uid,
         |        timestamp,
         |        searchid,
         |        userid,
         |        ext['exp_ctr'].int_value * 1.0 / 1000000 as exp_ctr,
         |        ext['exp_cvr'].int_value * 1.0 / 1000000 as exp_cvr,
         |        isclick,
         |        isshow,
         |        ideaid,
         |        exptags,
         |        price,
         |        ext_int['bid_ocpc'] as bid_ocpc,
         |        ext_int['is_ocpc'] as is_ocpc,
         |        ext_string['ocpc_log'] as ocpc_log,
         |        hour
         |    from
         |        dl_cpc.cpc_union_log
         |    WHERE
         |        `date` = '$date'
         |    and
         |        media_appsid  in ("80000001", "80000002")
         |    and
         |        ext['antispam'].int_value = 0
         |    and adsrc = 1
         |    and adslot_type in (1,2,3)
         |    and round(ext["adclass"].int_value/1000) != 132101  --去掉互动导流
         |    AND ext_int['is_ocpc']=1
         |    and ext_string['ocpc_log'] is not null
         |    and ext_string['ocpc_log'] != '') a
         |left outer join
         |    (
         |        select
         |            searchid,
         |            label2 as iscvr
         |        from dl_cpc.ml_cvr_feature_v1
         |        WHERE `date` = '$date'
         |    ) b on a.searchid = b.searchid;
       """.stripMargin
    println(sqlRequest)

    val rawData = spark.sql(sqlRequest)
    println("####### rawData ######################")
    rawData.show(10)

    rawData.write.mode("overwrite").saveAsTable("test.test_ocpc_complete_raw_table")

    rawData
  }

  def getDailyReport(dataset: DataFrame, date: String, spark: SparkSession): Unit ={
    dataset.createOrReplaceTempView("raw_table")

    val sqlRequest =
      s"""
         |SELECT
         |    ideaid,
         |    AVG(bid_ocpc) as cpa_given,
         |    sum(case WHEN isclick == 1 then price else 0 end)/sum(iscvr) as cpa_real,
         |    sum(CASE WHEN isclick=1 then exp_cvr else 0 end) * 1.0/SUM(isclick) as pcvr,
         |    round(sum(case WHEN isclick == 1 then price else 0 end)*10/sum(isshow),3) as cpm,
         |    round(sum(case WHEN isclick == 1 then price else 0 end)*10/count(distinct uid),3) as arpu,
         |    sum(isclick) * 1.0 / sum(isshow) as ctr,
         |    sum(iscvr) * 1.0 / sum(isclick) as click_cvr,
         |    sum(iscvr) * 1.0 / sum(isshow) as show_cvr,
         |    AVG(price) as price,
         |    COUNT(isshow) as show_cnt,
         |    SUM(isclick) as ctr_cnt,
         |    SUM(iscvr) as cvr_cnt,
         |    '$date' as date
         |FROM
         |    raw_table
         |GROUP BY
         |    ideaid
       """.stripMargin

    println(sqlRequest)

    val resultDF = spark.sql(sqlRequest)

    println("############# resultDF ###############")
    resultDF.show(10)

    resultDF.write.mode("overwrite").saveAsTable("test.ocpc_daily_report")
//    resultDF.write.mode("overwrite").insertInto("dl_cpc.ocpc_daily_report")
  }

}

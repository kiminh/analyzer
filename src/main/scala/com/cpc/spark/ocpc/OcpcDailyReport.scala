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

//    val rawCompleteData = getCompleteRawTable(date, spark)
    getDailyReport(date, spark)

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
         |    ) b on a.searchid = b.searchid
       """.stripMargin
    println(sqlRequest)

    val rawData = spark.sql(sqlRequest)
    println("####### rawData ######################")
    rawData.show(10)

    rawData.write.mode("overwrite").saveAsTable("test.test_ocpc_complete_raw_table")

    rawData
  }

  def getDailyReport(date: String, spark: SparkSession): Unit ={
    // 读取unionlog和cvr历史数据进行统计
    val sqlRequest1 =
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
         |    sum(case WHEN isclick == 1 then price else 0 end) as total_cost,
         |    COUNT(isshow) as show_cnt,
         |    SUM(isclick) as ctr_cnt,
         |    SUM(iscvr) as cvr_cnt
         |FROM
         |    dl_cpc.ocpc_result_unionlog_table_bak
         |WHERE
         |    `date`='$date'
         |GROUP BY
         |    ideaid
       """.stripMargin

    println(sqlRequest1)
    val data1 = spark.sql(sqlRequest1)

    // 读取pb历史数据进行统计
    val sqlRequest2 =
      s"""
         |SELECT
         |    ideaid,
         |    AVG(k_value) avg_k,
         |    STDDEV(k_value) stddev_k,
         |    COUNT(1) as k_cnt
         |FROM
         |    dl_cpc.ocpc_pb_result_table_v2
         |WHERE
         |    `date`='$date'
         |GROUP BY ideaid
       """.stripMargin

    println(sqlRequest2)
    val data2 = spark.sql(sqlRequest2)

    data1.createOrReplaceTempView("statistic_data1")
    data2.createOrReplaceTempView("statistic_data2")

    val sqlRequest =
      s"""
         |SELECT
         |    t1.*,
         |    t2.avg_k,
         |    t2.stddev_k,
         |    t2.k_cnt,
         |    '$date' as date
         |FROM
         |    statistic_data1 as t1
         |LEFT JOIN
         |    statistic_data2 as t2
         |ON
         |    t1.ideaid=t2.ideaid
       """.stripMargin

    println(sqlRequest)
    val resultDF = spark.sql(sqlRequest)


    println("############# resultDF ###############")
    resultDF.show(10)

    resultDF.write.mode("overwrite").saveAsTable("test.ocpc_daily_report")
    resultDF.write.mode("overwrite").insertInto("dl_cpc.ocpc_daily_report")
  }

}

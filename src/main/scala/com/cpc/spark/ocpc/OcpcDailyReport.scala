package com.cpc.spark.ocpc

import com.cpc.spark.udfs.Udfs_wj.udfStringToMap
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
//    getDailyReport(date, spark)
    getDailyReportV2(date, spark)

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
    // 读取基础表
    val sqlRequest0 =
      s"""
         |SELECT
         |  searchid,
         |  ideaid,
         |  exptags,
         |  uid,
         |  bid_ocpc,
         |  ocpc_log,
         |  price,
         |  isshow,
         |  isclick,
         |  iscvr,
         |  exp_cvr,
         |  hour
         |FROM
         |  dl_cpc.ocpc_result_unionlog_table_bak
         |WHERE
         |    `date`='$date'
       """.stripMargin

    println(sqlRequest0)
    val baseData = spark.sql(sqlRequest0).withColumn("ocpc_log_dict", udfStringToMap()(col("ocpc_log")))
    baseData.createOrReplaceTempView("base_table")

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
         |    base_table
         |GROUP BY
         |    ideaid
       """.stripMargin

    println(sqlRequest1)
    val data1 = spark.sql(sqlRequest1)

    // 读取pb历史数据进行统计
    val sqlRequest2 =
      s"""
         |SELECT
         |    searchid,
         |    ideaid,
         |    ocpc_log_dict['kvalue'] as kvalue
         |FROM
         |    base_table
       """.stripMargin

    println(sqlRequest2)
    val kvalueData = spark.sql(sqlRequest2)

    val data2 = kvalueData
      .groupBy("ideaid")
      .agg(
        avg(col("kvalue")).alias("avg_k"),
        stddev(col("kvalue")).alias("stddev_k"),
        count(col("kvalue")).alias("k_cnt"))

    val resultDF = data1
      .join(data2, Seq("ideaid"), "left_outer")
      .select("ideaid", "cpa_given", "cpa_real", "pcvr", "cpm", "arpu", "ctr", "click_cvr", "show_cvr", "price", "total_cost", "show_cnt", "ctr_cnt", "cvr_cnt", "avg_k", "stddev_k", "k_cnt")
      .withColumn("date", lit(date))


    println("############# resultDF ###############")
    resultDF.show(10)

//    resultDF.write.mode("overwrite").saveAsTable("test.ocpc_daily_report")
    resultDF.write.mode("overwrite").insertInto("dl_cpc.ocpc_daily_report")
  }

  def getKcurve(date: String, spark: SparkSession) ={
    val sqlRequest1 =
      s"""
         |SELECT
         |    ideaid,
         |    k_value,
         |    hour
         |FROM
         |    dl_cpc.ocpc_pb_result_table_v2
         |WHERE
         |    `date`='$date'
       """.stripMargin
    println(sqlRequest1)
    val kvalueData = spark.sql(sqlRequest1)

    val sqlRequest2 =
      s"""
         |SELECT
         |    ideaid,
         |    cpa_given,
         |    cpa_history as cpa_real,
         |    ratio as cpa_ratio,
         |    hour
         |FROM
         |    dl_cpc.ocpc_cpa_given_history_ratio
         |WHERE
         |    `date`='$date'
       """.stripMargin
    println(sqlRequest2)
    val ratioData = spark.sql(sqlRequest2)

    val resultData = kvalueData
      .join(ratioData, Seq("ideaid", "hour"))
      .select("ideaid", "k_value", "cpa_given", "cpa_real", "cpa_ratio", "hour")
      .withColumn("date", lit(date))

    resultData.write.mode("overwrite").insertInto("dl_cpc.ocpc_k_curve_daily")

  }

  def getDailyReportV2(date: String, spark: SparkSession) :Unit = {
    /**
      * 重新计算抽取全天的数据日志
      */

    // 抽取基础数据：所有跑ocpc的广告主
    val rawData = spark
      .table("dl_cpc.ocpc_unionlog")
      .where(s"`dt`='$date'")
      .withColumn("bid_ocpc", col("cpa_given"))
    rawData.write.mode("overwrite").saveAsTable("test.ocpc_daily_complete_data")

    val sqlRequest1=
      s"""
         |SELECT
         |  ideaid,
         |  userid,
         |  ocpc_log_dict['conversiongoal'] as conversion_goal
         |FROM
         |  test.ocpc_daily_complete_data
       """.stripMargin

    val ocpcAd = spark.sql(sqlRequest1).distinct()
    ocpcAd.write.mode("overwrite").saveAsTable("test.ocpc_daily_ad_list")

    val sqlRequest2 =
      s"""
         |SELECT
         |    ideaid,
         |    SUM(case when ocpc_log_dict['ocpcstep']=2 then 1 else 0 end) * 1.0 / sum(isclick) as step2_percent
         |FROM
         |    test.ocpc_daily_complete_data
         |WHERE
         |    isclick=1
         |GROUP BY ideaid
       """.stripMargin

    val ocpcStep2 = spark.sql(sqlRequest2)
    ocpcStep2.write.mode("overwrite").saveAsTable("test.ocpc_daily_step2_percent")


    val sqlRequest3 =
      s"""
         |SELECT
         |    ideaid,
         |    SUM(label) as cvr_cnt
         |FROM
         |    dl_cpc.ml_cvr_feature_v2
         |WHERE
         |    `date`='$date'
         |GROUP BY ideaid
       """.stripMargin

    val label3Data = spark.sql(sqlRequest3)
    label3Data.write.mode("overwrite").saveAsTable("test.ocpc_label3_daily_data")


    val sqlRequest4 =
      s"""
         |SELECT
         |    ideaid,
         |    ocpc_log_dict['conversiongoal'] as conversion_goal,
         |    SUM(case when isclick==1 then bid_ocpc else 0 end) * 1.0 / sum(isclick) as cpa_given,
         |    SUM(case when isclick==1 then price else 0 end) as cost,
         |    sum(CASE WHEN isclick=1 then exp_cvr else 0 end) * 1.0/SUM(isclick) as pcvr,
         |    SUM(isshow) as show_cnt,
         |    SUM(isclick) as ctr_cnt,
         |    SUM(iscvr) as cvr_cnt,
         |    sum(case when isclick=1 then ocpc_log_dict['kvalue'] else 0 end) * 1.0 / sum(isclick) as avg_k
         |FROM
         |    test.ocpc_daily_complete_data
         |GROUP BY ideaid, ocpc_log_dict['conversiongoal']
       """.stripMargin

    val label2Data = spark.sql(sqlRequest4)
    label2Data.write.mode("overwrite").saveAsTable("test.ocpc_label2_daily_data")

    val sqlRequest5 =
      s"""
         |SELECT
         |    a.ideaid,
         |    a.userid,
         |    a.conversion_goal,
         |    c.step2_percent,
         |    b.cpa_given,
         |    b.cost * 1.0 / b.cvr_cnt as cpa_real,
         |    b.pcvr,
         |    b.ctr_cnt * 1.0 / b.show_cnt as ctr,
         |    b.cvr_cnt * 1.0 / b.ctr_cnt as click_cvr,
         |    b.cvr_cnt * 1.0 / b.show_cnt as show_cvr,
         |    b.cost * 1.0 / b.ctr_cnt as price,
         |    b.show_cnt,
         |    b.ctr_cnt,
         |    b.cvr_cnt,
         |    b.avg_k,
         |    '$date' as date
         |FROM
         |    (SELECT
         |        ideaid,
         |        userid,
         |        conversion_goal
         |    FROM
         |        test.ocpc_daily_ad_list
         |    WHERE
         |        conversion_goal!=2) as a
         |INNER JOIN
         |    test.ocpc_label2_daily_data as b
         |ON
         |    a.ideaid=b.ideaid
         |AND
         |    a.conversion_goal=b.conversion_goal
         |INNER JOIN
         |    test.ocpc_daily_step2_percent as c
         |ON
         |    a.ideaid=c.ideaid
       """.stripMargin

    val noApiData = spark.sql(sqlRequest5)

    val sqlRequest6 =
      s"""
         |SELECT
         |    a.ideaid,
         |    a.userid,
         |    d.step2_percent,
         |    b.cpa_given,
         |    b.cost * 1.0 / c.cvr_cnt as cpa_real,
         |    b.pcvr,
         |    b.ctr_cnt * 1.0 / b.show_cnt as ctr,
         |    c.cvr_cnt * 1.0 / b.ctr_cnt as click_cvr,
         |    c.cvr_cnt * 1.0 / b.show_cnt as show_cvr,
         |    b.cost * 1.0 / b.ctr_cnt as price,
         |    b.show_cnt,
         |    b.ctr_cnt,
         |    c.cvr_cnt,
         |    b.avg_k,
         |    '$date' as date
         |FROM
         |    (SELECT
         |        ideaid,
         |        userid,
         |        conversion_goal
         |    FROM
         |        test.ocpc_daily_ad_list
         |    WHERE
         |        conversion_goal=2) as a
         |INNER JOIN
         |    test.ocpc_label2_daily_data as b
         |ON
         |    a.ideaid=b.ideaid
         |AND
         |    a.conversion_goal=b.conversion_goal
         |INNER JOIN
         |    test.ocpc_label3_daily_data as c
         |ON
         |    a.ideaid=c.ideaid
         |INNER JOIN
         |    test.ocpc_daily_step2_percent as d
         |ON
         |    a.ideaid=d.ideaid
       """.stripMargin

    val apiData = spark.sql(sqlRequest6)

//    noApiData.write.mode("overwrite").saveAsTable("test.ocpc_check_daily_report_noapi")
//    apiData.write.mode("overwrite").saveAsTable("test.ocpc_check_daily_report_api")

    noApiData.write.mode("overwrite").insertInto("dl_cpc.ocpc_check_daily_report_noapi")
    apiData.write.mode("overwrite").insertInto("dl_cpc.ocpc_check_daily_report_api")


  }

}

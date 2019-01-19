package com.cpc.spark.ocpc

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}


object OcpcHourlyReportV2 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("OcpcHourlyReport")
      .enableHiveSupport()
      .getOrCreate()

    val date = args(0).toString
    val hour = args(1).toString

    getHourlyReport(date, hour, spark)
  }

  def getHourlyReport(date: String, hour: String, spark: SparkSession) = {
    /**
      * 重新计算抽取全天截止当前时间的数据日志
      */

    // 抽取基础数据：所有跑ocpc的广告主
    val rawData = spark
      .table("dl_cpc.ocpc_unionlog")
      .where(s"`dt`='$date' and `hour` <= '$hour'")
      .withColumn("bid_ocpc", col("cpa_given"))
    rawData.write.mode("overwrite").saveAsTable("test.ocpc_hourly_complete_data")

    val sqlRequest1=
      s"""
         |SELECT
         |  ideaid,
         |  userid,
         |  ocpc_log_dict['conversiongoal'] as conversion_goal
         |FROM
         |  test.ocpc_hourly_complete_data
       """.stripMargin

    val ocpcAd = spark.sql(sqlRequest1).distinct()
    ocpcAd.write.mode("overwrite").saveAsTable("test.ocpc_hourly_ad_list")

    val sqlRequest2 =
      s"""
         |SELECT
         |    ideaid,
         |    SUM(case when ocpc_log_dict['ocpcstep']=2 then 1 else 0 end) * 1.0 / sum(isclick) as step2_percent
         |FROM
         |    test.ocpc_hourly_complete_data
         |WHERE
         |    isclick=1
         |GROUP BY ideaid
       """.stripMargin

    val ocpcStep2 = spark.sql(sqlRequest2)
    ocpcStep2.write.mode("overwrite").saveAsTable("test.ocpc_hourly_step2_percent")

    // 增加cvr1和cvr3
    val sqlRequestCvr1 =
      s"""
         |SELECT
         |  a.searchid,
         |  a.ideaid,
         |  b.label2 as label
         |FROM
         |  test.ocpc_hourly_complete_data as a
         |LEFT JOIN
         |  (SELECT
         |    searchid,
         |    label2
         |  FROM
         |    dl_cpc.ml_cvr_feature_v1
         |  WHERE
         |    `date`='$date' and `hour` <= '$hour'
         |  AND
         |    label_type!=12
         |  GROUP BY searchid, label2) as b
         |ON
         |  a.searchid=b.searchid
       """.stripMargin
    println(sqlRequestCvr1)
    val label1DataRaw = spark.sql(sqlRequestCvr1).groupBy("ideaid").agg(sum(col("label")).alias("cvr_cnt"))
    label1DataRaw.write.mode("overwrite").saveAsTable("test.ocpc_label1_hourly_data_raw")


    val sqlRequestCvr3 =
      s"""
         |SELECT
         |  a.searchid,
         |  a.ideaid,
         |  b.label
         |FROM
         |  test.ocpc_hourly_complete_data as a
         |LEFT JOIN
         |  (SELECT
         |    searchid,
         |    1 as label
         |  FROM
         |    dl_cpc.site_form_unionlog
         |  WHERE
         |    `date`='$date' and `hour` <= '$hour'
         |  AND
         |    ideaid>0
         |  GROUP BY searchid) as b
         |ON
         |  a.searchid=b.searchid
       """.stripMargin
    println(sqlRequestCvr3)
    val label3DataRaw = spark.sql(sqlRequestCvr3).groupBy("ideaid").agg(sum(col("label")).alias("cvr_cnt"))
    label3DataRaw.write.mode("overwrite").saveAsTable("test.ocpc_label3_hourly_data_raw")

    val sqlRequest3 =
      s"""
         |SELECT
         |    ideaid,
         |    SUM(label) as cvr_cnt
         |FROM
         |    dl_cpc.ml_cvr_feature_v2
         |WHERE
         |    `date`='$date'
         |AND
         |    `hour`<='$hour'
         |GROUP BY ideaid
       """.stripMargin

    val label2DataRaw = spark.sql(sqlRequest3)
    label2DataRaw.write.mode("overwrite").saveAsTable("test.ocpc_label2_hourly_data_raw")


    val sqlRequest4 =
      s"""
         |SELECT
         |    ideaid,
         |    SUM(case when isclick==1 then bid_ocpc else 0 end) * 1.0 / sum(isclick) as cpa_given,
         |    SUM(case when isclick==1 then price else 0 end) as cost,
         |    SUM(CASE WHEN isclick=1 then exp_cvr else 0 end) * 1.0/SUM(isclick) as pcvr,
         |    SUM(isshow) as show_cnt,
         |    SUM(isclick) as ctr_cnt,
         |    sum(case when isclick=1 then ocpc_log_dict['kvalue'] else 0 end) * 1.0 / sum(isclick) as avg_k,
         |    SUM(case when isclick=1 and `hour`='$hour' then ocpc_log_dict['kvalue'] else 0 end) * 1.0 / sum(case when `hour`='$hour' then isclick else 0 end) as recent_k
         |FROM
         |    test.ocpc_hourly_complete_data
         |GROUP BY ideaid
       """.stripMargin

    val costData = spark.sql(sqlRequest4)
    costData.write.mode("overwrite").saveAsTable("test.ocpc_cost_hourly_data")

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
         |    b.recent_k,
         |    '$date' as date,
         |    '$hour' as hour
         |FROM
         |    (SELECT
         |        ideaid,
         |        userid,
         |        conversion_goal
         |    FROM
         |        test.ocpc_hourly_ad_list
         |    WHERE
         |        conversion_goal!=2) as a
         |INNER JOIN
         |    test.ocpc_label2_hourly_data as b
         |ON
         |    a.ideaid=b.ideaid
         |INNER JOIN
         |    test.ocpc_hourly_step2_percent as c
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
         |    b.recent_k,
         |    '$date' as date,
         |    '$hour' as hour
         |FROM
         |    (SELECT
         |        ideaid,
         |        userid,
         |        conversion_goal
         |    FROM
         |        test.ocpc_hourly_ad_list
         |    WHERE
         |        conversion_goal=2) as a
         |INNER JOIN
         |    test.ocpc_label2_hourly_data as b
         |ON
         |    a.ideaid=b.ideaid
         |INNER JOIN
         |    test.ocpc_label2_hourly_data as c
         |ON
         |    a.ideaid=c.ideaid
         |INNER JOIN
         |    test.ocpc_hourly_step2_percent as d
         |ON
         |    a.ideaid=d.ideaid
       """.stripMargin

    val apiData = spark.sql(sqlRequest6)

    //    noApiData.write.mode("overwrite").saveAsTable("test.ocpc_check_hourly_report_noapi")
    //    apiData.write.mode("overwrite").saveAsTable("test.ocpc_check_hourly_report_api")

    noApiData
      .repartition(10).write.mode("overwrite").insertInto("dl_cpc.ocpc_check_hourly_report_noapi")
    apiData
      .repartition(10).write.mode("overwrite").insertInto("dl_cpc.ocpc_check_hourly_report_api")
  }


}
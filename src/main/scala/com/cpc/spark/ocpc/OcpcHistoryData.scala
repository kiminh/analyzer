package com.cpc.spark.ocpc

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.functions.{col, concat_ws, sum, when}

object OcpcHistoryData {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()

    // calculate time period for historical data
    val end_date = args(0)
    val hour = args(1)

    // read data and set redis configuration
    val sqlRequest =
      s"""
         |SELECT
         |    a.ideaid,
         |    a.userid,
         |    a.adclass,
         |    a.cost,
         |    (case when a.user_cvr_cnt<20 then b.adclass_ctr_cnt else a.user_ctr_cnt end) as ctr_cnt,
         |    (case when a.user_cvr_cnt<20 then b.adclass_cvr_cnt else a.user_cvr_cnt end) as cvr_cnt
         |FROM
         |    test.ocpc_data_userdata a
         |INNER JOIN
         |    test.ocpc_data_adclassdata b
         |ON
         |    a.adclass=b.adclass
       """.stripMargin
    println(sqlRequest)

    val useridAdclassData = spark.sql(sqlRequest)
    useridAdclassData.createOrReplaceTempView("useridTable")

    // step2
    val sql2 =
      s"""
         |SELECT
         |  a.searchid,
         |  a.ideaid,
         |  a.ext['exp_ctr'].int_value as exp_ctr,
         |  a.ext['exp_cvr'].int_value as exp_cvr,
         |  b.cost,
         |  b.ctr_cnt as history_ctr_cnt,
         |  b.cvr_cnt as history_cvr_cnt,
         |  b.cvr_cnt / b.ctr_cnt as history_cvr,
         |  b.cost / b.ctr_cnt as cpa_ctr,
         |  b.cost / b.cvr_cnt as cpa_cvr
         |FROM
         |  (
         |        select *
         |        from dl_cpc.cpc_union_log
         |        where `date`='$end_date' and hour = '$hour'
         |        and isclick is not null
         |        and media_appsid  in ("80000001", "80000002")
         |        and isshow = 1
         |        and ext['antispam'].int_value = 0
         |        and ideaid > 0
         |        and adsrc = 1
         |        and adslot_type in (1,2,3)
         |      ) a
         |INNER JOIN
         |  (
         |    SELECT
         |      ideaid,
         |      cost,
         |      ctr_cnt,
         |      cvr_cnt
         |    FROM
         |      useridTable) b
         |ON
         |  a.ideaid=b.ideaid
       """.stripMargin
    println(sql2)

    val resultDF = spark.sql(sql2)
    resultDF.write.mode("overwrite").saveAsTable("test.historical_union_log_data")


    val sql3 =
      s"""
         |SELECT
         |    t.ideaid,
         |    t.ratio - 1 as exp_value1,
         |    t.roi_ratio - 1 as  exp_value2
         |FROM
         |    (SELECT
         |        ideaid,
         |        exp_cvr * 1.0 / 1000000 as pcvr,
         |        history_cvr as hcvr,
         |        (exp_cvr * 1.0 / (history_cvr * 1000000.0)) as ratio,
         |        1.0 / cpa_cvr as roi,
         |        1.0 / 13.0 as roi_given,
         |        13.0 / cpa_cvr as roi_ratio
         |    FROM
         |        test.historical_union_log_data) t
       """.stripMargin

    val finalDF = spark.sql(sql3)
    finalDF.write.mode("overwrite").saveAsTable("test.ocpc_historical_data")

  }

}
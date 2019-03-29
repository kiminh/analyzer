package com.cpc.spark.ml.recall

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.spark.sql.SparkSession

object update_unitid_ratio {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("update unit ratio")
      .enableHiveSupport()
      .getOrCreate()

    val cal1 = Calendar.getInstance()
    val today = new SimpleDateFormat("yyyy-MM-dd").format(cal1.getTime)
    cal1.add(Calendar.DATE, -1)
    val tardate = new SimpleDateFormat("yyyy-MM-dd").format(cal1.getTime)

    spark.sql(
      s"""
        |select ta.unitid,ta.cvr as cvr_control, tb.cvr as cvr_enable, tb.cvr/coalesce(ta.cvr,0.00000001) as ratio from
        |(select unitid,cvr from dl_cpc.cpc_recall_bscvr_report where date='$tardate' and exp='control'
        |and unitid not in ('exp_unitid', 'all')) ta
        |join
        |(select unitid,cvr from dl_cpc.cpc_recall_bscvr_report where date='$tardate' and exp='enabled0.3'
        |and unitid not in ('exp_unitid', 'all')) tb
        |on ta.unitid=tb.unitid
      """.stripMargin).createOrReplaceTempView("bs_cvr")

    spark.sql(
      s"""
         |select ta.unitid,ta.cvr as cvr_control, tb.cvr as cvr_enable, tb.cvr/coalesce(ta.cvr,0.00000001) as ratio from
         |(select unitid,cvr from dl_cpc.cpc_recall_bsExp_report where date='$tardate' and exp='control'
         |and unitid not in ('exp_unitid', 'all')) ta
         |join
         |(select unitid,cvr from dl_cpc.cpc_recall_bsExp_report where date='$tardate' and exp='enabled0.3'
         |and unitid not in ('exp_unitid', 'all')) tb
         |on ta.unitid=tb.unitid
      """.stripMargin).createOrReplaceTempView("bs_exp")

    spark.sql(
      s"""
         |select * from bs_cvr
         |union
         |select * from bs_exp
      """.stripMargin).createOrReplaceTempView("bs_union")

    spark.sql(
      s"""
         |select ta.unitid, case when tb.ratio>1.2 then ta.ratio*1.1 when tb.ratio<1 then ta.ratio*0.9
         |else ta.ratio end as ratio from
         |(select * from dl_cpc.cpc_recall_bscvr_unitid_ratio where dt='$tardate') ta
         |join bs_union tb
         |on ta.unitid=tb.unitid
      """.stripMargin).repartition(1).createOrReplaceTempView("lastday_ratio")

    spark.sql(
      s"""
         |insert overwrite table dl_cpc.cpc_recall_bscvr_unitid_ratio partition(dt='$today')
         |select * from lastday_ratio
      """.stripMargin)

    spark.sql(
      s"""
         |select tb.unitid, 1 as ratio from
         |(select * from dl_cpc.cpc_recall_bscvr_unitid_ratio where dt='$tardate') ta
         |right join bs_union tb
         |on ta.unitid=tb.unitid where ta.unitid is null
      """.stripMargin).repartition(1).createOrReplaceTempView("new_unitid_ratio")

    spark.sql(
      s"""
         |insert into dl_cpc.cpc_recall_bscvr_unitid_ratio partition(dt='$today')
         |select * from new_unitid_ratio
      """.stripMargin)

  }

}

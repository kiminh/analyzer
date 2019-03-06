package com.cpc.spark.ml.recall

import java.text.SimpleDateFormat
import java.util.{Calendar, Properties}

import org.apache.spark.sql.SparkSession

object unitid_inAndOut {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("unit in and out")
      .enableHiveSupport()
      .getOrCreate()
    val cal1 = Calendar.getInstance()
    val today = new SimpleDateFormat("yyyy-MM-dd").format(cal1.getTime)
    cal1.add(Calendar.DATE, -1)
    val tardate = new SimpleDateFormat("yyyy-MM-dd").format(cal1.getTime)
    cal1.add(Calendar.DATE, -2)
    val startdate = new SimpleDateFormat("yyyy-MM-dd").format(cal1.getTime)

    spark.sql(
      s"""
         |select ta.unitid,'bscvr' as experiment,0 as performance from
         |(select unitid,cpm,cvr from dl_cpc.cpc_recall_bscvr_report where date='$tardate' and exp='control'
         |and unitid not in ('exp_unitid', 'all')) ta
         |join
         |(select unitid,cpm,cvr from dl_cpc.cpc_recall_bscvr_report where date='$tardate' and exp='enabled0.3'
         |and unitid not in ('exp_unitid', 'all')) tb
         |on ta.unitid=tb.unitid
         |where ta.cpm>tb.cpm or ta.cvr*0.95>tb.cvr group by ta.unitid
      """.stripMargin).createOrReplaceTempView("bscvr")

    spark.sql(
      s"""
         |select ta.unitid,'bscvrExp' as experiment,1 as performance from
         |(select unitid,cpm,cvr from dl_cpc.cpc_recall_bsExp_report where date='$tardate' and exp='control'
         |and unitid not in ('exp_unitid', 'all')) ta
         |join
         |(select unitid,cpm,cvr from dl_cpc.cpc_recall_bsExp_report where date='$tardate' and exp='enabled0.3'
         |and unitid not in ('exp_unitid', 'all')) tb
         |on ta.unitid=tb.unitid
         |where ta.cpm<tb.cpm or ta.cvr*0.95<tb.cvr group by ta.unitid
      """.stripMargin).createOrReplaceTempView("bscvrExp")

    spark.sql(
      s"""
         |select * from bscvr
         |union
         |select * from bscvrExp
      """.stripMargin).repartition(1).createOrReplaceTempView("bsUnion")

    spark.sql(
      s"""
         |insert overwrite table dl_cpc.cpc_recall_unitid_performance partition (day='$tardate')
         |select * from bsUnion
      """.stripMargin)

    spark.sql(
      s"""
         |select unitid from dl_cpc.cpc_recall_unitid_performance where day>='$startdate' and experiment='bscvrExp'
         |group by unitid having count(*)>1
      """.stripMargin).repartition(1).createOrReplaceTempView("desired")

    spark.sql(
      s"""
         |insert into dl_cpc.cpc_recall_high_confidence_unitid partition (date='$tardate')
         |select unitid from desired
      """.stripMargin)

    spark.sql(
      s"""
         |select unitid from dl_cpc.cpc_recall_unitid_performance where day>='$startdate' and experiment='bscvr'
         |group by unitid having count(*)>1
      """.stripMargin).repartition(1).createOrReplaceTempView("undesired")
    //剔除15天以内没有活跃的单元
    val jdbcProp = new Properties()
    val jdbcUrl = "jdbc:mysql://rr-2zehhy0xn8833n2u5.mysql.rds.aliyuncs.com"
    jdbcProp.put("user", "adv_live_read")
    jdbcProp.put("password", "seJzIPUc7xU")
    jdbcProp.put("driver", "com.mysql.jdbc.Driver")
    val cal2 = Calendar.getInstance()
    cal2.add(Calendar.DATE, -15)
    val dayCost = new SimpleDateFormat("yyyy-MM-dd").format(cal2.getTime)
    val adv=
      s"""
         |(SELECT unit_id FROM adv.cost where cost>0 and date>='$dayCost' group by unit_id) temp
      """.stripMargin

    spark.read.jdbc(jdbcUrl, adv, jdbcProp).createOrReplaceTempView("cost_unitid")

    val data = spark.sql(
      s"""
         |select * from dl_cpc.cpc_recall_high_confidence_unitid where unitid not in (select unitid from undesired)
         |and unitid in (select unit_id from cost_unitid) and date='$tardate'
      """.stripMargin).repartition(1).cache()
    data.show(10)

    data.createOrReplaceTempView("confidence_unitid")

    spark.sql(
      s"""
         |insert overwrite table dl_cpc.cpc_recall_high_confidence_unitid partition (date='$today')
         |select unitid from confidence_unitid
      """.stripMargin)
  }
}

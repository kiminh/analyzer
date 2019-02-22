package com.cpc.spark.ml.recall

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.spark.sql.SparkSession

object unitid_inAndOut {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("unit in and out")
      .enableHiveSupport()
      .getOrCreate()
    val cal1 = Calendar.getInstance()
    cal1.add(Calendar.DATE, -1)
    val tardate = new SimpleDateFormat("yyyy-MM-dd").format(cal1.getTime)
    cal1.add(Calendar.DATE, -2)
    val startdate = new SimpleDateFormat("yyyy-MM-dd").format(cal1.getTime)

    spark.sql(
      s"""
         |select unitid,'bscvr' as experiment,0 as performance from
         |(select unitid,cpm,cvr from dl_cpc.cpc_recall_bscvr_report where date='$tardate' and exp='control') ta
         |join
         |(select unitid,cpm,cvr from dl_cpc.cpc_recall_bscvr_report where date='$tardate' and exp='enabled0.3') tb
         |on ta.unitid=tb.unitid
         |where ta.cpm>tb.cpm or ta.cvr>tb.cvr group by unitid
      """.stripMargin).createOrReplaceTempView("bscvr")

    spark.sql(
      s"""
         |select unitid,'bscvrExp' as experiment,1 as performance from
         |(select unitid,cpm,cvr from dl_cpc.cpc_recall_bsExp_report where date='$tardate' and exp='control') ta
         |join
         |(select unitid,cpm,cvr from dl_cpc.cpc_recall_bsExp_report where date='$tardate' and exp='enabled0.3') tb
         |on ta.unitid=tb.unitid
         |where ta.cpm>tb.cpm or ta.cvr>tb.cvr group by unitid
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
         |select unitid dl_cpc.cpc_recall_unitid_performance where day>='$startdate' and experiment='bscvrExp'
         |group by unitid having count(*)>2
      """.stripMargin).repartition(1).createOrReplaceTempView("desired")

    spark.sql(
      s"""
         |insert overwrite table dl_cpc.cpc_recall_high_confidence_unitid partition (day='$tardate')
         |select unitid from desired
      """.stripMargin)

    spark.sql(
      s"""
         |select unitid dl_cpc.cpc_recall_unitid_performance where day>='$startdate' and experiment='bscvr'
         |group by unitid having count(*)>1
      """.stripMargin).repartition(1).createOrReplaceTempView("undesired")

    val data = spark.sql(
      s"""
         |select * from dl_cpc.cpc_recall_high_confidence_unitid where unitid not in (select unitid from undesired)
      """.stripMargin).repartition(1).cache()
    data.show(10)

    data.createOrReplaceTempView("confidence_unitid")

    spark.sql(
      s"""
         |insert overwrite table dl_cpc.cpc_recall_high_confidence_unitid partition (day='$tardate')
         |select unitid from confidence_unitid
      """.stripMargin)
  }
}

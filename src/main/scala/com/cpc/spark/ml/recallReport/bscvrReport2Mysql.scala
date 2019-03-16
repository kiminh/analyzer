package com.cpc.spark.ml.recallReport

import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.ml.recall.report_userprofile_effect.{mariaReport2dbProp, mariaReport2dbUrl}
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.{SaveMode, SparkSession}

object bscvrReport2Mysql {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("bscvrReport2Mysql")
      .enableHiveSupport()
      .getOrCreate()
    val cal = Calendar.getInstance()
    cal.add(Calendar.DATE, -1)
    val date = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime)
    val conf = ConfigFactory.load()
    mariaReport2dbUrl = conf.getString("mariadb.report2_write.url")
    mariaReport2dbProp.put("user", conf.getString("mariadb.report2_write.user"))
    mariaReport2dbProp.put("password", conf.getString("mariadb.report2_write.password"))
    mariaReport2dbProp.put("driver", conf.getString("mariadb.report2_write.driver"))

    spark.sql(
      s"""
         |select
         |cast(coalesce(imp,0) as int) as imp,
         |cast(coalesce(click,0) as int) as click,
         |cast(coalesce(iscvr, '0) as int) as iscvr,
         |exp,
         |cast(coalesce(adslot_type,0) as int) as adslot_type,
         |industry,
         |cast(coalesce(ctr,0) as double) as ctr,
         |cast(coalesce(cost,0) as double) as cost,
         |cast(coalesce(cpm,0) as double) as cpm,
         |cast(coalesce(arpu,0) as double) as arpu,
         |cast(coalesce(uv,0) as int) as uv,
         |cast(coalesce(cvr,0) as double) as cvr,
         |unitid as unitid_all,
         |to_date('$date') as date
         |from
         |dl_cpc.cpc_recall_bsCvr_report where date='$date'
      """.stripMargin).
      write.mode(SaveMode.Append).jdbc(mariaReport2dbUrl, "report2.cpc_recall_bscvr_report", mariaReport2dbProp)

    spark.sql(
      s"""
         |select
         |cast(coalesce(imp,0) as int) as imp,
         |cast(coalesce(click,0) as int) as click,
         |cast(coalesce(iscvr, '0) as int) as iscvr,
         |exp,
         |cast(coalesce(adslot_type,0) as int) as adslot_type,
         |industry,
         |cast(coalesce(ctr,0) as double) as ctr,
         |cast(coalesce(cost,0) as double) as cost,
         |cast(coalesce(cpm,0) as double) as cpm,
         |cast(coalesce(arpu,0) as double) as arpu,
         |cast(coalesce(uv,0) as int) as uv,
         |cast(coalesce(cvr,0) as double) as cvr,
         |unitid as unitid_all,
         |to_date('$date') as date
         |from
         |dl_cpc.cpc_recall_bsexp_report where date='$date'
      """.stripMargin).
      write.mode(SaveMode.Append).jdbc(mariaReport2dbUrl, "report2.cpc_recall_bsexp_report", mariaReport2dbProp)
  }
}

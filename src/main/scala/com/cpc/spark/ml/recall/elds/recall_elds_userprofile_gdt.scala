package com.cpc.spark.ml.recall.elds

import java.text.SimpleDateFormat
import java.util.{Calendar, Properties}

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object recall_elds_userprofile_gdt {
  var mariaReportdbUrl = ""
  val mariaReportdbProp = new Properties()
  def main(args: Array[String]): Unit = {
    Logger.getRootLogger.setLevel(Level.WARN)
    val spark = SparkSession.builder().appName("recall_elds_userprofile_gdt").enableHiveSupport().getOrCreate()
    val cal = Calendar.getInstance()
    val today = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime)
    val hour = new SimpleDateFormat("HH").format(cal.getTime)
    cal.add(Calendar.DATE, -1)
    val yesterday = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime)
    //连接mysql的report2
    val jdbcProp = new Properties()
    val jdbcUrl = "jdbc:mysql://rm-2zemny6nzg818jcdn.mysql.rds.aliyuncs.com:3306/report2"
    jdbcProp.put("user", "report")
    jdbcProp.put("password", "report!@#")
    jdbcProp.put("driver", "com.mysql.jdbc.Driver")

    val sql =
      s"""
        |(select * from report2.report_gdt_second_commerce where date = '$yesterday') temp
      """.stripMargin
    spark.read.jdbc(jdbcUrl, sql, jdbcProp).createOrReplaceTempView("temp_gdt")

    spark.sql(
      s"""
         |select ta.uid,tb.* from
         |(select uid,adid_str from dl_cpc.cpc_basedata_union_events where day = '$yesterday'
         |and adsrc=7 and adid_str is not null and isclick>0) ta
         |join (select * from temp_gdt) tb
         |on ta.adid_str = tb.adid_str
       """.stripMargin).repartition(20).createOrReplaceTempView("temp_result")
    spark.sql(
      s"""
         |insert overwrite table dl_cpc.cpc_userprofile_tag_hourly partition(`date`='$today', hour = '$hour', tag='351')
         |select uid,true
         |from temp_result group by uid
      """.stripMargin)


  }

}

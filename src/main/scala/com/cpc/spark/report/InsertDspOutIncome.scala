package com.cpc.spark.report

import java.sql.DriverManager
import java.util.Properties

import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

/**
  * Created on 2018-12-12 16
  */
object InsertDspOutIncome {

  var mariadbUrl = ""
  val mariadbProp = new Properties()
  var day = ""
  var table = ""

  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      System.err.println(
        s"""
           |Usage: day <date>
        """.stripMargin)
      System.exit(1)
    }
    day = args(0).toString
    table = args(1).toString

    Logger.getRootLogger.setLevel(Level.WARN)

    val conf = ConfigFactory.load()
    mariadbUrl = conf.getString("mariadb.union_write.url")
    mariadbProp.put("user", conf.getString("mariadb.union_write.user"))
    mariadbProp.put("password", conf.getString("mariadb.union_write.password"))
    mariadbProp.put("driver", conf.getString("mariadb.union_write.driver"))

    val spark = SparkSession.builder()
      .appName("insert dsp_out_income date " + day)
      .enableHiveSupport()
      .getOrCreate()


    /* 1.外媒dsp结算信息自动化 */
    /*val sql2 =
      s"""
         |SELECT
         |  `date`,
         |  adslotid,
         |  ext_string["dsp_adslotid_by_src_22"] as dsp_adslot_id,
         |  sum(
         |    CASE
         |      WHEN isshow == 1 THEN bid/1000
         |      ELSE 0
         |    END
         |  ) AS dsp_income,
         |  sum(isclick) as dsp_click,
         |  sum(isshow) as dsp_impression
         |FROM
         |  dl_cpc.cpc_union_log
         |WHERE
         |  adsrc = 22
         |  AND isshow = 1
         |  AND `date` = "$day"
         |GROUP BY
         |  `date`,
         |  adslotid,
         |  ext_string["dsp_adslotid_by_src_22"]
       """.stripMargin
    println("sql2: " + sql2)*/

    val sql2 =
      s"""
         |select
         |  b.day as date,
         |  b.adslot_id as adslotid,
         |  a.adslotid as dsp_adslot_id,
         |  sum(
         |    CASE
         |      WHEN b.isshow == 1 THEN b.bid*0.8/1000
         |      ELSE 0
         |    END
         |  ) AS dsp_income,
         |  sum(b.isclick) as dsp_click,
         |  sum(b.isshow) as dsp_impression
         |from dl_cpc.cpc_basedata_search_dsp a
         |join dl_cpc.cpc_basedata_union_events b
         |on a.searchid=b.searchid
         |where a.day="$day" and b.day="$day"
         |  and b.adsrc=22
         |  and b.isshow=1
         |group by
         |  b.day,
         |  b.adslot_id,
         |  a.adslotid
       """.stripMargin
    println("sql2: " + sql2)

    val df = spark.sql(sql2).cache()
    var dspIncomeLog = df.collect()

    for (log <- dspIncomeLog) {
      val dsp_adslot_id = log.getAs[String]("dsp_adslot_id")
      val adslot_id = log.getAs[String]("adslotid")
      val dsp_income = log.getAs[Double]("dsp_income")
      val dsp_click = log.getAs[Long]("dsp_click")
      val dsp_impression = log.getAs[Long]("dsp_impression")
      updateDspIncomeTable(day, dsp_adslot_id, adslot_id, dsp_income, dsp_click, dsp_impression)
    }
    println("~~~~~~write to union.dsp_income successfully")


    /* 2.外媒dsp结算信息自动化，类似于1，少了一个adslot_id维度 */
    var dspOutIncomeLog = df.groupBy("date", "dsp_adslot_id")
      .agg("dsp_income" -> "sum", "dsp_click" -> "sum", "dsp_impression" -> "sum")
      .toDF("date", "dsp_adslot_id", "dsp_income", "dsp_click", "dsp_impression")

    for (log <- dspOutIncomeLog.collect()) {
      val dsp_adslot_id = log.getAs[String]("dsp_adslot_id")
      val dsp_income = log.getAs[Double]("dsp_income")
      val dsp_click = log.getAs[Long]("dsp_click")
      val dsp_impression = log.getAs[Long]("dsp_impression")
      updateDspOutIncomeTable(table, day, dsp_adslot_id, dsp_income, dsp_click, dsp_impression)
    }
    println("~~~~~~write to union.dsp_out_income successfully")

    df.unpersist()
    println("----- done -----")
    spark.stop()
  }

  def updateDspOutIncomeTable(table: String, day: String, dsp_adslot_id: String, dsp_income: Double, dsp_click: Long, dsp_impression: Long): Unit = {
    println("#####: " + table + ", " + day + ", " + dsp_adslot_id + ", " + dsp_income + ", " + dsp_click + ", " + dsp_impression)
    try {
      Class.forName(mariadbProp.getProperty("driver"))
      val conn = DriverManager.getConnection(
        mariadbUrl,
        mariadbProp.getProperty("user"),
        mariadbProp.getProperty("password"))
      val stmt = conn.createStatement()
      val sql =
        s"""
           |update union.dsp_out_income
           |set dsp_income = %s,
           |dsp_click = %s,
           |dsp_impression = %s
           |where `date` = "%s" and dsp_adslot_id = "%s" and ad_src = 22
      """.stripMargin.format(dsp_income, dsp_click, dsp_impression, day, dsp_adslot_id)
      println("sql" + sql);
      stmt.executeUpdate(sql);

    } catch {
      case e: Exception => println("exception caught: " + e)
    }
  }

  def updateDspIncomeTable(day: String, dsp_adslot_id: String, adslot_id: String, dsp_income: Double, dsp_click: Long, dsp_impression: Long): Unit = {
    println("#####: " + "dsp_income, " + day + ", " + dsp_adslot_id + ", " + dsp_income + ", " + dsp_click + ", " + dsp_impression)
    try {
      Class.forName(mariadbProp.getProperty("driver"))
      val conn = DriverManager.getConnection(
        mariadbUrl,
        mariadbProp.getProperty("user"),
        mariadbProp.getProperty("password"))
      val stmt = conn.createStatement()
      val sql =
        s"""
           |update union.dsp_income
           |set media_income = %s,
           |dsp_click = %s,
           |dsp_impression = %s
           |where `date` = "%s"
           |   and dsp_adslot_id = "%s"
           |   and adslot_id = "%s"
           |   and ad_src = 22
      """.stripMargin.format(dsp_income, dsp_click, dsp_impression, day, dsp_adslot_id, adslot_id)
      println("sql" + sql);
      stmt.executeUpdate(sql);

    } catch {
      case e: Exception => println("exception caught: " + e)
    }
  }


}

package com.cpc.spark.small.tool

import java.text.SimpleDateFormat
import java.util.{Calendar, Properties}

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * Created by wanli on 2017/8/10.
  */
object GetReportCpmShow {
  val mariadbUrl = "jdbc:mysql://10.9.164.80:3306/report"

  def main(args: Array[String]): Unit = {
    Logger.getRootLogger().setLevel(Level.WARN)

    val dayBefore = args(0).toInt
    val cal = Calendar.getInstance()
    cal.add(Calendar.DATE, -dayBefore)
    val day = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime)

    val mariadbProp = new Properties()
    mariadbProp.put("user", "report")
    mariadbProp.put("password", "report!@#")
    mariadbProp.put("driver", "org.mariadb.jdbc.Driver")


    val ctx = SparkSession.builder()
      .appName("small tool GetReportCpmShow")
      .enableHiveSupport()
      .getOrCreate()


    val unionLog = ctx.sql(
      """
        |SELECT isshow,bid,ctr,adslot_type FROM dl_cpc.cpc_union_log WHERE `date`="%s" AND isshow=1
      """.stripMargin.format(day)).rdd
      .map {
        x =>
          val bid = x.getInt(1).toFloat
          val ctr = x.getLong(2).toFloat
          val cpm = (bid * ctr / 10000).toInt * 10
          (cpm.toString + x.getInt(3).toLong, (cpm, x.getInt(0).toLong, x.getInt(3)))
      }
      .reduceByKey {
        (a, b) =>
          (a._1, a._2 + b._2, a._3)
      }
      .map {
        x =>
          CpmShow(x._2._1, x._2._2, x._2._3, day)
      }
    //.cache()

    //    unionLog.take(10).foreach(println)
    //    println("unionLog.count()", unionLog.count().toInt)
    //
    ctx.createDataFrame(unionLog)
      .write
      .mode(SaveMode.Append)
      .jdbc(mariadbUrl, "report.report_cpm_show", mariadbProp)
  }

}

case class CpmShow(
                    cpm: Int = 0,
                    impression: Long = 0,
                    adslot_type: Int = 0,
                    date: String = ""
                  )
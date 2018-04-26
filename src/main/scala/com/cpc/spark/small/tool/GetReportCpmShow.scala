package com.cpc.spark.small.tool

import java.sql.DriverManager
import java.text.SimpleDateFormat
import java.util.{Calendar, Properties}

import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}


/**
  * Created by wanli on 2017/8/10.
  */
object GetReportCpmShow {
  var mariadbUrl = ""
  val mariadbProp = new Properties()

  def main(args: Array[String]): Unit = {
    Logger.getRootLogger().setLevel(Level.WARN)

    val dayBefore = args(0).toInt
    val cal = Calendar.getInstance()
    cal.add(Calendar.DATE, -dayBefore)
    val day = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime)

    val conf = ConfigFactory.load()
    mariadbUrl = conf.getString("mariadb.url")

    mariadbProp.put("user", conf.getString("mariadb.user"))
    mariadbProp.put("password", conf.getString("mariadb.password"))
    mariadbProp.put("driver", conf.getString("mariadb.driver"))

    val ctx = SparkSession.builder()
      .appName("small tool GetReportCpmShow %s".format(day))
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

    //unionLog.take(10).foreach(println)
    println("unionLog.count()", unionLog.count())
    clearDataByDay(day)

    ctx.createDataFrame(unionLog)
      .write
      .mode(SaveMode.Append)
      .jdbc(mariadbUrl, "report.report_cpm_show", mariadbProp)
  }

  def clearDataByDay(date: String): Unit = {
    try {
      Class.forName(mariadbProp.getProperty("driver"))
      val conn = DriverManager.getConnection(
        mariadbUrl,
        mariadbProp.getProperty("user"),
        mariadbProp.getProperty("password"))
      val stmt = conn.createStatement()
      val sql =
        """
          |delete from report.report_cpm_show where `date` = "%s"
        """.stripMargin.format(date)
      stmt.executeUpdate(sql);
    } catch {
      case e: Exception => println("exception caught: " + e);
    }
  }
}


case class CpmShow(
                    cpm: Int = 0,
                    impression: Long = 0,
                    adslot_type: Int = 0,
                    date: String = ""
                  )
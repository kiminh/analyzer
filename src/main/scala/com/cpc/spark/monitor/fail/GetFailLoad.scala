package com.cpc.spark.monitor.fail

import java.io.{File, PrintWriter}
import java.sql.DriverManager
import java.text.SimpleDateFormat
import java.util.{Calendar, Properties}

import com.cpc.spark.small.tool.InsertUserCvr.{mariadbProp, mariadbUrl}
import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.io.Source

object GetFailLoad {
    var mariadbUrl = ""
    val mariadbProp = new Properties()

    def main(args: Array[String]): Unit = {

        Logger.getRootLogger.setLevel(Level.WARN)

        val day = args(0).toString
        val hour = args(1).toInt

        val ctx = SparkSession
            .builder()
            .appName("GetFailLoad run ....time %s %s".format(day, hour))
            .enableHiveSupport()
            .getOrCreate()

        println("GetFailLoad run ....time %s %s".format(day, hour))

        val reg = "iclicashsid=[^&]*".r
        val logData = ctx
            .sql(
                """
                  |SELECT field['url'].string_type
                  |FROM gobblin.qukan_report_log_five_minutes
                  |WHERE field['cmd'].string_type = "9027"
                  |AND field['url'].string_type like "%s"
                  |AND field['error_code'].string_type != "0"
                  |AND thedate = "%s"
                  |AND thehour = "%s"
                """.stripMargin.format("%iclica%", day, hour))
            .rdd
            .map {
                x =>
                    val url = reg.replaceAllIn(x.getString(0), "")
                    (url, 1)
            }
            .reduceByKey {
                (a, b) =>
                    a + b
            }
            .cache()

        println("userData count", logData.count())

        var allCount = 0
        val data = logData.collect()
        for (d <- data) {
            println(d.toString())
            allCount += d._2
        }
        println("allCount:", allCount)

        logData
            .map {
                x =>
                    (x._1, x._2, day, hour)
            }
        clearReportHourData(day, hour)
        val userCvrDataFrame = ctx.createDataFrame(logData).toDF("url", "count", "date", "hour")

        userCvrDataFrame
            .write
            .mode(SaveMode.Append)
            .jdbc(mariadbUrl, "report.monitor_fail_load", mariadbProp)
    }

    def clearReportHourData(date: String, hour: Int): Unit = {
        try {
            Class.forName(mariadbProp.getProperty("driver"))
            val conn = DriverManager.getConnection(
                mariadbUrl,
                mariadbProp.getProperty("user"),
                mariadbProp.getProperty("password"))
            val stmt = conn.createStatement()
            val sql =
                """
                  |delete from report.monitor_fail_load where `date` = "%s" and `hour` = %d
                """.stripMargin.format(date, hour)
            stmt.executeUpdate(sql);
        } catch {
            case e: Exception => println("exception caught: " + e);
        }
    }
}

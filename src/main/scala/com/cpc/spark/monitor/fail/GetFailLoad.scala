package com.cpc.spark.monitor.fail

import java.sql.DriverManager
import java.util.Properties
import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

object GetFailLoad {
    var mariadbUrl = ""
    val mariadbProp = new Properties()
    val reg_and_iclicashsid = "&iclicashsid=[^&]*".r
    val reg_first_iclicashsid = "[?]iclicashsid=[^&]*&".r
    val reg_only_iclicashsid = "[?]iclicashsid=[^&]*".r

    def main(args: Array[String]): Unit = {
        Logger.getRootLogger.setLevel(Level.WARN)

        val day = args(0).toString
        val hour = args(1).toString
        val allTable = args(2).toString

        val conf = ConfigFactory.load()
        mariadbUrl = conf.getString("mariadb.url")
        mariadbProp.put("user", conf.getString("mariadb.user"))
        mariadbProp.put("password", conf.getString("mariadb.password"))
        mariadbProp.put("driver", conf.getString("mariadb.driver"))

        val ctx = SparkSession
            .builder()
            .appName("GetFailLoad run ....time %s %s".format(day, hour))
            .enableHiveSupport()
            .getOrCreate()

        println("GetFailLoad run ....time %s %s".format(day, hour))

        var tableName = "gobblin.qukan_report_log_five_minutes"
        if (allTable.equals("1")) {
            tableName = "dw_qukan.qukan_report_log"
        }

        val logData = ctx
            .sql(
                """
                  |SELECT field['url'].string_type
                  |FROM %s
                  |WHERE field['cmd'].string_type = "9027"
                  |AND field['url'].string_type like "%s"
                  |AND field['error_code'].string_type != "0"
                  |AND thedate = "%s"
                  |AND thehour = "%s"
                """.stripMargin.format(tableName, "%iclica%", day, hour))
            .rdd
            .map {
                x =>
                    var url = reg_and_iclicashsid.replaceAllIn(x.getString(0), "")
                    url = reg_first_iclicashsid.replaceAllIn(url, "?")
                    url = reg_only_iclicashsid.replaceAllIn(url, "")
                    (url, 1)
            }
            .reduceByKey {
                (a, b) =>
                    a + b
            }
            .cache()

        println("logData count", logData.count())

        var allCount = 0
        val data = logData.collect()
        for (d <- data) {
            //            println(d.toString())
            allCount += d._2
        }
        println("allCount:", allCount)

        val sqlData = logData
            .map {
                x =>
                    (x._1, x._2, day, hour.toInt)
            }
            .cache()
        clearReportHourData(day, hour.toInt)
        val userCvrDataFrame = ctx.createDataFrame(sqlData).toDF("url", "count", "date", "hour")

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

package com.cpc.spark.monitor.fail

import java.io.{File, PrintWriter}
import java.util.Properties

import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.io.Source

object GetFailLoad {
    var mariadbUrl = ""
    val mariadbProp = new Properties()

    def main(args: Array[String]): Unit = {

        Logger.getRootLogger.setLevel(Level.WARN)

        val path = args(0).toString
        if (path.length == 0) {
            System.err.println(
                s"""
                   |Usage: CtrModel <Last timestramp configure file:string>
        """.stripMargin)
            System.exit(1)
        }
        println("last timestamp configure file path:", path)

        val file = Source.fromFile(path)
        var lastTimestamp = ""
        try {
            lastTimestamp = file.getLines.next()
        } finally {
            file.close
        }
        println("last timestamp:", lastTimestamp)
        if (lastTimestamp.length < 13) {
            println("Last timestramp configure file is valid, content:" + lastTimestamp)
            System.exit(1)
        }

        val ctx = SparkSession
            .builder()
            .appName("GetFailLoad run ....lastTimestamp %s".format(lastTimestamp))
            .enableHiveSupport()
            .getOrCreate()

        println("GetFailLoad run ....lastTimestamp %s".format(lastTimestamp))

        val reg = "[?&](iclicashsid|iclitype|icliqqid|icliwxid)=[^&]*".r
        val logData = ctx
            .sql(
                """
                  |SELECT log_timestamp, field['url'].string_type, thedate, thehour
                  |FROM gobblin.qukan_report_log_five_minutes
                  |WHERE field['cmd'].string_type = "9027"
                  |AND field['url'].string_type like "%s"
                  |AND field['error_code'].string_type != "0"
                  |AND log_timestamp>%s
                """.stripMargin.format("%iclica%", lastTimestamp))
            .rdd
            .map {
                x =>
                    val timestamp = x.getLong(0)
                    val url = reg.replaceAllIn(x.getString(1), "")
                    val theDate = x.getString(2)
                    val theHour = x.getString(3)
                    (timestamp, url, theDate, theHour)
            }
            .cache()
        println("userData count", logData.count())

        var newestTimestramp = 0L
        val data = logData.collect()
        for (d <- data) {
            if (newestTimestramp < d._1) {
                newestTimestramp = d._1
            }
            println(d.toString())
        }

        println("newestTimestramp:", newestTimestramp)

        if (lastTimestamp.toLong < newestTimestramp) {
            val writer = new PrintWriter(new File(path))
            try {
                writer.print(newestTimestramp)
            } finally {
                writer.close()
            }
        }
    }
}

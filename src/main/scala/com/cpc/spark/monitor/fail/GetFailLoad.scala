package com.cpc.spark.monitor.fail

import java.util.Properties

import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

class GetFailLoad {
    var mariadbUrl = ""
    val mariadbProp = new Properties()

    def main(args: Array[String]): Unit = {

        Logger.getRootLogger.setLevel(Level.WARN)

        val lastTimestamp = args(0).toString

        val conf = ConfigFactory.load()
        mariadbUrl = conf.getString("mariadb.url")
        mariadbProp.put("user", conf.getString("mariadb.user"))
        mariadbProp.put("password", conf.getString("mariadb.password"))
        mariadbProp.put("driver", conf.getString("mariadb.driver"))

        val ctx = SparkSession
            .builder()
            .appName("GetFailLoad run ....lastTimestamp %s".format(lastTimestamp))
            .enableHiveSupport()
            .getOrCreate()

        println("GetFailLoad run ....lastTimestamp %s".format(lastTimestamp))

        val logData = ctx
            .sql(
                """
                  |SELECT log_timestamp, field['url'].string_type
                  |FROM gobblin.qukan_report_log_five_minutes
                  |WHERE field['cmd'].string_type = "9027"
                  |AND field['url'].string_type like "%iclica%"
                  |AND field['error_code'].string_type != "0"
                  |AND log_timestamp>%s
                """.stripMargin.format(lastTimestamp))
            .rdd
            .map {
                x =>
                    val timestamp = x.getString(0)
                    val url = x.getString(1)
                    (timestamp, url)
            }
            .cache()
        println("userData count", logData.count())
    }
}

package com.cpc.spark.log.report

import java.text.SimpleDateFormat
import java.util.{Calendar, Properties}

import com.cpc.spark.log.parser.UnionLog
import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * Created by Roy on 2017/4/26.
  */
object GetDayUv2 {

  var mariadbUrl = ""

  val mariadbProp = new Properties()

  def main(args: Array[String]): Unit = {
    Logger.getRootLogger.setLevel(Level.WARN)
    if (args.length < 2) {
      System.err.println(
        s"""
           |Usage: GetDayUv <hive_table> <day_before>
           |
        """.stripMargin)
      System.exit(1)
    }
    val table = args(0)
    val dayBefore = args(1).toInt
    val cal = Calendar.getInstance()
    cal.add(Calendar.DATE, -dayBefore)
    val date = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime)

    val conf = ConfigFactory.load()
    mariadbUrl = conf.getString("mariadb.url")
    mariadbProp.put("user", conf.getString("mariadb.user"))
    mariadbProp.put("password",conf.getString("mariadb.password"))
    mariadbProp.put("driver", conf.getString("mariadb.driver"))

    val ctx = SparkSession.builder()
      .appName("cpc get uv report from %s %s".format(table, date))
      .enableHiveSupport()
      .getOrCreate()
    import ctx.implicits._

    val unionLog = ctx.sql(
      s"""
         |select * from dl_cpc.%s where `date` = "%s" and adslotid > 0 and isshow = 1
       """.stripMargin.format(table, date))
//      .as[UnionLog]
      .rdd

    val uvData = unionLog
      .map {
        x =>
          val r = MediaUvReport(
            media_id = x.getAs[String]("media_appsid").toInt,
            adslot_id = x.getAs[String]("adslotid").toInt,
            adslot_type = x.getAs[Int]("adslot_type"),
            uniq_user = 1,
            date = x.getAs[String]("date")
          )
          ("%d-%d-%s".format(r.media_id, r.adslot_id, x.getAs[String]("uid")), r)
      }
      .reduceByKey((x, y) => x)
      .map {
        x =>
          val r = x._2
          ("%d-%d".format(r.media_id, r.adslot_id), r)
      }
      .reduceByKey((x, y) => x.sum(y))
      .map(_._2)

    ctx.createDataFrame(uvData)
      .write
      .mode(SaveMode.Append)
      .jdbc(mariadbUrl, "report.report_media_uv_daily", mariadbProp)

    println("done", uvData.count())
    ctx.stop()
  }
}


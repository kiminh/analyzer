package com.cpc.spark.log.report

import java.sql.DriverManager
import java.util.Properties

import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * count Conversion number.
  * data source: dl_cpc.ml_cvr_feature_v1、dl_cpc.ml_cvr_feature_v2
  * output table(mysql): report.convertion_hourly
  */
object ConvertionHourly {
  var mariadbUrl = ""
  val mariadbProp = new Properties()

  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      System.err.println(
        s"""
           |Usage: ConvertionHourly <date> <hour>
           |
        """.stripMargin)
      System.exit(1)
    }
    Logger.getRootLogger.setLevel(Level.WARN)

    val date = args(0)
    val hour = args(1)
    println("*******************")
    println("date:" + date+", hour:" + hour)

    val conf = ConfigFactory.load()
    mariadbUrl = conf.getString("mariadb.report2_write.url")
    mariadbProp.put("user", conf.getString("mariadb.report2_write.user"))
    mariadbProp.put("password", conf.getString("mariadb.report2_write.password"))
    mariadbProp.put("driver", conf.getString("mariadb.report2_write.driver"))

    val spark = SparkSession.builder()
      .appName("cpc get trace hour report from %s/%s".format(date, hour))
      .enableHiveSupport()
      .getOrCreate()

    val sqlv1 =
      s"""
         |select userid as user_id
         |      ,planid as plan_id
         |      ,unitid as unit_id
         |      ,ideaid as isea_id
         |      ,date
         |      ,hour
         |      ,label_type
         |      ,adclass
         |      ,media_appsid as media_id
         |      ,adslot_type
         |      ,sum(label2) as cvr_num
         |from dl_cpc.ml_cvr_feature_v1
         |where `date`='$date' and hour='$hour' and label_type not in (8,9,10,11)
         |group by
         |       userid
         |      ,planid
         |      ,unitid
         |      ,ideaid
         |      ,date
         |      ,hour
         |      ,label_type
         |      ,adclass
         |      ,media_appsid
         |      ,adslot_type
       """.stripMargin

    val sqlv2 =
      s"""
         |select userid as user_id
         |      ,planid as plan_id
         |      ,unitid as unit_id
         |      ,ideaid as idea_id
         |      ,date
         |      ,hour
         |      ,13 as label_type
         |      ,adclass
         |      ,media_appsid as media_id
         |      ,adslot_type
         |      ,sum(label) as cvr_num
         |from dl_cpc.ml_cvr_feature_v2
         |where `date`='$date' and hour='$hour'
         |group by
         |       userid
         |      ,planid
         |      ,unitid
         |      ,ideaid
         |      ,date
         |      ,hour
         |      ,adclass
         |      ,media_appsid
         |      ,adslot_type
       """.stripMargin

    val cvr = (spark.sql(sqlv1)).union(spark.sql(sqlv2))
      .rdd
      .map {
        r =>
          AdvConversionHourly(r.getAs[Int]("user_id"),
            r.getAs[Int]("plan_id"),
            r.getAs[Int]("unit_id"),
            r.getAs[Int]("idea_id"),
            r.getAs[String]("date"),
            r.getAs[String]("hour"),
            r.getAs[Int]("label_type"),
            r.getAs[Int]("adclass"),
            r.getAs[Int]("media_id"),
            r.getAs[Int]("adslot_type"),
            r.getAs[Int]("cvr_num")
          )
      }

    writeConversionHourlyTable(spark, cvr, date, hour)
    println("ConvertionHourly done")
  }

  def writeConversionHourlyTable(spark: SparkSession, result: RDD[AdvConversionHourly], date: String, hour: String): Unit = {
    clearReportHourData("convertion_hourly", date, hour)
    spark.createDataFrame(result)
      .write
      .mode(SaveMode.Append)
      .jdbc(mariadbUrl, "report2.convertion_hourly", mariadbProp)
  }

  def clearReportHourData(tbl: String, date: String, hour: String): Unit = {
    try {
      println("~~~~~clear ConvertionHourly Data~~~~~")
      Class.forName(mariadbProp.getProperty("driver"))
      val conn = DriverManager.getConnection(
        mariadbUrl,
        mariadbProp.getProperty("user"),
        mariadbProp.getProperty("password"))
      val stmt = conn.createStatement()
      val sql =
        """
          |delete from report2.%s where `date` = "%s" and `hour` = %d
        """.stripMargin.format(tbl, date, hour.toInt)
      stmt.executeUpdate(sql);
    } catch {
      case e: Exception => println("exception caught: " + e);
    }
  }

  case class AdvConversionHourly(
                                  user_id: Int = 0,
                                  plan_id: Int = 0,
                                  unit_id: Int = 0,
                                  idea_id: Int = 0,
                                  date: String = "",
                                  hour: String = "",
                                  label_type: Int = 0,
                                  adclass: Int = 0,
                                  media_id: Int = 0,
                                  adslot_type: Int = 0,
                                  cvr_num: Int = 0
                                )

}

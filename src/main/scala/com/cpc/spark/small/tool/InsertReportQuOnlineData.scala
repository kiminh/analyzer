package com.cpc.spark.small.tool

import java.sql.DriverManager
import java.util.Properties

import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * Created by wanli on 2018/8/27.
  */
object InsertReportQuOnlineData {

  var mariaReport2dbUrl = ""
  val mariaReport2dbProp = new Properties()

  case class Info(
                   date: String = "",
                   create_time: String = "",
                   daily_active_five: Long = 0,
                   daily_active_day: Long = 0,
                   read_pv_five: Long = 0,
                   read_pv_day: Long = 0,
                   read_uv_five: Long = 0,
                   read_uv_day: Long = 0,
                   info_flow_show_pv_five: Long = 0,
                   info_flow_show_pv_day: Long = 0,
                   info_flow_show_uv_five: Long = 0,
                   info_flow_show_uv_day: Long = 0
                 )

  def main(args: Array[String]): Unit = {

    Logger.getRootLogger.setLevel(Level.WARN)

    val argDay = args(0).toString

    val conf = ConfigFactory.load()
    mariaReport2dbUrl = conf.getString("mariadb.report2_write.url")
    mariaReport2dbProp.put("user", conf.getString("mariadb.report2_write.user"))
    mariaReport2dbProp.put("password", conf.getString("mariadb.report2_write.password"))
    mariaReport2dbProp.put("driver", conf.getString("mariadb.report2_write.driver"))

    val ctx = SparkSession
      .builder()
      .config("spark.debug.maxToStringFields", "2000")
      .appName("InsertReportQuOnlineData is run day is %s".format(argDay))
      .enableHiveSupport()
      .getOrCreate()


    println("InsertReportQuOnlineData is run day is %s".format(argDay))


    val allData = ctx.sql(
      """
        |SELECT day,from_unixtime(day_time_five,'yyyy-MM-dd HH:mm:ss'),
        |    daily_active_five,daily_active_day,
        |    read_pv_five,read_pv_day,read_uv_five,read_uv_day,
        |    info_flow_show_pv_five,info_flow_show_pv_day,info_flow_show_uv_five,info_flow_show_uv_day
        |FROM horizon.export_online_data
        |WHERE day="%s"
      """.stripMargin.format(argDay))
      .rdd
      .map {
        x =>
          val date = if(x.get(0) != null) x.get(0).toString else ""

          val create_time = if(x.get(1) != null) x.get(1).toString else ""

          val daily_active_five = if(x.get(2) != null) x.getLong(2) else 0.toLong//x.getLong(2)
        val daily_active_day = if(x.get(3) != null) x.getLong(3) else 0.toLong//x.getLong(3)

          val read_pv_five = if(x.get(4) != null) x.getLong(4) else 0.toLong//x.getLong(4)
        val read_pv_day = if(x.get(5) != null) x.getLong(5) else 0.toLong//x.getLong(5)
        val read_uv_five = if(x.get(6) != null) x.getLong(6) else 0.toLong//x.getLong(6)
        val read_uv_day = if(x.get(7) != null) x.getLong(7) else 0.toLong//x.getLong(7)

          val info_flow_show_pv_five = if(x.get(8) != null) x.getLong(8) else 0.toLong//x.getLong(8)
        val info_flow_show_pv_day = if(x.get(9) != null) x.getLong(9) else 0.toLong//x.getLong(9)
        val info_flow_show_uv_five = if(x.get(10) != null) x.getLong(10) else 0.toLong//x.getLong(10)
        val info_flow_show_uv_day = if(x.get(11) != null) x.getLong(11) else 0.toLong//x.getLong(11)
          Info(date, create_time, daily_active_five, daily_active_day, read_pv_five, read_pv_day, read_uv_five, read_uv_day,
            info_flow_show_pv_five, info_flow_show_pv_day, info_flow_show_uv_five, info_flow_show_uv_day)
      }
      .repartition(10)

    val insertDataFrame = ctx.createDataFrame(allData).toDF("date", "create_time", "daily_active_five", "daily_active_day",
      "read_pv_five", "read_pv_day", "read_uv_five", "read_uv_day",
      "info_flow_show_pv_five", "info_flow_show_pv_day", "info_flow_show_uv_five", "info_flow_show_uv_day")

    //insertDataFrame.show(10)

    println("insertDataFrame count is", insertDataFrame.count())

    clearReportX(argDay)

    insertDataFrame
      .write
      .mode(SaveMode.Append)
      .jdbc(mariaReport2dbUrl, "report2.report_qu_online_data", mariaReport2dbProp)
    println("report2 over!")
    //--------------------------------------
  }


  def clearReportX(date: String): Unit = {
    try {
      Class.forName(mariaReport2dbProp.getProperty("driver"))
      val conn = DriverManager.getConnection(
        mariaReport2dbUrl,
        mariaReport2dbProp.getProperty("user"),
        mariaReport2dbProp.getProperty("password"))
      val stmt = conn.createStatement()
      val sql =
        """
          |delete from report2.report_qu_online_data where `date` = "%s"
        """.stripMargin.format(date)
      stmt.executeUpdate(sql);
    } catch {
      case e: Exception => println("exception caught: " + e);
    }
  }
}

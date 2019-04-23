package com.cpc.spark.small.tool

import java.sql.DriverManager
import java.util.Properties

import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * Created by wanli on 2017/11/14.
  */
object InsertReportUserBid {

  var mariaReport2dbUrl = ""
  val mariaReport2dbProp = new Properties()

  case class Info(
                   userid: Int = 0,
                   adslot_type: Int = 0,
                   sum_cvr_real_bid: Long = 0,
                   cvr_type: String = "",
                   sum_bid: Long = 0,
                   cost: Long = 0,
                   isfill: Long = 0,
                   isshow: Long = 0,
                   isclick: Long = 0,
                   date: String = "",
                   hour: Int = 0
                 )


  def main(args: Array[String]): Unit = {
    Logger.getRootLogger.setLevel(Level.WARN)
    val argDay = args(0).toString
    val argHour = args(1).toString

    val conf = ConfigFactory.load()
    mariaReport2dbUrl = conf.getString("mariadb.report2_write.url")
    mariaReport2dbProp.put("user", conf.getString("mariadb.report2_write.user"))
    mariaReport2dbProp.put("password", conf.getString("mariadb.report2_write.password"))
    mariaReport2dbProp.put("driver", conf.getString("mariadb.report2_write.driver"))

    val ctx = SparkSession
      .builder()
      .config("spark.debug.maxToStringFields", "2000")
      .appName("InsertReportUserBid is run day is %s %s".format(argDay,argHour))
      .enableHiveSupport()
      .getOrCreate()

    println("InsertReportUserBid is run day is %s %s".format(argDay,argHour))

    val allData = ctx
      .sql(
        """
          |SELECT userid,adslot_type,
          |    SUM(
          |        CASE
          |    	   WHEN user_cvr_threshold = 200 THEN cvr_real_bid
          |    	   WHEN user_cvr_threshold >0 THEN 0
          |    	   ELSE 0
          |        END) as sum_cvr_real_bid,
          |	CASE
          |	   WHEN user_cvr_threshold = 200 THEN "cvr2"
          |	   WHEN user_cvr_threshold >0 THEN "cvr1"
          |	   ELSE "nocvr"
          |	END cvr_type,
          |	SUM(bid) sum_bid,
          |	SUM(
          |    	CASE
          |    	   WHEN isclick = 1 THEN price
          |    	   ELSE 0
          |    	END) cost,
          | SUM(isfill) fill,
          |	SUM(isshow) imp,
          |	SUM(isclick) clk
          |FROM dl_cpc.cpc_basedata_union_events
          |WHERE day='%s' AND hour="%s"
          |	AND media_appsid in ('80000001','80000002','80000006','800000062','80000064','80000066','80000141')
          |	AND ideaid>0 AND userid>0
          |	AND adsrc=1 AND charge_type=1
          |GROUP BY userid,adslot_type,
          |         CASE
          |           WHEN user_cvr_threshold=200 THEN "cvr2"
          |           WHEN user_cvr_threshold>0 THEN "cvr1"
          |           ELSE "nocvr"
          |       END
        """.stripMargin.format(argDay, argHour))
      .rdd
      .map {
        x =>
          val userid = x.getAs[Int](0)
          val adslot_type = x.getAs[Int](1)
          val sum_cvr_real_bid = x.getAs[Long](2)
          val cvr_type = x.getAs[String](3)
          val sum_bid = x.getAs[Long](4)
          val cost = x.getAs[Long](5)
          val isfill = x.getAs[Long](6)
          val isshow = x.getAs[Long](7)
          val isclick = x.getAs[Long](8)
          val date = argDay
          val hour = argHour.toInt
          Info(userid, adslot_type, sum_cvr_real_bid, cvr_type, sum_bid, cost, isfill, isshow, isclick, date, hour)
      }
      .repartition(50)

    println("allData count", allData.count())

    val insertDataFrame = ctx.createDataFrame(allData)
      .toDF("user_id", "adslot_type", "sum_cvr_real_bid", "cvr_type", "sum_bid", "cost", "served_request", "impression",
        "click", "date", "hour")

    insertDataFrame.show(10)


    //report2
    clearReportUserBid(argDay,argHour.toInt)

    insertDataFrame
      .write
      .mode(SaveMode.Append)
      .jdbc(mariaReport2dbUrl, "report2.report_user_bid_hourly", mariaReport2dbProp)
    println("report2 over!")

  }

  def clearReportUserBid(date: String,hour:Int): Unit = {
    try {
      Class.forName(mariaReport2dbProp.getProperty("driver"))
      val conn = DriverManager.getConnection(
        mariaReport2dbUrl,
        mariaReport2dbProp.getProperty("user"),
        mariaReport2dbProp.getProperty("password"))
      val stmt = conn.createStatement()
      val sql =
        """
          |delete from report2.report_user_bid_hourly where `date` = "%s" AND hour = %d
        """.stripMargin.format(date,hour)
      stmt.executeUpdate(sql);
    } catch {
      case e: Exception => println("exception caught: " + e);
    }
  }

  //  def clearReportSiteBuildingTargetByAmateur(date: String): Unit = {
  //    try {
  //      Class.forName(mariaAmateurdbProp.getProperty("driver"))
  //      val conn = DriverManager.getConnection(
  //        mariaAmateurdbUrl,
  //        mariaAmateurdbProp.getProperty("user"),
  //        mariaAmateurdbProp.getProperty("password"))
  //      val stmt = conn.createStatement()
  //      val sql =
  //        """
  //          |delete from report.report_site_building_target where `date` = "%s"
  //        """.stripMargin.format(date)
  //      stmt.executeUpdate(sql);
  //    } catch {
  //      case e: Exception => println("exception caught: " + e);
  //    }
  //  }
}

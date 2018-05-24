package com.cpc.spark.small.tool

import java.sql.DriverManager
import java.util.Properties

import com.cpc.spark.small.tool.InsertReportSiteBuildingTarget.{mariaReportdbProp, mariaReportdbUrl}
import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * Created by wanli on 2018/5/8.
  */
object InsertReportDspIdea {

  var mariaReportdbUrl = ""
  val mariaReportdbProp = new Properties()

  case class info(
                   src: Int = 0,
                   adid_str: String = "",
                   ad_title: String = "",
                   ad_desc: String = "",
                   ad_img_urls: String = "",
                   ad_click_url: String = "",
                   isshow: Long = 0,
                   isclick: Long = 0,
                   date: String = ""
                 )

  def main(args: Array[String]): Unit = {
    Logger.getRootLogger.setLevel(Level.WARN)
    val argDay = args(0).toString

    val conf = ConfigFactory.load()
    mariaReportdbUrl = conf.getString("mariadb.url")
    mariaReportdbProp.put("user", conf.getString("mariadb.user"))
    mariaReportdbProp.put("password", conf.getString("mariadb.password"))
    mariaReportdbProp.put("driver", conf.getString("mariadb.driver"))

    val ctx = SparkSession
      .builder()
      .config("spark.debug.maxToStringFields", "2000")
      .appName("InsertReportDspIdea is run day is %s".format(argDay))
      .enableHiveSupport()
      .getOrCreate()

    println("InsertReportDspIdea is run day is %s".format(argDay))

    val unionData = ctx.sql(
      """
        |SELECT cul.ext["adid_str"].string_value,cul.ext["ad_title"].string_value,
        |cul.ext["ad_desc"].string_value,cul.ext["ad_img_urls"].string_value,
        |cul.ext["ad_click_url"].string_value,isshow,isclick,cul.adsrc
        |FROM dl_cpc.cpc_union_log cul
        |WHERE cul.`date`="%s" AND cul.ext["adid_str"].string_value != "" AND cul.adsrc>1
        |AND (cul.isclick+cul.isshow)>0
        |""".stripMargin.format(argDay))
      .rdd
      .map {
        x =>
          val adid_str = x.getString(0)
          val ad_title = x.getString(1)
          val ad_desc = x.getString(2)
          val ad_img_urls = x.getString(3)
          val ad_click_url = x.getString(4)
          val isshow = x.getInt(5).toLong
          val isclick = x.getInt(6).toLong
          val src = x.get(7).toString.toInt
          info(src, adid_str, ad_title, ad_desc, ad_img_urls, ad_click_url, isshow, isclick)
          ((src, adid_str), info(src, adid_str, ad_title, ad_desc, ad_img_urls, ad_click_url, isshow, isclick, argDay))
      }
      .reduceByKey {
        (a, b) =>
          val src = a.src
          val adid_str = a.adid_str
          val ad_title = if (a.ad_title.length > 0) a.ad_title else b.ad_title
          val ad_desc = if (a.ad_desc.length > 0) a.ad_desc else b.ad_desc
          val ad_img_urls = if (a.ad_img_urls.length > 0) a.ad_img_urls else b.ad_img_urls
          val ad_click_url = if (a.ad_click_url.length > 0) a.ad_click_url else b.ad_click_url
          val isshow = a.isshow + b.isshow
          val isclick = a.isclick + b.isclick
          info(src, adid_str, ad_title, ad_desc, ad_img_urls, ad_click_url, isshow, isclick, a.date)
      }
      .map {
        x =>
          x._2
      }
      .filter {
        x =>
          x.ad_click_url.length > 0 && x.isshow >= 100
      }
      .cache()

    println("unionData count is", unionData.count())


    val insertDataFrame = ctx.createDataFrame(unionData)
      .toDF("dsp_src", "adid_str", "ad_title", "ad_desc", "ad_img_urls", "ad_click_url", "impression", "click", "date")

    insertDataFrame.show(20)

    clearReportDspIdea(argDay)

    insertDataFrame
      .write
      .mode(SaveMode.Append)
      .jdbc(mariaReportdbUrl, "report.report_dsp_idea", mariaReportdbProp)

    ///////////////////////////////////
    //////////////////////////////////
    /////////////////////////////////
  }

  def clearReportDspIdea(date: String): Unit = {
    try {
      Class.forName(mariaReportdbProp.getProperty("driver"))
      val conn = DriverManager.getConnection(
        mariaReportdbUrl,
        mariaReportdbProp.getProperty("user"),
        mariaReportdbProp.getProperty("password"))
      val stmt = conn.createStatement()
      val sql =
        """
          |delete from report.report_dsp_idea where `date` = "%s"
        """.stripMargin.format(date)
      stmt.executeUpdate(sql);
    } catch {
      case e: Exception => println("exception caught: " + e);
    }
  }
}

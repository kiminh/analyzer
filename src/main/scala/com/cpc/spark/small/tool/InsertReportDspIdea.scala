package com.cpc.spark.small.tool

import java.sql.DriverManager
import java.util.Properties

import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * Created by wanli on 2018/5/8.
  */
object InsertReportDspIdea {

  case class info(
                   src: Int = 0,
                   adid_str: String = "",
                   ad_title: String = "",
                   ad_desc: String = "",
                   ad_img_urls: String = "",
                   ad_click_url: String = "",
                   isshow: Long = 0,
                   isclick: Long = 0,
                   adslotid: Int = 0,
                   date: String = ""
                 )

  var mariaReport2dbUrl = ""
  val mariaReport2dbProp = new Properties()

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
      .appName("InsertReportDspIdea is run day is %s".format(argDay))
      .enableHiveSupport()
      .getOrCreate()

    println("InsertReportDspIdea is run day is %s".format(argDay))

    val unionData = ctx.sql(
      """
        |SELECT adid_str,ad_title,
        |ext_string['ad_desc'], ext_string['ad_img_urls'],
        |ad_click_url,isshow,isclick,cul.adsrc,cul.adslot_id
        |FROM dl_cpc.cpc_basedata_union_events cul
        |WHERE cul.day="%s" AND adid_str != "" AND cul.adsrc>1
        |AND cul.isshow>0
        |""".stripMargin.format(argDay))
      .rdd
      .map {
        x =>
          val adid_str = x.getAs[String](0)
          val ad_title = x.getAs[String](1)
          val ad_desc = x.getAs[String](2)
          val ad_img_urls = x.getAs[String](3)
          val ad_click_url = x.getAs[String](4)
          val isshow = x.getAs[Int](5).toLong
          val isclick = x.getAs[Int](6).toLong
          val src = x.getAs[Int](7)
          val adslotid = x.getAs[String](8).toInt
          info(src, adid_str, ad_title, ad_desc, ad_img_urls, ad_click_url, isshow, isclick)
          ((src, adid_str,adslotid), info(src, adid_str, ad_title, ad_desc, ad_img_urls, ad_click_url, isshow, isclick, adslotid, argDay))
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
          val adslotid = if (a.adslotid > 0) a.adslotid else b.adslotid
          info(src, adid_str, ad_title, ad_desc, ad_img_urls, ad_click_url, isshow, isclick, adslotid, a.date)
      }
      .map {
        x =>
          x._2
      }
      .filter {
        x =>
          x.ad_click_url.length > 0 && x.isshow >= 100 && x.adslotid>0
      }
      .repartition(50)
      .cache()

    //println("unionData count is", unionData.count())

    val insertDataFrame = ctx.createDataFrame(unionData)
      .toDF("dsp_src", "adid_str", "ad_title", "ad_desc", "ad_img_urls", "ad_click_url", "impression", "click","adslot_id","date")

    insertDataFrame.show(20)

    clearReportDspIdea(argDay)

    insertDataFrame
      .write
      .mode(SaveMode.Append)
      .jdbc(mariaReport2dbUrl, "report2.report_dsp_adslot_idea", mariaReport2dbProp)

    ///////////////////////////////////
    //////////////////////////////////
    /////////////////////////////////
  }

  def clearReportDspIdea(date: String): Unit = {
    try {
      Class.forName(mariaReport2dbProp.getProperty("driver"))
      val conn = DriverManager.getConnection(
        mariaReport2dbUrl,
        mariaReport2dbProp.getProperty("user"),
        mariaReport2dbProp.getProperty("password"))
      val stmt = conn.createStatement()
      val sql =
        """
          |delete from report2.report_dsp_adslot_idea where `date` = "%s"
        """.stripMargin.format(date)
      stmt.executeUpdate(sql);
    } catch {
      case e: Exception => println("exception caught: " + e);
    }
  }
}

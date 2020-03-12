package com.cpc.spark.small.tool

import java.sql.DriverManager
import java.util.Properties

import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.util.matching.Regex

/**
  * Created by wanli on 2018/5/8.
  * Modify by xuyang on 2020/3/12
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
      .config("spark.driver.maxResultSize","2g")
      .appName("InsertReportDspIdea is run day is %s".format(argDay))
      .enableHiveSupport()
      .getOrCreate()

    println("InsertReportDspIdea is run day is %s".format(argDay))

    ctx.udf.register("myFilter",
      (str:String) => {
        //        val charPattern: Regex="""[^x00-xff]|[_a-zA-Z0-9]+""".r   //过滤出标题的中英文和数字  排除非法字符
        val charPattern: Regex="""[_a-zA-Z0-9]|[\u4e00-\u9fa5]|[\u3002|\uff1f|\uff01|\uff0c|\u3001|\uff1b|\uff1a|\u201c|\u201d|\u2018|\u2019|\uff08|\uff09|\u300a|\u300b|\u3008|\u3009|\u3010|\u3011|\u300e|\u300f|\u300c|\u300d|\ufe43|\ufe44|\u3014|\u3015|\u2026|\u2014|\uff5e|\ufe4f|\uffe5]+""".r
        val result = charPattern.findAllMatchIn(str).mkString

        result
      }
    )

    val unionData = ctx.sql(
      s"""
         |SELECT adid_str,
         |       myFilter(ad_title),
         |       bd.ad_desc,
         |       bd.ad_img_urls,
         |       ad_click_url,
         |       isshow,
         |       isclick,
         |       cul.adsrc,
         |       cul.adslot_id
         |FROM (select searchid,
         |             adid_str,
         |             ad_title,
         |             ad_click_url,
         |             isshow,
         |             isclick,
         |             adsrc,
         |             adslot_id
         |      from dl_cpc.cpc_basedata_union_events
         |      WHERE day = '${argDay}' AND adid_str != "" AND adsrc > 1  AND isshow > 0
         |    ) cul left join
         |     (select searchid,
         |             ext_string['ad_desc']     ad_desc,
         |             ext_string['ad_img_urls'] ad_img_urls
         |      from dl_cpc.cpc_bd_adx_blob
         |      where day = '${argDay}'
         |      ) bd
         |     on cul.searchid = bd.searchid
         |""".stripMargin)
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
      .repartition(50)
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

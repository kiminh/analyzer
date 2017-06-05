package com.cpc.spark.log.report

import java.sql.DriverManager
import java.text.SimpleDateFormat
import java.util.{Calendar, Properties}

import com.cpc.spark.log.parser.UnionLog
import com.cpc.spark.qukan.parser.HdfsParser
import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}
/**
  * Created by zhaogang on 2017/5/24.
  */
object GetTagReport {

  val mariadbUrl = "jdbc:mysql://10.9.180.16:3306/report"
  val mariadbProp = new Properties()

  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      System.err.println(
        s"""
           |Usage: GetTagReport <day_before>
           |
        """.stripMargin)
      System.exit(1)
    }
    mariadbProp.put("user", "report")
    mariadbProp.put("password", "report!@#")
    mariadbProp.put("driver", "org.mariadb.jdbc.Driver")
    Logger.getRootLogger.setLevel(Level.WARN)

    val dayBefore = args(0).toInt
    val cal = Calendar.getInstance()
    cal.add(Calendar.DATE, -dayBefore)
    val day = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime)
    val conf = ConfigFactory.load()
    val allowedPkgs = conf.getStringList("userprofile.allowed_pkgs")
    val pkgTags = conf.getConfig("userprofile.pkg_tags")
    val ctx = SparkSession.builder()
      .appName("cpc get tag report [%s]".format(day))
      .enableHiveSupport()
      .getOrCreate()

    val aiPath = "/gobblin/source/lechuan/qukan/extend_report/%s".format(day)
    val aiRdd = ctx.read.orc(aiPath).rdd
      .map(HdfsParser.parseInstallApp(_, x => allowedPkgs.contains(x), pkgTags))
      .filter(x => x != null && x.devid.length > 0).cache()

    import ctx.implicits._
    val unionLog = ctx.sql(
      s"""
         |select * from dl_cpc.cpc_union_log where `date` = "%s" and uid != "" and interests !=""
       """.stripMargin.format(day))
      .as[UnionLog]
      .rdd.distinct()

    val tagData1 = aiRdd.map{
      profile =>
        var data = List[(Int,String)]()
        profile.uis.foreach(
          row =>
            data = data :+ (row.tag, profile.devid)
        )
        data
    }.flatMap(x => x).distinct().map{
      case(x, y) => (x, 1)
    }.reduceByKey((x,y) => x +y).map{
      case (x,count) =>
        TagReport(x, day, count)
    }
    val tagData2 =  unionLog.map{
      log =>
        var data = List[(Int, Int, Int, String, String)]()
        log.interests.split(",").foreach(
          row =>
            try {
              val Array(tag , score) = row.trim.split("=", 2)
              data = data :+ (tag.toInt, log.isclick, log.isshow,log.searchid, log.uid)
            } catch {
                case e: Exception => null
            }
        )
        data
    }.filter(x => x != null).flatMap(x => x).map{
      case (tag, isclick, isshow, searchid, uid) =>
        (searchid,(tag, isclick, isshow, uid))
    }.cache()

    val tagUvData = tagData2.filter(x => x._2._4 != "").map{
      case (searchid,(tag, isclick, isshow, uid)) =>
        ((tag, uid), 1)
    }.reduceByKey((x,y) => x).map{
      case ((tag, uid), count) =>
        (tag, 1)
    }.reduceByKey((x, y) => x + y).map{
      case (tag, uv) =>
        TagReport(tag, day, 0, uv)
    }

    val tagPvData = tagData2.map{
      case (searchid, (tag, isclick, isshow, uid)) =>
        (tag, (isclick, isshow))
    }.reduceByKey{
      case (x,y) =>
        (x._1 + y._1, x._2 + y._2)
    }.map{
      case (tag, (isclick, isshow)) =>
       TagReport(tag, day, 0, 0, isshow, isclick)
    }

    val mergeData = tagData1.union(tagUvData)
    val mergeData2 = mergeData.union(tagPvData)
    val tagData3 = mergeData2.map(tag => (tag.tag, tag)).reduceByKey{
      case (x, y) =>
        var log = x
        if(y.device_num > 0){
          log = log.copy(
            device_num = y.device_num
          )
        }
        if(y.uv > 0){
          log = log.copy(
            uv = y.uv
          )
        }
        if(y.pv > 0){
          log = log.copy(
            pv = y.pv
          )
        }
        if(y.click > 0){
          log = log.copy(
            click = y.click
          )
        }
        log
    }.map(x => x._2)
    println("*********tagData**********")
    tagData3.collect().foreach(println)
    clearReportHourData("report_tag", day)
    ctx.createDataFrame(tagData3)
      .write
      .mode(SaveMode.Append)
      .jdbc(mariadbUrl, "report.report_tag", mariadbProp)
    ctx.stop()
  }
  def clearReportHourData(tbl: String, date: String): Unit = {
    try {
      Class.forName(mariadbProp.getProperty("driver"));
      val conn = DriverManager.getConnection(
        mariadbUrl,
        mariadbProp.getProperty("user"),
        mariadbProp.getProperty("password"));
      val stmt = conn.createStatement();
      val sql =
        """
          |delete from report.%s where `date` = "%s"
        """.stripMargin.format(tbl, date);
      stmt.executeUpdate(sql);
    } catch {
      case e: Exception => println("exception caught: " + e);
    }
  }
}

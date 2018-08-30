package com.cpc.spark.ml.ctrmodel.hourly


import com.cpc.spark.log.parser.TraceLog
import com.cpc.spark.ml.common.Utils
import org.apache.spark.rdd.RDD
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Row, SaveMode, SparkSession}

import scala.collection.mutable.Map

/**
  * Created by roydong on 15/12/2017.
  */
object SaveFeatures {

  Logger.getRootLogger.setLevel(Level.WARN)

  private var version = "v1"
  private var versionV2 = "v2"


  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      System.err.println(
        s"""
           |Usage: SaveFeatures <date=string> <hour=string>
           |
        """.stripMargin
      )
      System.exit(1)
    }
    val date = args(0)
    val hour = args(1)

    val spark = SparkSession.builder()
      .appName("Save features from UnionLog [%s/%s/%s]".format(version, date, hour))
      .enableHiveSupport()
      .getOrCreate()

    saveDataFromLog(spark, date, hour)
    //saveCvrData(spark, date, hour, version)
    saveCvrData(spark, date, hour, versionV2)
    println("SaveFeatures_done")
  }

  def saveDataFromLog(spark: SparkSession, date: String, hour: String): Unit = {
    val stmt =
      """
        |select searchid,isclick as label,sex,age,os,isp,network,
        |       city,media_appsid,ext['phone_level'].int_value as phone_level,`timestamp`,adtype,
        |       planid,unitid,ideaid,ext['adclass'].int_value as adclass,adslotid,
        |       adslot_type,ext['pagenum'].int_value as pagenum,ext['bookid'].string_value as bookid,
        |       ext['brand_title'].string_value as brand_title,
        |       ext['user_req_ad_num'].int_value as user_req_ad_num,
        |       ext['user_req_num'].int_value as user_req_num,uid,
        |       ext['click_count'].int_value as user_click_num,
        |       ext['click_unit_count'].int_value as user_click_unit_num,
        |       ext['long_click_count'].int_value as user_long_click_count
        |from dl_cpc.cpc_union_log where `date` = "%s" and `hour` = "%s" and isshow = 1
        |and ext['antispam'].int_value = 0 and ideaid > 0 and unitid > 0
        |
      """.stripMargin.format(date, hour)

    println(stmt)
    val rows = spark.sql(stmt)
    println("num", rows.count())
    rows.write
      .mode(SaveMode.Overwrite)
      .parquet("/user/cpc/lrmodel/ctrdata_%s/%s/%s".format(version, date, hour))
    spark.sql(
      """
        |ALTER TABLE dl_cpc.ml_ctr_feature_v1 add if not exists PARTITION(`date` = "%s", `hour` = "%s")
        | LOCATION  '/user/cpc/lrmodel/ctrdata_v1/%s/%s'
      """.stripMargin.format(date, hour, date, hour))

    val ulog = rows.rdd.cache()
    //int dict
    saveIntIds(spark, ulog.map(_.getAs[String]("media_appsid").toInt), "mediaid", date)
    saveIntIds(spark, ulog.map(_.getAs[Int]("planid")), "planid", date)
    saveIntIds(spark, ulog.map(_.getAs[Int]("unitid")), "unitid", date)
    saveIntIds(spark, ulog.map(_.getAs[Int]("ideaid")), "ideaid", date)
    saveIntIds(spark, ulog.map(_.getAs[String]("adslotid").toInt), "slotid", date)
    saveIntIds(spark, ulog.map(_.getAs[Int]("city")), "cityid", date)
    saveIntIds(spark, ulog.map(_.getAs[Int]("adclass")), "adclass", date)

    //string dict
    saveStrIds(spark, ulog.map(_.getAs[String]("brand_title")), "brand", date)
    ulog.unpersist()

  }

  def saveStrIds(spark: SparkSession, ids: RDD[String], name: String, date: String): Unit = {
    import spark.implicits._
    val path = "/user/cpc/lrmodel/feature_ids_%s/%s/%s".format(version, name, date)

    var daily: RDD[String] = ids
    try {
      val old = spark.read.parquet(path).rdd.map(x => x.getString(0))
      daily = daily.union(old)
    } catch {
      case e: Exception =>
    }

    daily = daily.distinct().filter(_.length > 0)
    val num = daily.count()
    daily.sortBy(x => x)
      .toDF()
      .write
      .mode(SaveMode.Overwrite)
      .parquet(path)
    println(path, num)
  }

  def saveIntIds(spark: SparkSession, ids: RDD[Int], name: String, date: String): Unit = {
    import spark.implicits._
    val path = "/user/cpc/lrmodel/feature_ids_%s/%s/%s".format(version, name, date)

    var daily: RDD[Int] = ids
    try {
      val old = spark.read.parquet(path).rdd.map(x => x.getInt(0))
      daily = daily.union(old)
    } catch {
      case e: Exception =>
    }

    daily = daily.distinct().filter(_ > 0)
    val num = daily.count()
    daily.sortBy(x => x)
      .toDF()
      .write
      .mode(SaveMode.Overwrite)
      .parquet(path)
    println(path, num)
  }

  def saveCvrData(spark: SparkSession, date: String, hour: String, version: String): Unit = {
    import spark.implicits._
    val cvrlog = spark.sql(
      s"""
         |select * from dl_cpc.cpc_union_trace_log where `date` = "%s" and hour = "%s"
        """.stripMargin.format(date, hour))
      //      .as[TraceLog]
      .rdd
      .map {
        x =>
          (x.getAs[String]("searchid"), Seq(x))
      }
      .reduceByKey(_ ++ _)
      .map {
        x =>
          val convert = Utils.cvrPositiveV(x._2, version)

          //存储active行为数据
          var active_map: Map[String, Int] = Map()
          //active1,active2,active3,active4,active5,active6,disactive,active_auto,active_auto_download,active_auto_submit,active_wx,active_third
          x._2.foreach(
            x => {
              val trace_type = x.getAs[String]("trace_type")
              val trace_op1 = x.getAs[String]("trace_op1")

              trace_type match {
                case s if (s == "active1" || s == "active2" || s == "active3" || s == "active4" || s == "active5"
                  || s == "active6" || s == "disactive" || s == "active_href")
                => active_map += (s -> 1)
                case _ =>
              }

              //增加下载激活字段,trace_op1=="REPORT_DOWNLOAD_PKGADDED"(包含apkdown和lpdown下载安装), 则installed记为1，否则为0
              if (trace_op1 == "REPORT_DOWNLOAD_PKGADDED") {
                active_map += ("installed" -> 1)
              }

            }
          )

          (x._1, convert, active_map.getOrElse("active1", 0), active_map.getOrElse("active2", 0), active_map.getOrElse("active3", 0),
            active_map.getOrElse("active4", 0), active_map.getOrElse("active5", 0), active_map.getOrElse("active6", 0),
            active_map.getOrElse("disactive", 0), active_map.getOrElse("active_href", 0), active_map.getOrElse("installed", 0))
      }
      .toDF("searchid", "label", "active1", "active2", "active3", "active4", "active5", "active6", "disactive", "active_href", "installed")

    println("cvr log", cvrlog.count(), cvrlog.filter(r => r.getInt(1) > 0).count())

    val sqlStmt =
      """
        |select searchid,sex,age,os,isp,network,
        |       city,media_appsid,ext['phone_level'].int_value as phone_level,`timestamp`,adtype,
        |       planid,unitid,ideaid,ext['adclass'].int_value as adclass,adslotid,
        |       adslot_type,ext['pagenum'].int_value as pagenum,ext['bookid'].string_value as bookid,
        |       ext['brand_title'].string_value as brand_title,
        |       ext['user_req_ad_num'].int_value as user_req_ad_num,
        |       ext['user_req_num'].int_value as user_req_num,uid,
        |       ext['click_count'].int_value as user_click_num,
        |       ext['click_unit_count'].int_value as user_click_unit_num,
        |       ext['long_click_count'].int_value as user_long_click_count
        |from dl_cpc.cpc_union_log where `date` = "%s" and `hour` = "%s" and isclick = 1
        |and ext['antispam'].int_value = 0 and ideaid > 0 and unitid > 0
        |
      """.stripMargin.format(date, hour)
    println(sqlStmt)
    val clicklog = spark.sql(sqlStmt)
    println("click log", clicklog.count())

    clicklog.join(cvrlog, Seq("searchid"))
      .write
      .mode(SaveMode.Overwrite)
      .parquet("/user/cpc/lrmodel/cvrdata_%s/%s/%s".format(version, date, hour))
    spark.sql(
      """
        |ALTER TABLE dl_cpc.ml_cvr_feature_v1 add if not exists PARTITION(`date` = "%s", `hour` = "%s")
        | LOCATION  '/user/cpc/lrmodel/cvrdata_v2/%s/%s'
      """.stripMargin.format(date, hour, date, hour))
  }
}



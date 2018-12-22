package com.cpc.spark.ml.ctrmodel.hourly


import java.text.SimpleDateFormat
import java.util.{Calendar, Properties}

import com.cpc.spark.log.parser.TraceLog
import com.cpc.spark.ml.common.Utils
import com.typesafe.config.ConfigFactory
import org.apache.spark.rdd.RDD
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Row, SaveMode, SparkSession}

import scala.collection.mutable.{ListBuffer, Map}
import scala.sys.process._

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
    val yesterday = args(2)
    println("day: " + date, " yesterday: " + yesterday)

    val spark = SparkSession.builder()
      .appName("Save features from UnionLog [%s/%s/%s]".format(version, date, hour))
      .enableHiveSupport()
      .getOrCreate()

    //saveDataFromLog(spark, date, hour)
    //saveCvrData(spark, date, hour, version)  //第一版 cvr  deprecated
    //saveCvrDataV2(spark, date, hour, yesterday, versionV2) //第二版cvr
    saveCvrDataV3(spark, date, hour, yesterday, versionV2) //第二版cvr
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
    rows.repartition(10)
      .write
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
      .repartition(1)
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
      .repartition(1)
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

              //REPORT_USER_STAYINWX：用户点击落地页里的加微信链接跳转到微信然后10秒内没有回来,表示已经转化，REPORT_USER_STAYINWX记为1，否则为0
              if (trace_op1 == "REPORT_USER_STAYINWX") {
                active_map += ("report_user_stayinwx" -> 1)
              }
            }
          )

          (x._1, convert, active_map.getOrElse("active1", 0), active_map.getOrElse("active2", 0), active_map.getOrElse("active3", 0),
            active_map.getOrElse("active4", 0), active_map.getOrElse("active5", 0), active_map.getOrElse("active6", 0),
            active_map.getOrElse("disactive", 0), active_map.getOrElse("active_href", 0), active_map.getOrElse("installed", 0),
            active_map.getOrElse("report_user_stayinwx", 0))
      }
      .toDF("searchid", "label", "active1", "active2", "active3", "active4", "active5", "active6", "disactive", "active_href", "installed", "report_user_stayinwx")

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

  def saveCvrDataV2(spark: SparkSession, date: String, hour: String, yesterday: String, version: String): Unit = {
    import spark.implicits._

    val cal = Calendar.getInstance()
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    cal.set(date.substring(0, 4).toInt, date.substring(5, 7).toInt - 1, date.substring(8, 10).toInt, hour.toInt, 0, 0)
    cal.add(Calendar.HOUR, -1)
    val fDate = dateFormat.format(cal.getTime)
    val before1hour = fDate.substring(11, 13)

    //激励下载转化  取有点击的
    val motivateRDD = spark.sql(
      s"""
         |select   b.trace_type as flag1
         |        ,b.trace_op1 as flag2
         |        ,a.searchid
         |        ,a.ideaid
         |        ,b.trace_type
         |        ,b.trace_op1
         |from (select * from dl_cpc.cpc_motivation_log
         |        where `date` = "%s" and `hour` = "%s" and searchid is not null and searchid != "" and isclick = 1) a
         |    left join (select id from bdm.cpc_userid_test_dim where day='%s') t2
         |        on a.userid = t2.id
         |    left join
         |        (select *
         |            from dl_cpc.logparsed_cpc_trace_minute
         |            where `thedate` = "%s" and `thehour` = "%s"
         |         ) b
         |    on a.searchid=b.searchid and a.ideaid=b.opt['ideaid']
         | where t2.id is null
        """.stripMargin.format(date, hour, yesterday, date, hour))
      .rdd
      .map {
        x =>
          ((x.getAs[String]("searchid"), x.getAs[Int]("ideaid")), Seq(x))
      }
      .reduceByKey(_ ++ _)
      .map { x =>
        val (convert, label_type) = Utils.cvrPositiveV3(x._2, version)
        (x._1._1, x._1._2, convert)
      }.toDF("searchid", "ideaid", "label")
    println("motivate: " + motivateRDD.count())

    motivateRDD
      .repartition(1)
      .write
      .mode(SaveMode.Overwrite)
      .parquet("/user/cpc/lrmodel/cvrdata_motivate/%s/%s".format(date, hour))
    spark.sql(
      """
        |ALTER TABLE dl_cpc.ml_cvr_feature_motivate add if not exists PARTITION(`date` = "%s", `hour` = "%s")
        | LOCATION  '/user/cpc/lrmodel/cvrdata_motivate/%s/%s'
      """.stripMargin.format(date, hour, date, hour))


    //用户Api回传数据(如已经安装但未激活) cvr计算
    //    val userApiBackRDD = spark.sql(
    //      s"""
    //         |select   b.trace_type as flag1
    //         |        ,a.searchid
    //         |        ,a.uid
    //         |        ,a.userid
    //         |        ,a.ideaid
    //         |        ,b.trace_type
    //         |from (select * from dl_cpc.cpc_union_log
    //         |        where `date` = "%s" and `hour` = "%s" and searchid is not null and searchid != "") a
    //         |    left join (select id from bdm.cpc_userid_test_dim where day='%s') t2
    //         |        on a.userid = t2.id
    //         |    left join
    //         |        (select *
    //         |            from dl_cpc.cpc_union_trace_log
    //         |            where `date` = "%s" and `hour` = "%s"
    //         |         ) b
    //         |    on a.searchid=b.searchid
    //         | where t2.id is null
    //       """.stripMargin.format(date, hour, yesterday, date, hour))
    val sql =
    s"""
       |select tr.trace_type as flag1
       |      ,tr.searchid
       |      ,un.userid
       |      ,un.uid
       |      ,un.ideaid
       |      ,un.date
       |      ,un.hour
       |      ,un.adclass
       |      ,un.media_appsid
       |      ,un.planid
       |      ,un.unitid
       |      ,tr.trace_type
       |from (select searchid, userid, uid, planid, unitid, ideaid, adslot_type, isclick, ext['adclass'].int_value as adclass, media_appsid, planid, unitid, date, hour
       |from dl_cpc.cpc_user_api_callback_union_log where %s) as un
       |join dl_cpc.logparsed_cpc_trace_minute as tr on tr.searchid = un.searchid
       |left join (select id from bdm.cpc_userid_test_dim where day='%s') t2 on un.userid = t2.id
       |where  tr.`thedate` = "%s" and tr.`thehour` = "%s" and un.isclick = 1 and un.adslot_type <> 7 and t2.id is null
       """.stripMargin.format(get3DaysBefore(date, hour), yesterday, date, hour)
    println("sql: " + sql)

    //没有api回传标记，直接上报到trace
    val sql2 =
      s"""
         |select tr.trace_type as flag1
         |      ,tr.searchid
         |      ,un.userid
         |      ,un.uid
         |      ,un.ideaid
         |      ,un.date
         |      ,un.hour
         |      ,un.adclass
         |      ,un.media_appsid
         |      ,un.planid
         |      ,un.unitid
         |      ,tr.trace_type
         |from (select a.searchid, a.userid, a.uid ,a.planid ,a.unitid ,a.ideaid, ext['adclass'].int_value as adclass, media_appsid, planid, unitid, a.date, a.hour from dl_cpc.cpc_union_log a
         |where a.`date`="%s" and a.hour>="%s" and a.hour<="%s" and a.ext_int['is_api_callback'] = 0 and a.adslot_type <> 7 and a.isclick = 1) as un
         |join dl_cpc.logparsed_cpc_trace_minute as tr on tr.searchid = un.searchid
         |left join (select id from bdm.cpc_userid_test_dim where day='%s') t2 on un.userid = t2.id
         |where  tr.`thedate` = "%s" and tr.`thehour` = "%s" and t2.id is null
       """.stripMargin.format(date, before1hour, hour, yesterday, date, hour)
    println("sql2: " + sql2)

    //应用商城api转化
    val sql_moti =
      s"""
         |select tr.trace_type as flag1
         |      ,tr.searchid
         |      ,un.userid
         |      ,un.uid
         |      ,un.ideaid
         |      ,un.date
         |      ,un.hour
         |      ,un.adclass
         |      ,un.media_appsid
         |      ,un.planid
         |      ,un.unitid
         |      ,tr.trace_type
         |from (
         |      select searchid
         |            ,opt['ideaid'] as ideaid
         |            ,trace_type
         |      from dl_cpc.logparsed_cpc_trace_minute
         |      where `thedate` = "%s" and `thehour` = "%s" and trace_type = 'active_third'
         |   ) as tr
         |join
         |   (  select searchid, userid, "" as uid, planid, unitid, ideaid, adclass, media_appsid, date, hour
         |      from dl_cpc.cpc_motivation_log
         |      where %s and isclick = 1
         |   ) as un
         |on tr.searchid = un.searchid and tr.ideaid = un.ideaid
         |left join (select id from bdm.cpc_userid_test_dim where day='%s') t2 on un.userid = t2.id
         |where t2.id is null
       """.stripMargin.format(date, hour, get3DaysBefore(date, hour), yesterday)
    println("sql_moti: " + sql_moti)

    //    val userApiBackRDD = spark.sql(
    //            """
    //              |select tr.trace_type as flag1
    //              |      ,tr.searchid
    //              |      ,un.userid
    //              |      ,un.uid
    //              |      ,un.ideaid
    //              |      ,un.date
    //              |      ,un.hour
    //              |      ,tr.trace_type
    //              |from dl_cpc.logparsed_cpc_trace_minute as tr
    //              |left join
    //              |(select searchid, userid, planid, uid, ideaid, adslot_type, isclick, date, hour from dl_cpc.cpc_user_api_callback_union_log where %s) as un on tr.searchid = un.searchid
    //              |left join (select id from bdm.cpc_userid_test_dim where day='%s') t2 on un.userid = t2.id
    //              |where  tr.`thedate` = "%s" and tr.`thehour` = "%s" and un.isclick = 1 and un.adslot_type <> 7 and t2.id is null
    //            """.stripMargin.format(get3DaysBefore(date, hour), yesterday, date, hour))

    val userApiBackRDD = (spark.sql(sql)).union(spark.sql(sql2)).union(spark.sql(sql_moti))
      .rdd
      .map {
        x =>
          (x.getAs[String]("searchid"), Seq(x))
      }
      .map {
        x =>
          var active_third = 0
          var uid = ""
          var userid = 0
          var ideaid = 0
          var adclass = 0
          var media_appsid = ""
          var planid = 0
          var unitid = 0
          var date = ""
          var hour = ""
          var search_time = ""
          x._2.foreach(
            x => {
              uid = x.getAs[String]("uid")
              userid = x.getAs[Int]("userid")
              ideaid = x.getAs[Int]("ideaid")
              adclass = x.getAs[Int]("adclass")
              media_appsid = x.getAs[String]("media_appsid")
              planid = x.getAs[Int]("planid")
              unitid = x.getAs[Int]("unitid")
              date = x.getAs[String]("date")
              hour = x.getAs[String]("hour")
              search_time = date + " " + hour

              if (!x.isNullAt(0)) { //trace_type为null时过滤
                val trace_type = x.getAs[String]("trace_type")
                if (trace_type == "active_third") {
                  active_third = 1
                }
              } else {
                active_third = -1
              }
            }
          )
          (x._1, active_third, uid, userid, ideaid, search_time, adclass, media_appsid, planid, unitid)
      }
      .filter(x => x._2 != -1) //过滤空值
      .toDF("searchid", "label", "uid", "userid", "ideaid", "search_time", "adclass", "media_appsid", "planid", "unitid")

    println("user api back: " + userApiBackRDD.count())

    userApiBackRDD
      .repartition(1)
      .write
      .mode(SaveMode.Overwrite)
      .parquet("/user/cpc/lrmodel/cvrdata_userapiback/%s/%s".format(date, hour))
    spark.sql(
      """
        |ALTER TABLE dl_cpc.ml_cvr_feature_v2 add if not exists PARTITION(`date` = "%s", `hour` = "%s")
        | LOCATION  '/user/cpc/lrmodel/cvrdata_userapiback/%s/%s'
      """.stripMargin.format(date, hour, date, hour))

    s"hadoop fs -touchz /user/cpc/okdir/ml_cvr_feature_v2_done/$date-$hour.ok" !


    //加粉类、直接下载类、落地页下载类、其他类(落地页非下载非加粉类) cvr计算

    //读取建站表单转化数据； active2建站表单 替换为adv.site_form_data中的转化数据
    /*
    val config = ConfigFactory.load()
    val url = config.getString("mariadb.adv.url")
    val properties = new Properties()
    properties.put("user", config.getString("mariadb.adv.user"))
    properties.put("password", config.getString("mariadb.adv.password"))
    properties.put("driver", config.getString("mariadb.adv.driver"))

    val table =
      s"""
         |(select "" as flag1
         |       ,"" as flag2
         |       ,0 as flag3
         |       ,search_id
         |       ,0 as adslot_type
         |       ,"" as client_type
         |       ,0 as adclass
         |       ,site_id as siteid
         |       ,0 as adsrc
         |       ,0 as interaction
         |       ,"" as uid
         |       ,user_id as userid
         |       ,idea_id as ideaid
         |       ,telephone
         |       ,"" as trace_type
         |       ,"" as trace_op1
         |       ,0 as duration
         |from adv.site_form_data
         |where SUBSTR(create_time,1,10)="%s" and SUBSTR(create_time,12,2)>="%s" and SUBSTR(create_time,12,2)<="%s"
         |    and idea_id > 0) as t
       """.stripMargin.format(date, before1hour, hour)
    println("table: " + table)
    val site_form = spark.read.jdbc(url, table, properties)
    println("site_form " + site_form.count())
    println("site_form schema" + site_form.printSchema())
    */
    val cvrlog = spark.sql(
      s"""
         |select  b.trace_type as flag1
         |       ,b.trace_op1 as flag2
         |       ,a.searchid as search_id
         |       ,a.adslot_type
         |       ,a.ext["client_type"].string_value as client_type
         |       ,a.ext["adclass"].int_value  as adclass
         |       ,a.ext_int['siteid'] as siteid
         |       ,a.adsrc
         |       ,a.interaction
         |       ,a.uid
         |       ,a.userid
         |       ,a.ideaid
         |       ,b.trace_type
         |       ,b.trace_op1
         |       ,b.duration
         |from (select * from dl_cpc.cpc_union_log
         |        where `date` = "%s" and `hour` = "%s" and searchid is not null and searchid != "") a
         |    left join (select id from bdm.cpc_userid_test_dim where day='%s') t2
         |        on a.userid = t2.id
         |    left join
         |        (select *
         |            from dl_cpc.cpc_union_trace_log
         |            where `date` = "%s" and `hour` = "%s"
         |         ) b
         |    on a.searchid=b.searchid
         | where t2.id is null
            """.stripMargin.format(date, hour, yesterday, date, hour))
      //.union(site_form)
      .rdd
      .map {
        x =>
          ((x.getAs[String]("search_id"), x.getAs[Int]("ideaid")), Seq(x))
      }
      .reduceByKey(_ ++ _)
      .map {
        x =>
          val convert = Utils.cvrPositiveV(x._2, version)
          val (convert2, label_type) = Utils.cvrPositiveV2(x._2, version) //新cvr
        val convert_sdk_dlapp = Utils.cvrPositive_sdk_dlapp(x._2, version) //sdk栏位下载app的转化数

          //存储active行为数据
          var active_map: Map[String, Int] = Map()
          //active1,active2,active3,active4,active5,active6,disactive,active_auto,active_auto_download,active_auto_submit,active_wx,active_third
          x._2.foreach(
            x => {
              if ((!x.isNullAt(0)) && (!x.isNullAt(1))) { //过滤 cpc_union_log有cpc_union_trace_log 没有的
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

                //REPORT_USER_STAYINWX：用户点击落地页里的加微信链接跳转到微信然后10秒内没有回来,表示已经转化，REPORT_USER_STAYINWX记为1，否则为0
                if (trace_op1 == "REPORT_USER_STAYINWX") {
                  active_map += ("report_user_stayinwx" -> 1)
                }

              }
            }
          )

          (x._1._1, x._1._2, convert, convert2, label_type, convert_sdk_dlapp,
            active_map.getOrElse("active1", 0), active_map.getOrElse("active2", 0), active_map.getOrElse("active3", 0),
            active_map.getOrElse("active4", 0), active_map.getOrElse("active5", 0), active_map.getOrElse("active6", 0),
            active_map.getOrElse("disactive", 0), active_map.getOrElse("active_href", 0), active_map.getOrElse("installed", 0),
            active_map.getOrElse("report_user_stayinwx", 0))
      }
      .toDF("searchid", "ideaid", "label", "label2", "label_type", "label_sdk_dlapp", "active1", "active2", "active3", "active4", "active5", "active6",
        "disactive", "active_href", "installed", "report_user_stayinwx")

    //cvrlog.filter(x => x.getAs[String]("searchid") == "02c2cfe082a1aa43074b6841ac37a36efefd4e8d").show()
    cvrlog.take(3).map(x => println(x))
    println("cvr log", cvrlog.count(), cvrlog.filter(r => r.getAs[Int]("label2") > 0).count())


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
        |       ext['long_click_count'].int_value as user_long_click_count,
        |       ext['exp_ctr'].int_value as exp_ctr,
        |       ext['exp_cvr'].int_value as exp_cvr,
        |       ext['usertype'].int_value as usertype
        |from dl_cpc.cpc_union_log where `date` = "%s" and `hour` = "%s" and isclick = 1
        |
          """.stripMargin.format(date, hour)
    println(sqlStmt)
    val clicklog = spark.sql(sqlStmt)
    println("click log", clicklog.count())

    clicklog.join(cvrlog, Seq("searchid", "ideaid"))
      .repartition(1)
      .write
      .mode(SaveMode.Overwrite)
      .parquet("/user/cpc/lrmodel/cvrdata_%s/%s/%s".format(version, date, hour))
    spark.sql(
      """
        |ALTER TABLE dl_cpc.ml_cvr_feature_v1 add if not exists PARTITION(`date` = "%s", `hour` = "%s")
        | LOCATION  '/user/cpc/lrmodel/cvrdata_v2/%s/%s'
      """.stripMargin.format(date, hour, date, hour)) // //

    //输出标记文件
    s"hadoop fs -touchz /user/cpc/okdir/ml_cvr_feature_v1_done/$date-$hour.ok" ! //

  }

  def saveCvrDataV3(spark: SparkSession, date: String, hour: String, yesterday: String, version: String): Unit = {
    import spark.implicits._

    val cal = Calendar.getInstance()
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    cal.set(date.substring(0, 4).toInt, date.substring(5, 7).toInt - 1, date.substring(8, 10).toInt, hour.toInt, 0, 0)
    cal.add(Calendar.HOUR, -1)
    val fDate = dateFormat.format(cal.getTime)
    val before1hour = fDate.substring(11, 13)

    /* 激励下载转化 */
    val sql_motivate =
      s"""
         |select   a.searchid
         |        ,a.ideaid
         |        ,7  as adslot_type
         |        ,"unknown" as client_type
         |        ,-1 as adclass
         |        ,-1 as siteid
         |        ,-1 as adsrc
         |        ,-1 as interaction
         |        ,"unknown" as uid
         |        ,a.userid
         |        ,b.trace_type
         |        ,b.trace_op1
         |        ,b.duration
         |        ,"conv_motivate" as flag
         |from (select * from dl_cpc.cpc_motivation_log
         |        where `date` = "%s" and `hour` = "%s" and searchid is not null and searchid != "" and isclick = 1) a
         |    left join (select id from bdm.cpc_userid_test_dim where day='%s') t2
         |        on a.userid = t2.id
         |    join
         |        (select *
         |            from dl_cpc.logparsed_cpc_trace_minute
         |            where `thedate` = "%s" and `thehour` = "%s"
         |         ) b
         |    on a.searchid=b.searchid and a.ideaid=b.opt['ideaid']
         | where t2.id is null
        """.stripMargin.format(date, hour, yesterday, date, hour)
    println("sql_motivate: " + sql_motivate)


    /* 用户Api回传转化 */
    val sql_api =
      s"""
         |select   a.searchid
         |        ,a.ideaid
         |        ,a.adslot_type
         |        ,a.ext["client_type"].string_value as client_type
         |        ,a.ext["adclass"].int_value  as adclass
         |        ,a.ext_int['siteid'] as siteid
         |        ,a.adsrc
         |        ,a.interaction
         |        ,a.uid
         |        ,a.userid
         |        ,b.trace_type
         |        ,b.trace_op1
         |        ,b.duration
         |        ,"conv_api" as flag
         |from (select * from dl_cpc.cpc_user_api_callback_union_log where %s and adslot_type <> 7) as a
         |    join dl_cpc.logparsed_cpc_trace_minute as b
         |        on a.searchid = b.searchid and a.ideaid = b.opt['ideaid']
         |    left join (select id from bdm.cpc_userid_test_dim where day='%s') t2 on a.userid = t2.id
         |where  b.`thedate` = "%s" and b.`thehour` = "%s" and t2.id is null
       """.stripMargin.format(get3DaysBefore(date, hour), yesterday, date, hour)
    println("sql_api: " + sql_api)

    /* 没有api回传标记，直接上报到trace */
    val sql_api_callback =
      s"""
         |select  a.searchid
         |       ,a.ideaid
         |       ,a.adslot_type
         |       ,a.ext["client_type"].string_value as client_type
         |       ,a.ext["adclass"].int_value  as adclass
         |       ,a.ext_int['siteid'] as siteid
         |       ,a.adsrc
         |       ,a.interaction
         |       ,a.uid
         |       ,a.userid
         |       ,b.trace_type
         |       ,b.trace_op1
         |       ,b.duration
         |       ,"conv_api" as flag
         |from ( select * from dl_cpc.cpc_union_log
         |       where `date`="%s" and hour>="%s" and hour<="%s" and ext_int['is_api_callback'] = 0 and adslot_type <> 7 and isclick = 1
         |      ) a
         |    join dl_cpc.logparsed_cpc_trace_minute b on a.searchid = b.searchid and a.ideaid = b.opt['ideaid']
         |    left join (select id from bdm.cpc_userid_test_dim where day='%s') t2 on a.userid = t2.id
         |where  b.`thedate` = "%s" and b.`thehour` = "%s" and t2.id is null
       """.stripMargin.format(date, before1hour, hour, yesterday, date, hour)
    println("sql_api_callback: " + sql_api_callback)

    /* 应用商城api转化 */
    val sql_api_moti =
      s"""
         |select  a.searchid
         |       ,a.ideaid
         |       ,7  as adslot_type
         |       ,"unknown" as client_type
         |       ,-1 as adclass
         |       ,-1 as siteid
         |       ,-1 as adsrc
         |       ,-1 as interaction
         |       ,"unknown" as uid
         |       ,a.userid
         |       ,b.trace_type
         |       ,b.trace_op1
         |       ,b.duration
         |       ,"conv_api" as flag
         |from ( select * from dl_cpc.cpc_motivation_log where %s and isclick = 1) a
         |  join ( select searchid
         |            ,opt['ideaid'] as ideaid
         |            ,trace_type
         |            ,trace_op1
         |            ,duration
         |      from dl_cpc.logparsed_cpc_trace_minute
         |      where `thedate` = "%s" and `thehour` = "%s" and trace_type = 'active_third'
         |   ) b
         |on a.searchid = b.searchid and a.ideaid = b.ideaid
         |left join (select id from bdm.cpc_userid_test_dim where day='%s') t2 on a.userid = t2.id
         |where t2.id is null
       """.stripMargin.format(get3DaysBefore(date, hour), date, hour, yesterday)
    println("sql_api_moti: " + sql_api_moti)

    val userApiBackRDD = (spark.sql(sql_api)).union(spark.sql(sql_api_callback)).union(spark.sql(sql_api_moti))


    /* 信息流转化：加粉类、直接下载类、落地页下载类、其他类(落地页非下载非加粉类) */
    val sql_info_flow =
      s"""
         |select  a.searchid
         |       ,a.ideaid
         |       ,a.adslot_type
         |       ,a.ext["client_type"].string_value as client_type
         |       ,a.ext["adclass"].int_value  as adclass
         |       ,a.ext_int['siteid'] as siteid
         |       ,a.adsrc
         |       ,a.interaction
         |       ,a.uid
         |       ,a.userid
         |       ,b.trace_type
         |       ,b.trace_op1
         |       ,b.duration
         |       ,"conv_info_flow" as flag
         |from (select * from dl_cpc.cpc_union_log
         |        where `date` = "%s" and `hour` = "%s" and searchid is not null and searchid != "") a
         |    left join (select id from bdm.cpc_userid_test_dim where day='%s') t2
         |        on a.userid = t2.id
         |    join
         |        (select *
         |            from dl_cpc.cpc_union_trace_log
         |            where `date` = "%s" and `hour` = "%s"
         |         ) b
         |    on a.searchid=b.searchid and a.ideaid = b.opt['ideaid']
         | where t2.id is null
            """.stripMargin.format(date, hour, yesterday, date, hour)
    println("sql_info_flow: " + sql_info_flow)

    val cvrlog = (spark.sql(sql_motivate)).union(spark.sql(sql_api)).union(spark.sql(sql_api_callback)).union(spark.sql(sql_api_moti)).union(spark.sql(sql_info_flow))
      .rdd
      .map {
        x =>
          ((x.getAs[String]("searchid"), x.getAs[Int]("ideaid"), x.getAs[String]("flag")), Seq(x))
      }
      .reduceByKey(_ ++ _)
      .map {
        x =>
          val flag = x._1._3
          var convert = 0
          var convert2 = 0
          var convert_sdk_dlapp = 0
          var label_type = 0

          if (flag == "conv_motivate") {//激励下载转化
            (convert2, label_type) = Utils.cvrPositive_motivate(x._2, version)
          } else if (flag == "conv_api") {//Api回传转化
            (convert2, label_type) = Utils.cvrPositive_api(x._2, version)
          } else if (flag == "conv_info_flow") {//信息流转化
            convert = Utils.cvrPositiveV(x._2, version)
            (convert2, label_type) = Utils.cvrPositiveV2(x._2, version)
            (convert_sdk_dlapp, label_type) = Utils.cvrPositive_sdk_dlapp(x._2, version) //sdk栏位下载app的转化数
          }

          //存储active行为数据
          var active_map: Map[String, Int] = Map()
          //active1,active2,active3,active4,active5,active6,disactive,active_auto,active_auto_download,active_auto_submit,active_wx,active_third
          x._2.foreach(
            x => {
              if ((!x.isNullAt(0)) && (!x.isNullAt(1))) { //过滤 cpc_union_log有cpc_union_trace_log 没有的
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

                //REPORT_USER_STAYINWX：用户点击落地页里的加微信链接跳转到微信然后10秒内没有回来,表示已经转化，REPORT_USER_STAYINWX记为1，否则为0
                if (trace_op1 == "REPORT_USER_STAYINWX") {
                  active_map += ("report_user_stayinwx" -> 1)
                }

              }
            }
          )

          (x._1._1, x._1._2, convert, convert2, label_type, convert_sdk_dlapp,
            active_map.getOrElse("active1", 0), active_map.getOrElse("active2", 0), active_map.getOrElse("active3", 0),
            active_map.getOrElse("active4", 0), active_map.getOrElse("active5", 0), active_map.getOrElse("active6", 0),
            active_map.getOrElse("disactive", 0), active_map.getOrElse("active_href", 0), active_map.getOrElse("installed", 0),
            active_map.getOrElse("report_user_stayinwx", 0))

      }
      .toDF("searchid", "ideaid", "label", "label2", "label_type", "label_sdk_dlapp", "active1", "active2", "active3", "active4", "active5", "active6",
        "disactive", "active_href", "installed", "report_user_stayinwx")

    println("cvr log", cvrlog.count(), cvrlog.filter(r => r.getAs[Int]("label2") > 0).count())


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
        |       ext['long_click_count'].int_value as user_long_click_count,
        |       ext['exp_ctr'].int_value as exp_ctr,
        |       ext['exp_cvr'].int_value as exp_cvr,
        |       ext['usertype'].int_value as usertype
        |from dl_cpc.cpc_union_log where `date` = "%s" and `hour` = "%s" and isclick = 1
          """.stripMargin.format(date, hour)
    println(sqlStmt)
    val clicklog = spark.sql(sqlStmt)
    println("click log", clicklog.count())

    clicklog.join(cvrlog, Seq("searchid", "ideaid"))
      .repartition(1)
      .write
      .mode(SaveMode.Overwrite)
      .parquet("/warehouse/test.db/ml_cvr_feature_v1/%s/%s".format(date, hour))
    spark.sql(
      """
        |ALTER TABLE test.ml_cvr_feature_v1 add if not exists PARTITION(`date` = "%s", `hour` = "%s")
        | LOCATION  '/warehouse/test.db/ml_cvr_feature_v1/%s/%s'
      """.stripMargin.format(date, hour, date, hour))

  }

  def get3DaysBefore(date: String, hour: String): String = {
    val dateHourList = ListBuffer[String]()
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val cal = Calendar.getInstance()
    cal.set(date.substring(0, 4).toInt, date.substring(5, 7).toInt - 1, date.substring(8, 10).toInt, hour.toInt, 0, 0)
    for (t <- 0 to 72) {
      if (t > 0) {
        cal.add(Calendar.HOUR, -1)
      }
      val formatDate = dateFormat.format(cal.getTime)
      val datee = formatDate.substring(0, 10)
      val hourr = formatDate.substring(11, 13)

      val dateL = s"(`date`='$datee' and `hour`='$hourr')"
      dateHourList += dateL
    }

    "(" + dateHourList.mkString(" or ") + ")"
  }


}




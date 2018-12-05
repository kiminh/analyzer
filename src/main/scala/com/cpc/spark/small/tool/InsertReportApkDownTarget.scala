package com.cpc.spark.small.tool

import java.sql.DriverManager
import java.util.Properties

import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * Created by wanli on 2017/11/14.
  */
object InsertReportApkDownTarget {

  var mariaAdvdbUrl = ""
  val mariaAdvdbProp = new Properties()

  var mariaReportdbUrl = ""
  val mariaReportdbProp = new Properties()

  case class Info(
                   searchid: String = "",
                   planid: Int = 0,
                   unitid: Int = 0,
                   isshow: Long = 0,
                   isclick: Long = 0,
                   sex: Int = 0,
                   age: Int = 0,
                   os: Int = 0,
                   province: Int = 0,
                   phoneLevel: Int = 0,
                   hour: Int = 0,
                   network: Int = 0,
                   userLevel: Int = 0,
                   qukanNewUser: Int = 0,
                   adslotType: Int = 0,
                   mediaid: Int = 0,
                   adslotid: Int = 0,
                   brand: String = "",
                   browserType: Int = 0,
                   isStudent: Int = 0, //0未知，1学生，2非学生
                   start: Long = 0,
                   finish: Long = 0,
                   pkgadded: Long = 0,
                   traceType: String = "",
                   userid: Int = 0,
                   instHijack: Long = 0
                 )


  def main(args: Array[String]): Unit = {
    Logger.getRootLogger.setLevel(Level.WARN)
    val argDay = args(0).toString

    val confDav = ConfigFactory.load()
    mariaAdvdbUrl = confDav.getString("mariadb.adv.url")
    mariaAdvdbProp.put("user", confDav.getString("mariadb.adv.user"))
    mariaAdvdbProp.put("password", confDav.getString("mariadb.adv.password"))
    mariaAdvdbProp.put("driver", confDav.getString("mariadb.adv.driver"))

    val conf = ConfigFactory.load()
    mariaReportdbUrl = conf.getString("mariadb.url")
    mariaReportdbProp.put("user", conf.getString("mariadb.user"))
    mariaReportdbProp.put("password", conf.getString("mariadb.password"))
    mariaReportdbProp.put("driver", conf.getString("mariadb.driver"))

    val ctx = SparkSession
      .builder()
      .config("spark.debug.maxToStringFields", "2000")
      .appName("InsertReportApkDownTarget is run day is %s".format(argDay))
      .enableHiveSupport()
      .getOrCreate()

    println("InsertReportApkDownTarget is run day is %s".format(argDay))


    var brandMaps: Map[String, Int] = Map(
      "oppo" -> 1,
      "vivo" -> 2,
      "huawei" -> 3,
      "xiaomi" -> 4,
      "honor" -> 5,
      "meizu" -> 6,
      "samsung" -> 7,
      "gionee" -> 8,
      "leeco" -> 9,
      "zte" -> 10,
      "360" -> 11,
      "coolpad" -> 12,
      "letv" -> 13,
      "lenovo" -> 14,
      "nubia" -> 15,
      "smartisan" -> 16,
      "hisense" -> 17,
      "doov" -> 18,
      "cmcc" -> 19,
      "koobee" -> 20,
      "lephone" -> 21,
      "xiaolajiao" -> 22,
      "sugar" -> 23,
      "oneplus" -> 24,
      "ivvi" -> 25,
      "htc" -> 26
    )

    val broadcastBrandMaps = ctx.sparkContext.broadcast(brandMaps)

    val lploadUnionData = ctx
      .sql(
        """
          |SELECT searchid,planid,unitid,isshow,isclick,sex,age,os,province,ext['phone_level'].int_value,hour,
          |network,coin,ext['qukan_new_user'].int_value,adslot_type,media_appsid,adslotid,brand,ext_int["browser_type"],
          |interests,userid
          |FROM dl_cpc.cpc_union_log cul
          |WHERE date="%s" AND (isshow+isclick)>0 AND ext["client_type"].string_value="NATIVESDK" AND cul.adsrc=1 AND adslot_type<>7
          |AND cul.ideaid in(
          | SELECT DISTINCT cul.ideaid
          | FROM dl_cpc.cpc_union_trace_log cutl
          | INNER JOIN dl_cpc.cpc_union_log cul ON cutl.searchid=cul.searchid
          | WHERE cutl.date="%s" AND cul.date="%s" AND cutl.trace_type in("lpload")
          | AND cutl.trace_op1 in("REPORT_DOWNLOAD_START","REPORT_DOWNLOAD_FINISH","REPORT_DOWNLOAD_PKGADDED")
          | AND cul.ext["client_type"].string_value="NATIVESDK"
          | AND cul.isclick>0 AND cul.adsrc=1 AND adslot_type<>7
          |)
        """.stripMargin.format(argDay, argDay, argDay))
      .rdd
      .map {
        x =>
          val searchid = x.getString(0)
          val planid = x.getInt(1)
          val unitid = x.getInt(2)
          val isshow = if (x.getInt(4) > 0) 1 else x.get(3).toString.toLong
          val isclick = x.get(4).toString.toLong
          val sex = x.getInt(5)
          val age = x.getInt(6)
          val os = x.getInt(7)
          val province = x.getInt(8)
          val phoneLevel = x.getInt(9)
          val hour = x.getString(10).toInt
          val network = x.getInt(11)
          val coin = x.getInt(12)
          //coin
          var userLevel = 0
          if (coin == 0) {
            userLevel = 1
          } else if (coin <= 60) {
            userLevel = 2
          } else if (coin <= 90) {
            userLevel = 3
          } else {
            userLevel = 4
          }

          val qukanNewUser = x.getInt(13)
          val adslotType = x.getInt(14)
          val mediaId = x.getString(15).toInt
          val adslotid = x.getString(16).toInt
          val brand = if (x.get(17) != null) x.get(17).toString else ""
          val browserType = x.get(18).toString.toInt

          val interests = x.get(19).toString
          val isStudent = if (interests.contains("224=")) 1 else if (interests.contains("225=")) 2 else 0
          val userid = x.getInt(20)

          (searchid, (Info(searchid, planid, unitid, isshow, isclick, sex, age, os, province, phoneLevel, hour,
            network, userLevel, qukanNewUser, adslotType, mediaId, adslotid, brand, browserType, isStudent, 0, 0, 0, "", userid)))
      }
      .repartition(50)
    // println("lploadUnionData count", lploadUnionData.count())


    val lploadTraceData = ctx.sql(
      """
        |SELECT DISTINCT cutl.searchid,cutl.trace_type,cutl.trace_op1
        |FROM dl_cpc.cpc_union_trace_log cutl
        |INNER JOIN dl_cpc.cpc_union_log cul ON cutl.searchid=cul.searchid
        |WHERE cutl.date="%s" AND cul.date="%s" AND cutl.trace_type in("lpload")
        |AND cutl.trace_op1 in("REPORT_DOWNLOAD_START","REPORT_DOWNLOAD_FINISH","REPORT_DOWNLOAD_PKGADDED","REPORT_DOWNLOAD_INST_HIJACK")
        |AND cul.ext["client_type"].string_value="NATIVESDK"
        |AND cul.isclick>0 AND cul.adsrc=1 AND adslot_type<>7
      """.stripMargin.format(argDay, argDay))
      .rdd
      .map {
        x =>
          val searchid = x.getString(0)
          val trace_type = x.getString(1)
          var trace_op1 = x.getString(2)
          var start: Int = 0
          var finish: Int = 0
          var pkgadded: Int = 0
          var instHijack: Int = 0
          if (trace_op1 == "REPORT_DOWNLOAD_START") {
            start = 1
          } else if (trace_op1 == "REPORT_DOWNLOAD_FINISH") {
            finish = 1
          } else if (trace_op1 == "REPORT_DOWNLOAD_PKGADDED") {
            pkgadded = 1
          } else if (trace_op1 == "REPORT_DOWNLOAD_INST_HIJACK") {
            instHijack = 1
          }
          (searchid, (Info(searchid, -1, -1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, "", 0, 0, start, finish, pkgadded, trace_type, 0, instHijack)))
      }
      .repartition(50)

    println("lploadTraceData count", lploadTraceData.count())


    val lploadAllData = lploadUnionData
      .union(lploadTraceData)
      .reduceByKey {
        (a, b) =>
          val searchid = if (a.planid != -1) a.searchid else b.searchid
          val planid = if (a.planid != -1) a.planid else b.planid
          val unitid = if (a.planid != -1) a.unitid else b.unitid
          val isshow = a.isshow + b.isshow
          val isclick = a.isclick + b.isclick
          val sex = if (a.planid != -1) a.sex else b.sex
          val age = if (a.planid != -1) a.age else b.age
          val os = if (a.planid != -1) a.os else b.os
          val province = if (a.planid != -1) a.province else b.province
          val phoneLevel = if (a.planid != -1) a.phoneLevel else b.phoneLevel
          val hour = if (a.planid != -1) a.hour else b.hour
          val network = if (a.planid != -1) a.network else b.network
          val userLevel = if (a.planid != -1) a.userLevel else b.userLevel
          val qukanNewUser = if (a.planid != -1) a.qukanNewUser else b.qukanNewUser
          val adslotType = if (a.planid != -1) a.adslotType else b.adslotType
          val mediaid = if (a.planid != -1) a.mediaid else b.mediaid
          val adslotid = if (a.planid != -1) a.adslotid else b.adslotid
          val brand = if (a.planid != -1) a.brand else b.brand
          val browserType = if (a.planid != -1) a.browserType else b.browserType
          val isStudent = if (a.planid != -1) a.isStudent else b.isStudent
          val start = a.start + b.start
          val finish = a.finish + b.finish
          val pkgadded = a.pkgadded + b.pkgadded
          val traceType = "lpload"
          val instHijack = a.instHijack + b.instHijack
          //if (a.traceType.length > 0) a.traceType else b.traceType
          var userid = if (a.userid > 0) a.userid else b.userid

          Info(searchid, planid, unitid, isshow, isclick, sex, age, os, province, phoneLevel, hour, network, userLevel, qukanNewUser, adslotType,
            mediaid, adslotid, brand, browserType, isStudent, start, finish, pkgadded, traceType, userid, instHijack)
      }
      .map {
        x =>
          val info = x._2
          Info(info.searchid, info.planid, info.unitid, info.isshow, info.isclick, info.sex, info.age, info.os, info.province, info.phoneLevel, info.hour,
            info.network, info.userLevel, info.qukanNewUser, info.adslotType, info.mediaid, info.adslotid, info.brand, info.browserType,
            info.isStudent, info.start, info.finish, info.pkgadded, "lpload", info.userid, info.instHijack)
      }
      .filter { x => x.unitid > 0 && x.planid > 0 }
      .repartition(50)
      .cache()
    println("lploadAllData count", lploadAllData.count())


    val motivationUnionData = ctx
      .sql(
        """
          |SELECT searchid,m.planid,m.unitid,m.isshow,m.isclick,sex,age,os,province,ext['phone_level'].int_value,hour,
          |network,coin,ext['qukan_new_user'].int_value,adslot_type,media_appsid,adslotid,brand,ext_int["browser_type"],
          |interests,m.userid,m.ideaid
          |FROM dl_cpc.cpc_union_log cul
          |lateral view explode(motivation) b as m
          |WHERE `date`="%s" AND (m.isshow+m.isclick)>0 AND ext["client_type"].string_value="NATIVESDK" AND cul.adsrc=1
          |AND adslot_type=7 AND m.ideaid>0
        """.stripMargin.format(argDay))
      .rdd
      .map {
        x =>
          val searchid = x.getString(0)
          val planid = x.getInt(1)
          val unitid = x.getInt(2)
          val isshow = if (x.getInt(4) > 0) 1 else x.get(3).toString.toLong
          val isclick = x.get(4).toString.toLong
          val sex = x.getInt(5)
          val age = x.getInt(6)
          val os = x.getInt(7)
          val province = x.getInt(8)
          val phoneLevel = x.getInt(9)
          val hour = x.getString(10).toInt
          val network = x.getInt(11)
          val coin = x.getInt(12)
          //coin
          var userLevel = 0
          if (coin == 0) {
            userLevel = 1
          } else if (coin <= 60) {
            userLevel = 2
          } else if (coin <= 90) {
            userLevel = 3
          } else {
            userLevel = 4
          }

          val qukanNewUser = x.getInt(13)
          val adslotType = x.getInt(14)
          val mediaId = x.getString(15).toInt
          val adslotid = x.getString(16).toInt
          val brand = if (x.get(17) != null) x.get(17).toString else ""
          val browserType = x.get(18).toString.toInt

          val interests = x.get(19).toString
          val isStudent = if (interests.contains("224=")) 1 else if (interests.contains("225=")) 2 else 0
          val userid = x.getInt(20)
          val ideaid = x.getInt(21)

          ((searchid, ideaid), (Info(searchid, planid, unitid, isshow, isclick, sex, age, os, province, phoneLevel, hour,
            network, userLevel, qukanNewUser, adslotType, mediaId, adslotid, brand, browserType, isStudent, 0, 0, 0, "", userid)))
      }
      .repartition(50)
    //    println("motivationUnionData count", motivationUnionData.count())
    //    motivationUnionData.take(10).foreach(println)

    val motivationTraceData = ctx.sql(
      """
        |select DISTINCT searchid,trace_type,trace_op1,opt["ideaid"]
        |from dl_cpc.logparsed_cpc_trace_minute
        |where trace_type="sdk_incite" and `thedate`="%s"
        |and trace_op1 in("DOWNLOAD_START","DOWNLOAD_FINISH","INSTALL_FINISH","INSTALL_HIJACK") AND opt["ideaid"]>0
      """.stripMargin.format(argDay))
      .rdd
      .map {
        x =>
          val searchid = x.getString(0)
          val trace_type = x.getString(1)
          var trace_op1 = x.getString(2)
          val ideaid = x.get(3).toString.toInt
          var start: Int = 0
          var finish: Int = 0
          var pkgadded: Int = 0
          var instHijack: Int = 0
          if (trace_op1 == "DOWNLOAD_START") {
            start = 1
          } else if (trace_op1 == "DOWNLOAD_FINISH") {
            finish = 1
          } else if (trace_op1 == "INSTALL_FINISH") {
            pkgadded = 1
          } else if (trace_op1 == "INSTALL_HIJACK") {
            instHijack = 1
          }
          ((searchid, ideaid), (Info(searchid, -1, -1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, "", 0, 0, start, finish, pkgadded, trace_type, 0, instHijack)))
      }
      .repartition(50)

    //    println("motivationTraceData count", motivationTraceData.count())
    //    motivationTraceData.take(10).foreach(println)

    val motivationAllData = motivationUnionData
      .union(motivationTraceData)
      .reduceByKey {
        (a, b) =>
          val searchid = if (a.planid != -1) a.searchid else b.searchid
          val planid = if (a.planid != -1) a.planid else b.planid
          val unitid = if (a.planid != -1) a.unitid else b.unitid
          val isshow = a.isshow + b.isshow
          val isclick = a.isclick + b.isclick
          val sex = if (a.planid != -1) a.sex else b.sex
          val age = if (a.planid != -1) a.age else b.age
          val os = if (a.planid != -1) a.os else b.os
          val province = if (a.planid != -1) a.province else b.province
          val phoneLevel = if (a.planid != -1) a.phoneLevel else b.phoneLevel
          val hour = if (a.planid != -1) a.hour else b.hour
          val network = if (a.planid != -1) a.network else b.network
          val userLevel = if (a.planid != -1) a.userLevel else b.userLevel
          val qukanNewUser = if (a.planid != -1) a.qukanNewUser else b.qukanNewUser
          val adslotType = if (a.planid != -1) a.adslotType else b.adslotType
          val mediaid = if (a.planid != -1) a.mediaid else b.mediaid
          val adslotid = if (a.planid != -1) a.adslotid else b.adslotid
          val brand = if (a.planid != -1) a.brand else b.brand
          val browserType = if (a.planid != -1) a.browserType else b.browserType
          val isStudent = if (a.planid != -1) a.isStudent else b.isStudent
          val start = a.start + b.start
          val finish = a.finish + b.finish
          val pkgadded = a.pkgadded + b.pkgadded
          val traceType = "sdk_incite"
          //if (a.traceType.length > 0) a.traceType else b.traceType
          var userid = if (a.userid > 0) a.userid else b.userid
          val instHijack = a.instHijack + b.instHijack
          Info(searchid, planid, unitid, isshow, isclick, sex, age, os, province, phoneLevel, hour, network, userLevel, qukanNewUser, adslotType,
            mediaid, adslotid, brand, browserType, isStudent, start, finish, pkgadded, traceType, userid, instHijack)
      }
      .map {
        x =>
          val info = x._2
          Info(info.searchid, info.planid, info.unitid, info.isshow, info.isclick, info.sex, info.age, info.os, info.province, info.phoneLevel, info.hour,
            info.network, info.userLevel, info.qukanNewUser, info.adslotType, info.mediaid, info.adslotid, info.brand, info.browserType,
            info.isStudent, info.start, info.finish, info.pkgadded, "sdk_incite", info.userid, info.instHijack)
      }
      .filter { x => x.unitid > 0 && x.planid > 0 }
      .repartition(50)
      .cache()

    //println("motivationAllData count", motivationAllData.count())
    //motivationAllData.take(10).foreach(println)

    var siteData = ctx.read.jdbc(mariaAdvdbUrl,
      """
        |(
        | select id
        | from site_building
        | where ext like "%downloadLink%" and ext not like '%s:12:"downloadLink";i:%' and template_type != 2
        |) xx
      """.stripMargin, mariaAdvdbProp)
      .rdd
      .map(
        x =>
          x.get(0)
      )
      .map {
        x =>
          x.toString.toInt
      }
      .take(99999)

    val siteUnionData = ctx
      .sql(
        """
          |SELECT searchid,planid,unitid,isshow,isclick,sex,age,os,province,ext['phone_level'].int_value,hour,
          |network,coin,ext['qukan_new_user'].int_value,adslot_type,media_appsid,adslotid,brand,ext_int["browser_type"],
          |interests,userid
          |FROM dl_cpc.cpc_union_log cul
          |WHERE date="%s" AND (isshow+isclick)>0 AND ext["client_type"].string_value="NATIVESDK" AND cul.adsrc=1 AND adslot_type<>7
          |AND ext_int["siteid"]>0 AND ext_int["siteid"] in(%s)
        """.stripMargin.format(argDay, siteData.mkString(",")))
      .rdd
      .map {
        x =>
          val searchid = x.getString(0)
          val planid = x.getInt(1)
          val unitid = x.getInt(2)
          val isshow = if (x.getInt(4) > 0) 1 else x.get(3).toString.toLong
          val isclick = x.get(4).toString.toLong
          val sex = x.getInt(5)
          val age = x.getInt(6)
          val os = x.getInt(7)
          val province = x.getInt(8)
          val phoneLevel = x.getInt(9)
          val hour = x.getString(10).toInt
          val network = x.getInt(11)
          val coin = x.getInt(12)
          //coin
          var userLevel = 0
          if (coin == 0) {
            userLevel = 1
          } else if (coin <= 60) {
            userLevel = 2
          } else if (coin <= 90) {
            userLevel = 3
          } else {
            userLevel = 4
          }

          val qukanNewUser = x.getInt(13)
          val adslotType = x.getInt(14)
          val mediaId = x.getString(15).toInt
          val adslotid = x.getString(16).toInt
          val brand = if (x.get(17) != null) x.get(17).toString else ""
          val browserType = x.get(18).toString.toInt

          val interests = x.get(19).toString
          val isStudent = if (interests.contains("224=")) 1 else if (interests.contains("225=")) 2 else 0
          val userid = x.getInt(20)

          (searchid, (Info(searchid, planid, unitid, isshow, isclick, sex, age, os, province, phoneLevel, hour,
            network, userLevel, qukanNewUser, adslotType, mediaId, adslotid, brand, browserType, isStudent, 0, 0, 0, "", userid)))
      }
      .repartition(50)
    //println("siteUnionData count", siteUnionData.count())


    val siteTraceData = ctx.sql(
      """
        |SELECT DISTINCT cutl.searchid,cutl.trace_type,cutl.trace_op1
        |FROM dl_cpc.cpc_union_trace_log cutl
        |INNER JOIN dl_cpc.cpc_union_log cul ON cutl.searchid=cul.searchid
        |WHERE cutl.date="%s" AND cul.date="%s" AND cutl.trace_type in("apkdown")
        |AND cutl.trace_op1 in("REPORT_DOWNLOAD_START","REPORT_DOWNLOAD_FINISH","REPORT_DOWNLOAD_PKGADDED","REPORT_DOWNLOAD_INST_HIJACK")
        |AND cul.ext["client_type"].string_value="NATIVESDK"
        |AND cul.isclick>0 AND cul.adsrc=1 AND adslot_type<>7
        |AND cul.ext_int["siteid"]>0 AND cul.ext_int["siteid"] in (%s)
      """.stripMargin.format(argDay, argDay, siteData.mkString(",")))
      .rdd
      .map {
        x =>
          val searchid = x.getString(0)
          val trace_type = x.getString(1)
          var trace_op1 = x.getString(2)
          var start: Int = 0
          var finish: Int = 0
          var pkgadded: Int = 0
          var instHijack: Int = 0
          if (trace_op1 == "REPORT_DOWNLOAD_START") {
            start = 1
          } else if (trace_op1 == "REPORT_DOWNLOAD_FINISH") {
            finish = 1
          } else if (trace_op1 == "REPORT_DOWNLOAD_PKGADDED") {
            pkgadded = 1
          } else if (trace_op1 == "REPORT_DOWNLOAD_INST_HIJACK") {
            instHijack = 1
          }
          (searchid, (Info(searchid, -1, -1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, "", 0, 0, start, finish, pkgadded, trace_type, 0, instHijack)))
      }
      .repartition(50)

    val siteAllData = siteUnionData
      .union(siteTraceData)
      .reduceByKey {
        (a, b) =>
          val searchid = if (a.planid != -1) a.searchid else b.searchid
          val planid = if (a.planid != -1) a.planid else b.planid
          val unitid = if (a.planid != -1) a.unitid else b.unitid
          val isshow = a.isshow + b.isshow
          val isclick = a.isclick + b.isclick
          val sex = if (a.planid != -1) a.sex else b.sex
          val age = if (a.planid != -1) a.age else b.age
          val os = if (a.planid != -1) a.os else b.os
          val province = if (a.planid != -1) a.province else b.province
          val phoneLevel = if (a.planid != -1) a.phoneLevel else b.phoneLevel
          val hour = if (a.planid != -1) a.hour else b.hour
          val network = if (a.planid != -1) a.network else b.network
          val userLevel = if (a.planid != -1) a.userLevel else b.userLevel
          val qukanNewUser = if (a.planid != -1) a.qukanNewUser else b.qukanNewUser
          val adslotType = if (a.planid != -1) a.adslotType else b.adslotType
          val mediaid = if (a.planid != -1) a.mediaid else b.mediaid
          val adslotid = if (a.planid != -1) a.adslotid else b.adslotid
          val brand = if (a.planid != -1) a.brand else b.brand
          val browserType = if (a.planid != -1) a.browserType else b.browserType
          val isStudent = if (a.planid != -1) a.isStudent else b.isStudent
          val start = a.start + b.start
          val finish = a.finish + b.finish
          val pkgadded = a.pkgadded + b.pkgadded
          val traceType = "site_building"
          //if (a.traceType.length > 0) a.traceType else b.traceType
          var userid = if (a.userid > 0) a.userid else b.userid
          val instHijack = a.instHijack + b.instHijack
          Info(searchid, planid, unitid, isshow, isclick, sex, age, os, province, phoneLevel, hour, network, userLevel, qukanNewUser, adslotType,
            mediaid, adslotid, brand, browserType, isStudent, start, finish, pkgadded, traceType, userid, instHijack)
      }
      .map {
        x =>
          val info = x._2
          Info(info.searchid, info.planid, info.unitid, info.isshow, info.isclick, info.sex, info.age, info.os, info.province, info.phoneLevel, info.hour,
            info.network, info.userLevel, info.qukanNewUser, info.adslotType, info.mediaid, info.adslotid, info.brand, info.browserType,
            info.isStudent, info.start, info.finish, info.pkgadded, "site_building", info.userid, info.instHijack)
      }
      .filter { x => x.unitid > 0 && x.planid > 0 }
      .repartition(50)
      .cache()

    //siteAllData.take(100).foreach(println)


    val unionData = ctx
      .sql(
        """
          |SELECT searchid,planid,unitid,isshow,isclick,sex,age,os,province,ext['phone_level'].int_value,hour,
          |network,coin,ext['qukan_new_user'].int_value,adslot_type,media_appsid,adslotid,brand,ext_int["browser_type"],
          |interests,userid
          |FROM dl_cpc.cpc_union_log cul
          |WHERE date="%s" AND (isshow+isclick)>0 AND ext["client_type"].string_value="NATIVESDK" AND cul.adsrc=1 AND adslot_type<>7
          |AND cul.interaction=2
        """.stripMargin.format(argDay))
      .rdd
      .map {
        x =>
          val searchid = x.getString(0)
          val planid = x.getInt(1)
          val unitid = x.getInt(2)
          val isshow = if (x.getInt(4) > 0) 1 else x.get(3).toString.toLong
          val isclick = x.get(4).toString.toLong
          val sex = x.getInt(5)
          val age = x.getInt(6)
          val os = x.getInt(7)
          val province = x.getInt(8)
          val phoneLevel = x.getInt(9)
          val hour = x.getString(10).toInt
          val network = x.getInt(11)
          val coin = x.getInt(12)
          //coin
          var userLevel = 0
          if (coin == 0) {
            userLevel = 1
          } else if (coin <= 60) {
            userLevel = 2
          } else if (coin <= 90) {
            userLevel = 3
          } else {
            userLevel = 4
          }

          val qukanNewUser = x.getInt(13)
          val adslotType = x.getInt(14)
          val mediaId = x.getString(15).toInt
          val adslotid = x.getString(16).toInt
          val brand = if (x.get(17) != null) x.get(17).toString else ""
          val browserType = x.get(18).toString.toInt

          val interests = x.get(19).toString
          val isStudent = if (interests.contains("224=")) 1 else if (interests.contains("225=")) 2 else 0
          val userid = x.getInt(20)

          (searchid, (Info(searchid, planid, unitid, isshow, isclick, sex, age, os, province, phoneLevel, hour,
            network, userLevel, qukanNewUser, adslotType, mediaId, adslotid, brand, browserType, isStudent, 0, 0, 0, "", userid)))
      }
      .repartition(50)
    //println("unionData count", unionData.count())


    val traceData = ctx.sql(
      """
        |SELECT DISTINCT cutl.searchid,cutl.trace_type,cutl.trace_op1
        |FROM dl_cpc.cpc_union_trace_log cutl
        |INNER JOIN dl_cpc.cpc_union_log cul ON cutl.searchid=cul.searchid
        |WHERE cutl.date="%s" AND cul.date="%s" AND cutl.trace_type in("apkdown")
        |AND cutl.trace_op1 in("REPORT_DOWNLOAD_START","REPORT_DOWNLOAD_FINISH","REPORT_DOWNLOAD_PKGADDED","REPORT_DOWNLOAD_INST_HIJACK")
        |AND cul.ext["client_type"].string_value="NATIVESDK"
        |AND cul.interaction=2
        |AND cul.isclick>0 AND cul.adsrc=1 AND adslot_type<>7
      """.stripMargin.format(argDay, argDay))
      .rdd
      .map {
        x =>
          val searchid = x.getString(0)
          val trace_type = x.getString(1)
          var trace_op1 = x.getString(2)
          var start: Int = 0
          var finish: Int = 0
          var pkgadded: Int = 0
          var instHijack: Int = 0
          if (trace_op1 == "REPORT_DOWNLOAD_START") {
            start = 1
          } else if (trace_op1 == "REPORT_DOWNLOAD_FINISH") {
            finish = 1
          } else if (trace_op1 == "REPORT_DOWNLOAD_PKGADDED") {
            pkgadded = 1
          } else if (trace_op1 == "REPORT_DOWNLOAD_INST_HIJACK") {
            instHijack = 1
          }
          (searchid, (Info(searchid, -1, -1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, "", 0, 0, start, finish, pkgadded, trace_type, 0, instHijack)))
      }
      .repartition(50)

    //println("traceData count", traceData.count())


    val allDatax = unionData
      .union(traceData)
      .reduceByKey {
        (a, b) =>
          val searchid = if (a.planid != -1) a.searchid else b.searchid
          val planid = if (a.planid != -1) a.planid else b.planid
          val unitid = if (a.planid != -1) a.unitid else b.unitid
          val isshow = a.isshow + b.isshow
          val isclick = a.isclick + b.isclick
          val sex = if (a.planid != -1) a.sex else b.sex
          val age = if (a.planid != -1) a.age else b.age
          val os = if (a.planid != -1) a.os else b.os
          val province = if (a.planid != -1) a.province else b.province
          val phoneLevel = if (a.planid != -1) a.phoneLevel else b.phoneLevel
          val hour = if (a.planid != -1) a.hour else b.hour
          val network = if (a.planid != -1) a.network else b.network
          val userLevel = if (a.planid != -1) a.userLevel else b.userLevel
          val qukanNewUser = if (a.planid != -1) a.qukanNewUser else b.qukanNewUser
          val adslotType = if (a.planid != -1) a.adslotType else b.adslotType
          val mediaid = if (a.planid != -1) a.mediaid else b.mediaid
          val adslotid = if (a.planid != -1) a.adslotid else b.adslotid
          val brand = if (a.planid != -1) a.brand else b.brand
          val browserType = if (a.planid != -1) a.browserType else b.browserType
          val isStudent = if (a.planid != -1) a.isStudent else b.isStudent
          val start = a.start + b.start
          val finish = a.finish + b.finish
          val pkgadded = a.pkgadded + b.pkgadded
          val traceType = "apkdown"
          //if (a.traceType.length > 0) a.traceType else b.traceType
          var userid = if (a.userid > 0) a.userid else b.userid
          val instHijack = a.instHijack + b.instHijack
          Info(searchid, planid, unitid, isshow, isclick, sex, age, os, province, phoneLevel, hour, network, userLevel, qukanNewUser, adslotType,
            mediaid, adslotid, brand, browserType, isStudent, start, finish, pkgadded, traceType, userid, instHijack)
      }
      .map {
        x =>
          val info = x._2
          Info(info.searchid, info.planid, info.unitid, info.isshow, info.isclick, info.sex, info.age, info.os, info.province, info.phoneLevel, info.hour,
            info.network, info.userLevel, info.qukanNewUser, info.adslotType, info.mediaid, info.adslotid, info.brand, info.browserType,
            info.isStudent, info.start, info.finish, info.pkgadded, "apkdown", info.userid, info.instHijack)
      }
      .filter { x => x.unitid > 0 && x.planid > 0 }
      .repartition(50)
      .cache()

    clearReportApkDownTarget(argDay)

    //-----
    var insertDataFramelpload = ctx.createDataFrame(getInsertAllData(lploadAllData, argDay, broadcastBrandMaps))
      .toDF("user_id", "plan_id", "unit_id", "impression", "click", "trace_type", "target_type", "target_value", "dstart", "dfinish", "dpkgadded", "date", "inst_hijack")
      .repartition(50)

    insertDataFramelpload.show(10)

    insertDataFramelpload
      .write
      .mode(SaveMode.Append)
      .jdbc(mariaReportdbUrl, "report.report_apk_down_target", mariaReportdbProp)
    println("insertDataFramelpload over!")
    //
    //
    //    //-----
    var insertDataFrameSite = ctx.createDataFrame(getInsertAllData(siteAllData, argDay, broadcastBrandMaps))
      .toDF("user_id", "plan_id", "unit_id", "impression", "click", "trace_type", "target_type", "target_value", "dstart", "dfinish", "dpkgadded", "date", "inst_hijack")
      .repartition(50)

    insertDataFrameSite.show(10)

    insertDataFrameSite
      .write
      .mode(SaveMode.Append)
      .jdbc(mariaReportdbUrl, "report.report_apk_down_target", mariaReportdbProp)
    println("insertDataFrameSite over!")
    //
    //
    //-----
    var insertDataFrameallDatax = ctx.createDataFrame(getInsertAllData(allDatax, argDay, broadcastBrandMaps))
      .toDF("user_id", "plan_id", "unit_id", "impression", "click", "trace_type", "target_type", "target_value", "dstart", "dfinish", "dpkgadded", "date", "inst_hijack")
      .repartition(50)

    insertDataFrameallDatax.show(10)

    insertDataFrameallDatax
      .write
      .mode(SaveMode.Append)
      .jdbc(mariaReportdbUrl, "report.report_apk_down_target", mariaReportdbProp)
    println("insertDataFrameallDatax over!")


    var insertDataFrameMotivation = ctx.createDataFrame(getInsertAllData(motivationAllData, argDay, broadcastBrandMaps))
      .toDF("user_id", "plan_id", "unit_id", "impression", "click", "trace_type", "target_type", "target_value", "dstart", "dfinish", "dpkgadded", "date", "inst_hijack")
      .repartition(50)

    insertDataFrameMotivation.show(10)

    insertDataFrameMotivation
      .write
      .mode(SaveMode.Append)
      .jdbc(mariaReportdbUrl, "report.report_apk_down_target", mariaReportdbProp)
    println("insertDataFrameMotivation over!")

    println("report over!")


  }

  def getInsertAllData(allData: RDD[Info], argDay: String, broadcastBrandMaps: Broadcast[Map[String, Int]]) = {
    val inputStudentData = allData
      .map {
        x =>
          val info = x
          val userid = x.userid
          val planid = info.planid
          var unitid = info.unitid
          val isshow = info.isshow
          val isclick = info.isclick
          val typeVal = info.isStudent
          val start = info.start
          val finish = info.finish
          val pkgadded = info.pkgadded
          val traceType = info.traceType
          val instHijack = info.instHijack
          ((unitid, traceType, typeVal), (userid, planid, unitid, isshow, isclick, traceType, typeVal, start, finish, pkgadded, instHijack))
      }
    val studentData = getTargetData(inputStudentData, "student", argDay)
    //println("studentData count is", studentData.count())
    var insertAllData = studentData

    val inputBrandData = allData
      .repartition(50)
      .filter {
        x =>
          val mediaId = x.mediaid
          val adslotType = x.adslotType
          ((mediaId == 80000001) || (mediaId == 80000002)) && (adslotType == 1)
      }
      .map {
        x =>
          val info = x
          val userid = x.userid
          val planid = info.planid
          var unitid = info.unitid
          val isshow = info.isshow
          val isclick = info.isclick
          val typeVal = if (broadcastBrandMaps.value.contains(info.brand.toLowerCase)) broadcastBrandMaps.value(info.brand.toLowerCase) else 0
          val start = info.start
          val finish = info.finish
          val pkgadded = info.pkgadded
          val traceType = info.traceType
          val instHijack = info.instHijack
          ((unitid, traceType, typeVal), (userid, planid, unitid, isshow, isclick, traceType, typeVal, start, finish, pkgadded, instHijack))
      }

    val brandData = getTargetData(inputBrandData, "brand", argDay)
    //println("brandData count is", brandData.count())
    insertAllData = insertAllData.union(brandData)


    val inputBrowserTypeData = allData
      //.repartition(50)
      .filter {
      x =>
        val mediaId = x.mediaid
        var ok = true
        mediaId match {
          case 80000001 => ok = false
          case 80000002 => ok = false
          case 80000006 => ok = false
          case 800000062 => ok = false
          case 80000064 => ok = false
          case 80000066 => ok = false
          case 80000141 => ok = false
          case _ =>
        }
        ok
    }
      .map {
        x =>
          val info = x
          val userid = x.userid
          val planid = info.planid
          var unitid = info.unitid
          val isshow = info.isshow
          val isclick = info.isclick
          val typeVal = info.browserType
          val start = info.start
          val finish = info.finish
          val pkgadded = info.pkgadded
          val traceType = info.traceType
          val instHijack = info.instHijack
          ((unitid, traceType, typeVal), (userid, planid, unitid, isshow, isclick, traceType, typeVal, start, finish, pkgadded, instHijack))
      }

    val browserTypeData = getTargetData(inputBrowserTypeData, "browser_type", argDay)
    //println("browserTypeData count is", browserTypeData.count())
    insertAllData = insertAllData.union(browserTypeData)

    val inputAdslotIdData = allData
      //.repartition(50)
      .map {
      x =>
        val info = x
        val userid = x.userid
        val planid = info.planid
        var unitid = info.unitid
        val isshow = info.isshow
        val isclick = info.isclick
        val typeVal = info.adslotid
        val start = info.start
        val finish = info.finish
        val pkgadded = info.pkgadded
        val traceType = info.traceType
        val instHijack = info.instHijack
        ((unitid, traceType, typeVal), (userid, planid, unitid, isshow, isclick, traceType, typeVal, start, finish, pkgadded, instHijack))
    }
    val adslotIdData = getTargetData(inputAdslotIdData, "adslot_id", argDay)
    //println("adslotIdData count is", adslotIdData.count())
    insertAllData = insertAllData.union(adslotIdData)

    val inputSexData = allData
      //.repartition(50)
      .map {
      x =>
        val info = x
        val userid = x.userid
        val planid = info.planid
        var unitid = info.unitid
        val isshow = info.isshow
        val isclick = info.isclick
        val typeVal = info.sex
        val start = info.start
        val finish = info.finish
        val pkgadded = info.pkgadded
        val traceType = info.traceType
        val instHijack = info.instHijack
        ((unitid, traceType, typeVal), (userid, planid, unitid, isshow, isclick, traceType, typeVal, start, finish, pkgadded, instHijack))
    }
    val sexData = getTargetData(inputSexData, "sex", argDay)
    //println("sex count is", sexData.count())
    insertAllData = insertAllData.union(sexData)

    val inputAgeData = allData
      //.repartition(50)
      .map {
      x =>
        val info = x
        val userid = x.userid
        val planid = info.planid
        var unitid = info.unitid
        val isshow = info.isshow
        val isclick = info.isclick
        val typeVal = info.age
        val start = info.start
        val finish = info.finish
        val pkgadded = info.pkgadded
        val traceType = info.traceType
        val instHijack = info.instHijack
        ((unitid, traceType, typeVal), (userid, planid, unitid, isshow, isclick, traceType, typeVal, start, finish, pkgadded, instHijack))
    }
    val ageData = getTargetData(inputAgeData, "age", argDay)
    //println("ageData count is", ageData.count())

    insertAllData = insertAllData.union(ageData)

    val inputOsData = allData
      //.repartition(50)
      .map {
      x =>
        val info = x
        val userid = x.userid
        val planid = info.planid
        var unitid = info.unitid
        val isshow = info.isshow
        val isclick = info.isclick
        val typeVal = info.os
        val start = info.start
        val finish = info.finish
        val pkgadded = info.pkgadded
        val traceType = info.traceType
        val instHijack = info.instHijack
        ((unitid, traceType, typeVal), (userid, planid, unitid, isshow, isclick, traceType, typeVal, start, finish, pkgadded, instHijack))
    }
    val osData = getTargetData(inputOsData, "os", argDay)
    //println("osData count is", osData.count())

    insertAllData = insertAllData.union(osData)

    val inputProvinceData = allData
      //.repartition(50)
      .map {
      x =>
        val info = x
        val userid = x.userid
        val planid = info.planid
        var unitid = info.unitid
        val isshow = info.isshow
        val isclick = info.isclick
        val typeVal = info.province
        val start = info.start
        val finish = info.finish
        val pkgadded = info.pkgadded
        val traceType = info.traceType
        val instHijack = info.instHijack
        ((unitid, traceType, typeVal), (userid, planid, unitid, isshow, isclick, traceType, typeVal, start, finish, pkgadded, info.instHijack))
    }
    val provinceData = getTargetData(inputProvinceData, "province", argDay)
    //println("provinceData count is", provinceData.count())
    insertAllData = insertAllData.union(provinceData)

    val inputPhoneLevelData = allData
      //.repartition(50)
      .map {
      x =>
        val info = x
        val userid = x.userid
        val planid = info.planid
        var unitid = info.unitid
        val isshow = info.isshow
        val isclick = info.isclick
        val typeVal = info.phoneLevel
        val start = info.start
        val finish = info.finish
        val pkgadded = info.pkgadded
        val traceType = info.traceType
        val instHijack = info.instHijack
        ((unitid, traceType, typeVal), (userid, planid, unitid, isshow, isclick, traceType, typeVal, start, finish, pkgadded, info.instHijack))
    }

    val phoneLevelData = getTargetData(inputPhoneLevelData, "phone_level", argDay)
    //println("phoneLevelData count is", phoneLevelData.count())
    insertAllData = insertAllData.union(phoneLevelData)


    val inputHourData = allData
      //.repartition(50)
      .map {
      x =>
        val info = x
        val userid = x.userid
        val planid = info.planid
        var unitid = info.unitid
        val isshow = info.isshow
        val isclick = info.isclick
        val typeVal = info.hour
        val start = info.start
        val finish = info.finish
        val pkgadded = info.pkgadded
        val traceType = info.traceType
        val instHijack = info.instHijack
        ((unitid, traceType, typeVal), (userid, planid, unitid, isshow, isclick, traceType, typeVal, start, finish, pkgadded, instHijack))
    }

    val hourData = getTargetData(inputHourData, "hour", argDay)
    //println("hourData count is", hourData.count())
    insertAllData = insertAllData.union(hourData)


    val inputNetworkData = allData
      //.repartition(50)
      .map {
      x =>
        val info = x
        val userid = x.userid
        val planid = info.planid
        var unitid = info.unitid
        val isshow = info.isshow
        val isclick = info.isclick
        val typeVal = info.network
        val start = info.start
        val finish = info.finish
        val pkgadded = info.pkgadded
        val traceType = info.traceType
        val instHijack = info.instHijack
        ((unitid, traceType, typeVal), (userid, planid, unitid, isshow, isclick, traceType, typeVal, start, finish, pkgadded, instHijack))
    }

    val networkData = getTargetData(inputNetworkData, "network_type", argDay).cache()
    //println("networkData count is", networkData.count())
    insertAllData = insertAllData.union(networkData)


    val inputUserLevelData = allData
      //.repartition(50)
      .map {
      x =>
        val info = x
        val userid = x.userid
        val planid = info.planid
        var unitid = info.unitid
        val isshow = info.isshow
        val isclick = info.isclick
        val typeVal = info.userLevel
        val start = info.start
        val finish = info.finish
        val pkgadded = info.pkgadded
        val traceType = info.traceType
        val instHijack = info.instHijack
        ((unitid, traceType, typeVal), (userid, planid, unitid, isshow, isclick, traceType, typeVal, start, finish, pkgadded, instHijack))
    }

    val userLevelData = getTargetData(inputUserLevelData, "user_level", argDay)
    //println("userLevelData count is", userLevelData.count())
    insertAllData = insertAllData.union(userLevelData)

    val inputQukanNewUserData = allData
      //.repartition(50)
      .map {
      x =>
        val info = x
        val userid = x.userid
        val planid = info.planid
        var unitid = info.unitid
        val isshow = info.isshow
        val isclick = info.isclick
        val typeVal = info.qukanNewUser
        val start = info.start
        val finish = info.finish
        val pkgadded = info.pkgadded
        val traceType = info.traceType
        val instHijack = info.instHijack
        ((unitid, traceType, typeVal), (userid, planid, unitid, isshow, isclick, traceType, typeVal, start, finish, pkgadded, instHijack))
    }

    val qukanNewUserData = getTargetData(inputQukanNewUserData, "user_orient", argDay)
    //println("qukanNewUserData count is", qukanNewUserData.count())
    insertAllData = insertAllData.union(qukanNewUserData)

    val inputAdslotTypeData = allData
      //.repartition(50)
      .map {
      x =>
        val info = x
        val userid = x.userid
        val planid = info.planid
        var unitid = info.unitid
        val isshow = info.isshow
        val isclick = info.isclick
        val typeVal = info.adslotType
        val start = info.start
        val finish = info.finish
        val pkgadded = info.pkgadded
        val traceType = info.traceType
        val instHijack = info.instHijack
        ((unitid, traceType, typeVal), (userid, planid, unitid, isshow, isclick, traceType, typeVal, start, finish, pkgadded, instHijack))
    }

    val adslotTypeData = getTargetData(inputAdslotTypeData, "adslot_type", argDay)
    //println("adslotTypeData count is", adslotTypeData.count())
    insertAllData = insertAllData.union(adslotTypeData)

    val inputQuAdslotTypeData = allData
      //.repartition(50)
      .filter {
      x =>
        val mediaId = x.mediaid
        (mediaId == 80000001) || (mediaId == 80000002)
    }
      .map {
        x =>
          val info = x
          val userid = x.userid
          val planid = info.planid
          var unitid = info.unitid
          val isshow = info.isshow
          val isclick = info.isclick
          val typeVal = info.adslotType
          val start = info.start
          val finish = info.finish
          val pkgadded = info.pkgadded
          val traceType = info.traceType
          val instHijack = info.instHijack
          ((unitid, traceType, typeVal), (userid, planid, unitid, isshow, isclick, traceType, typeVal, start, finish, pkgadded, instHijack))
      }
    val quAdslotTypeData = getTargetData(inputQuAdslotTypeData, "adslot_type_media", argDay)
    //println("quAdslotTypeData count is", quAdslotTypeData.count())

    insertAllData.union(quAdslotTypeData).repartition(50)
  }

  def getTargetData(data: RDD[((Int, String, Int), (Int, Int, Int, Long, Long, String, Int, Long, Long, Long, Long))],
                    targetType: String, argDay: String): (RDD[(Int, Int, Int, Long, Long, String, String, Int, Long, Long, Long, String, Long)]) = {
    data
      .reduceByKey {
        (a, b) =>
          val userid = a._1
          val planid = a._2
          val unitid = a._3
          val isshow = a._4 + b._4
          val isclick = a._5 + b._5
          val traceType = a._6
          val typeVal = a._7
          val start = a._8 + b._8
          val finish = a._9 + b._9
          val pkgadded = a._10 + b._10
          val instHijack = a._11 + b._11
          (userid, planid, unitid, isshow, isclick, traceType, typeVal, start, finish, pkgadded, instHijack)
      }
      .map {
        x =>
          val userid = x._2._1
          val planid = x._2._2
          val unitid = x._2._3
          val isshow = x._2._4
          val isclick = x._2._5
          val traceType = x._2._6
          val typeVal = x._2._7
          val start = x._2._8
          val finish = x._2._9
          val pkgadded = x._2._10
          val instHijack = x._2._11
          (userid, planid, unitid, isshow, isclick, traceType, targetType, typeVal, start, finish, pkgadded, argDay, instHijack)
      }
      .repartition(50)
  }

  def clearReportApkDownTarget(date: String): Unit = {
    try {
      Class.forName(mariaReportdbProp.getProperty("driver"))
      val conn = DriverManager.getConnection(
        mariaReportdbUrl,
        mariaReportdbProp.getProperty("user"),
        mariaReportdbProp.getProperty("password"))
      val stmt = conn.createStatement()
      val sql =
        """
          |delete from report.report_apk_down_target where `date` = "%s"
        """.stripMargin.format(date)
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

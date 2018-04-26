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
object InsertReportSiteBuildingTarget {

  var mariaAdvdbUrl = ""
  val mariaAdvdbProp = new Properties()

  var mariaReportdbUrl = ""
  val mariaReportdbProp = new Properties()

  case class UnionLogInfo(
                           searchid: String = "",
                           userid: Int = 0,
                           unitid: Int = 0,
                           ideaid: Int = 0,
                           isshow: Int = 0,
                           isclick: Int = 0,
                           trace_type: String,
                           total: Int,
                           siteid: Int = 0,
                           price: Int = 0) {

  }

  case class Info(
                   siteId: Int = 0,
                   ideaid: Int = 0,
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
                   load: Long = 0,
                   active: Long = 0
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
      .appName("InsertReportSiteBuildingTarget is run day is %s".format(argDay))
      .enableHiveSupport()
      .getOrCreate()

    println("InsertReportSiteBuildingTarget is run day is %s".format(argDay))

    var ideaData = ctx.read.jdbc(mariaAdvdbUrl,
      """
        |(
        | SELECT DISTINCT(c.idea_id),i.clk_site_id,i.user_id
        | FROM cost c
        | INNER JOIN idea i ON i.id=c.idea_id
        | WHERE c.date="%s" AND i.clk_site_id>0
        |) xidea
      """.stripMargin.format(argDay), mariaAdvdbProp)
      .rdd
      .map(
        x =>
          (x.get(0), x.get(1), x.get(2))
      )
      .map {
        x =>
          val ideaid = x._1.toString.toInt
          val siteid = x._2.toString.toInt
          val userid = x._3.toString.toInt
          (UnionLogInfo("", userid, 0, ideaid, 0, 0, "", 0, siteid))
      }
      .cache()
    println("ideaData count", ideaData.count())

    var ideaMaps: Map[Int, UnionLogInfo] = Map()
    ideaData
      .map {
        x =>
          (x.ideaid, x)
      }
      .take(ideaData.count().toInt)
      .foreach {
        x =>
          ideaMaps += (x._1 -> x._2)
      }

    val broadcastIdeaMaps = ctx.sparkContext.broadcast(ideaMaps)


    val unionData = ctx
      .sql(
        """
          |SELECT searchid,ideaid,isshow,isclick,sex,age,os,province,ext['phone_level'].int_value,hour,
          |network,coin,ext['qukan_new_user'].int_value,adslot_type,media_appsid
          |FROM dl_cpc.cpc_union_log
          |WHERE date="%s" AND (isshow+isclick)>0
        """.stripMargin.format(argDay))
      .rdd
      .map {
        x =>
          val searchid = x.getString(0)
          val ideaid = x.getInt(1)
          val isshow = if (x.getInt(3) > 0) 1 else x.get(2).toString.toLong
          val isclick = x.get(3).toString.toLong
          val sex = x.getInt(4)
          val age = x.getInt(5)
          val os = x.getInt(6)
          val province = x.getInt(7)
          val phoneLevel = x.getInt(8)
          val hour = x.getString(9).toInt
          val network = x.getInt(10)
          val coin = x.getInt(11)
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

          val qukanNewUser = x.getInt(12)
          val adslotType = x.getInt(13)
          val mediaId = x.getString(14).toInt

          val siteid = 0

          (searchid, (Info(siteid, ideaid, isshow, isclick, sex, age, os, province, phoneLevel, hour,
            network, userLevel, qukanNewUser, adslotType, mediaId, 0, 0)))
      }
      .cache()
    println("unionData count", unionData.count())

    val traceData = ctx.sql(
      """
        |SELECT DISTINCT cutl.searchid,cutl.trace_type,cutl.duration
        |FROM dl_cpc.cpc_union_trace_log cutl
        |INNER JOIN dl_cpc.cpc_union_log cul ON cutl.searchid=cul.searchid
        |WHERE cutl.date="%s" AND cul.date="%s" AND cul.isclick>0
      """.stripMargin.format(argDay, argDay))
      .rdd
      .map {
        x =>
          val searchid = x.getString(0)
          val trace_type = x.getString(1)
          var load = 0
          var active = 0
          trace_type match {
            case "load" => load += 1
            case s if s.startsWith("active") => active += 1
            case "disactive" => active -= 1
            //case "press" => active += 1
            case _ =>
          }
          (searchid, (Info(-1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, load, active)))
      }
      .cache()
    println("traceData count", traceData.count())


    val allData = unionData
      .union(traceData)
      .reduceByKey {
        (a, b) =>
          val siteId = if (a.siteId != -1) a.siteId else b.siteId
          val ideaid = if (a.siteId != -1) a.ideaid else b.ideaid
          val isshow = if (a.siteId != -1) a.isshow else b.isshow
          val isclick = if (a.siteId != -1) a.isclick else b.isclick
          val sex = if (a.siteId != -1) a.sex else b.sex
          val age = if (a.siteId != -1) a.age else b.age
          val os = if (a.siteId != -1) a.os else b.os
          val province = if (a.siteId != -1) a.province else b.province
          val phoneLevel = if (a.siteId != -1) a.phoneLevel else b.phoneLevel
          val hour = if (a.siteId != -1) a.hour else b.hour
          val network = if (a.siteId != -1) a.network else b.network
          val userLevel = if (a.siteId != -1) a.userLevel else b.userLevel
          val qukanNewUser = if (a.siteId != -1) a.qukanNewUser else b.qukanNewUser
          val adslotType = if (a.siteId != -1) a.adslotType else b.adslotType
          val mediaid = if (a.siteId != -1) a.mediaid else b.mediaid
          val load = a.load + b.load
          val active = a.active + b.active

          Info(siteId, ideaid, isshow, isclick, sex, age, os, province, phoneLevel, hour, network, userLevel, qukanNewUser, adslotType,
            mediaid, load, active)
      }
      .filter(_._2.ideaid > 0)
      .filter {
        x =>
          broadcastIdeaMaps.value.contains(x._2.ideaid)
      }
      .map {
        x =>
          val info = x._2
          val siteId = broadcastIdeaMaps.value(info.ideaid).siteid
          (0, Info(siteId, info.ideaid, info.isshow, info.isclick, info.sex, info.age, info.os, info.province, info.phoneLevel, info.hour,
            info.network, info.userLevel, info.qukanNewUser, info.adslotType, info.mediaid, info.load, info.active))
      }
      //.filter(_._2.siteId == 1797)
      .cache()

    val inputSexData = allData
      .map {
        x =>
          val info = x._2
          val siteId = info.siteId
          val isshow = info.isshow
          val isclick = info.isclick
          val typeVal = info.sex
          val load = info.load
          val active = info.active
          ((siteId, typeVal), (siteId, isshow, isclick, typeVal, load, active))
      }
    val sexData = getTargetData(inputSexData, "sex", argDay).cache()
    println("sexData count is", sexData.count())


    val inputAgeData = allData
      .map {
        x =>
          val info = x._2
          val siteId = info.siteId
          val isshow = info.isshow
          val isclick = info.isclick
          val typeVal = info.age
          val load = info.load
          val active = info.active
          ((siteId, typeVal), (siteId, isshow, isclick, typeVal, load, active))
      }
    val ageData = getTargetData(inputAgeData, "age", argDay).cache()
    println("age count is", ageData.count())

    val inputOsData = allData
      .map {
        x =>
          val info = x._2
          val siteId = info.siteId
          val isshow = info.isshow
          val isclick = info.isclick
          val typeVal = info.os
          val load = info.load
          val active = info.active
          ((siteId, typeVal), (siteId, isshow, isclick, typeVal, load, active))
      }
    val osData = getTargetData(inputOsData, "os", argDay).cache()
    println("os count is", osData.count())

    val inputProvinceData = allData
      .map {
        x =>
          val info = x._2
          val siteId = info.siteId
          val isshow = info.isshow
          val isclick = info.isclick
          val typeVal = info.province
          val load = info.load
          val active = info.active
          ((siteId, typeVal), (siteId, isshow, isclick, typeVal, load, active))
      }
    val provinceData = getTargetData(inputProvinceData, "province", argDay).cache()
    println("province count is", provinceData.count())

    val inputPhoneLevelData = allData
      .map {
        x =>
          val info = x._2
          val siteId = info.siteId
          val isshow = info.isshow
          val isclick = info.isclick
          val typeVal = info.phoneLevel
          val load = info.load
          val active = info.active
          ((siteId, typeVal), (siteId, isshow, isclick, typeVal, load, active))
      }
    val phoneLevelData = getTargetData(inputPhoneLevelData, "phone_level", argDay).cache()
    println("phone_level count is", phoneLevelData.count())

    val inputHourData = allData
      .map {
        x =>
          val info = x._2
          val siteId = info.siteId
          val isshow = info.isshow
          val isclick = info.isclick
          val typeVal = info.hour
          val load = info.load
          val active = info.active
          ((siteId, typeVal), (siteId, isshow, isclick, typeVal, load, active))
      }
    val hourData = getTargetData(inputHourData, "hour", argDay).cache()
    println("hour count is", hourData.count())

    val inputNetworkData = allData
      .map {
        x =>
          val info = x._2
          val siteId = info.siteId
          val isshow = info.isshow
          val isclick = info.isclick
          val typeVal = info.network
          val load = info.load
          val active = info.active
          ((siteId, typeVal), (siteId, isshow, isclick, typeVal, load, active))
      }
    val networkData = getTargetData(inputNetworkData, "network_type", argDay).cache()
    println("network_type count is", networkData.count())

    val inputUserLevelData = allData
      .map {
        x =>
          val info = x._2
          val siteId = info.siteId
          val isshow = info.isshow
          val isclick = info.isclick
          val typeVal = info.userLevel
          val load = info.load
          val active = info.active
          ((siteId, typeVal), (siteId, isshow, isclick, typeVal, load, active))
      }
    val userLevelData = getTargetData(inputUserLevelData, "user_level", argDay).cache()
    println("user_level count is", userLevelData.count())

    val inputQukanNewUserData = allData
      .map {
        x =>
          val info = x._2
          val siteId = info.siteId
          val isshow = info.isshow
          val isclick = info.isclick
          val typeVal = info.qukanNewUser
          val load = info.load
          val active = info.active
          ((siteId, typeVal), (siteId, isshow, isclick, typeVal, load, active))
      }
    val qukanNewUserData = getTargetData(inputQukanNewUserData, "user_orient", argDay).cache()
    println("user_orient count is", qukanNewUserData.count())

    val inputAdslotTypeData = allData
      .map {
        x =>
          val info = x._2
          val siteId = info.siteId
          val isshow = info.isshow
          val isclick = info.isclick
          val typeVal = info.adslotType
          val load = info.load
          val active = info.active
          ((siteId, typeVal), (siteId, isshow, isclick, typeVal, load, active))
      }
    val adslotTypeData = getTargetData(inputAdslotTypeData, "adslot_type", argDay).cache()
    println("adslot_type count is", adslotTypeData.count())

    val inputQuAdslotTypeData = allData
      .filter {
        x =>
          val mediaId = x._2.mediaid
          (mediaId == 80000001) || (mediaId == 80000002)
      }
      .map {
        x =>
          val info = x._2
          val siteId = info.siteId
          val isshow = info.isshow
          val isclick = info.isclick
          val typeVal = info.adslotType
          val load = info.load
          val active = info.active
          ((siteId, typeVal), (siteId, isshow, isclick, typeVal, load, active))
      }

    val quAdslotTypeData = getTargetData(inputQuAdslotTypeData, "adslot_type_media", argDay).cache()
    println("adslot_type_media count is", quAdslotTypeData.count())


    val insertAllData = sexData
      .union(ageData)
      .union(osData)
      .union(provinceData)
      .union(phoneLevelData)
      .union(hourData)
      .union(networkData)
      .union(userLevelData)
      .union(qukanNewUserData)
      .union(adslotTypeData)
      .union(quAdslotTypeData)

    var insertDataFrame = ctx.createDataFrame(insertAllData)
      .toDF("site_id", "impression", "click", "target_type", "target_value", "load", "active", "date")
    insertDataFrame.show(100)

    clearReportSiteBuildingTarget(argDay)

    insertDataFrame
      .write
      .mode(SaveMode.Append)
      .jdbc(mariaReportdbUrl, "report.report_site_building_target", mariaReportdbProp)
  }

  def getTargetData(data: RDD[((Int, Int), (Int, Long, Long, Int, Long, Long))],
                    target_type: String, argDay: String): (RDD[(Int, Long, Long, String, Int, Long, Long, String)]) = {
    data
      .reduceByKey {
        (a, b) =>
          val siteId = a._1
          val isshow = a._2 + b._2
          val isclick = a._3 + b._3
          val typeVal = a._4
          val load = a._5 + b._5
          val active = a._6 + b._6
          (siteId, isshow, isclick, typeVal, load, active)
      }
      .map {
        x =>
          val siteId = x._2._1
          val isshow = x._2._2
          val isclick = x._2._3
          val typeVal = x._2._4
          val load = x._2._5
          val active = x._2._6
          val targetType = target_type
          (siteId, isshow, isclick, targetType, typeVal, load, active, argDay)
      }
  }

  def clearReportSiteBuildingTarget(date: String): Unit = {
    try {
      Class.forName(mariaReportdbProp.getProperty("driver"))
      val conn = DriverManager.getConnection(
        mariaReportdbUrl,
        mariaReportdbProp.getProperty("user"),
        mariaReportdbProp.getProperty("password"))
      val stmt = conn.createStatement()
      val sql =
        """
          |delete from report.report_site_building_target where `date` = "%s"
        """.stripMargin.format(date)
      stmt.executeUpdate(sql);
    } catch {
      case e: Exception => println("exception caught: " + e);
    }
  }
}

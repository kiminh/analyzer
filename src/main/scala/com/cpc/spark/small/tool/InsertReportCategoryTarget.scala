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
object InsertReportCategoryTarget {

  case class UnionLogInfo(
                           searchid: String = "",
                           mediaid: String = "",
                           adslotid: String = "",
                           adslot_type: Int = 0,
                           isshow: Int = 0,
                           isclick: Int = 0,
                           sex: Int = 0,
                           age: Int = 0,
                           os: Int = 0,
                           province: Int = 0,
                           phone_level: Int = 0,
                           hour: Int = 0,
                           adclass: Int = 0,
                           req: Int = 0,
                           isfull: Int = 0,
                           price: Int = 0,
                           network: Int = 0,
                           //coin: Int = 0,
                           user_level: Int = 0,
                           city_level: Int = 0,
                           qu_adslot_type: Int = 0,
                           ext_adslot_type: Int = 0,
                           load: Int = 0,
                           active: Int = 0,
                           isStudent:Int=0//0未知，1学生，2非学生
                         ){

  }

  var mariadbUrl = ""
  val mariadbProp = new Properties()

  def main(args: Array[String]): Unit = {
    Logger.getRootLogger.setLevel(Level.WARN)
    val argDay = args(0).toString

    val conf = ConfigFactory.load()
    mariadbUrl = conf.getString("mariadb.url")
    mariadbProp.put("user", conf.getString("mariadb.user"))
    mariadbProp.put("password", conf.getString("mariadb.password"))
    mariadbProp.put("driver", conf.getString("mariadb.driver"))

    val ctx = SparkSession.builder().appName("InsertReportCategoryTarget is run day is %s".format(argDay)).enableHiveSupport().getOrCreate()

    println("InsertReportCategoryTarget is run day is %s".format(argDay))

    val quMedia = Array("80000001", "80000002", "80000006", "800000062", "80000064", "80000066", "80000141")
    val unionLogData = ctx
      .sql(
        """
          |SELECT searchid,media_appsid,adslot_id,adslot_type,isshow,isclick,sex,age,os,province,phone_level,
          |hour,adclass,isfill,price,network,coin,qukan_new_user,city_level,
          |interests
          |FROM dl_cpc.cpc_basedata_union_events
          |WHERE day="%s" AND adclass>0
        """.stripMargin.format(argDay))
      .rdd
      .map {
        x =>
          val searchid = x.getAs[String](0)
          val mediaid = x.getAs[String](1)
          val adslotid = x.getAs[String](2)
          val adslot_type = x.getAs[Int](3)
          val isshow = x.getAs[Int](4)
          val isclick = x.getAs[Int](5)
          val sex = x.getAs[Int](6)
          val age = x.getAs[Int](7)
          val os = x.getAs[Int](8)
          val province = x.getAs[Int](9)
          val phone_level = x.getAs[Int](10)
          val hour = x.getAs[String](11).toInt
          val adclass = x.getAs[Int](12)
          val req = 1
          val isfull = x.getAs[Int](13)
          val price = if (isclick > 0) x.getAs[Int](14) else 0

          val load = 0
          val active = 0

          val network = x.getAs[Int](15)
          val coin = x.getAs[Int](16)
          //coin
          var user_level = 0
          if (coin == 0) {
            user_level = 1
          } else if (coin <= 60) {
            user_level = 2
          } else if (coin <= 90) {
            user_level = 3
          } else {
            user_level = 4
          }

          val qukan_new_user = x.getAs[Int](17)
          val city_level = x.getAs[Int](18)
          val isStudent = 0

          val mtype = if (quMedia.filter(_ == mediaid).length > 0) 1 else 0
          val qu_adslot_type = if (mtype == 1) adslot_type else 0
          val ext_adslot_type = if (mtype == 0) adslot_type else 0
          val info = UnionLogInfo(searchid, mediaid, adslotid, adslot_type, isshow, isclick, sex, age, os, province, phone_level,
            hour, adclass, req, isfull, price, network, user_level, city_level, qu_adslot_type, ext_adslot_type, load, active,isStudent)
          (info.searchid, (info))
      }
      .repartition(50)
      .cache()
    println("unionLogData count", unionLogData.count())

    val traceData = ctx.sql(
      """
        |SELECT DISTINCT cutl.searchid,cutl.trace_type
        |FROM dl_cpc.cpc_basedata_trace_event cutl
        |LEFT JOIN dl_cpc.cpc_basedata_union_events cul ON cul.searchid=cutl.searchid
        |WHERE cutl.day="%s" AND cul.day="%s" AND cul.adclass>0 AND cul.isclick>0
        |AND cutl.trace_type IN("load","active1","active2","active3","active4","active5","disactive")
      """.stripMargin.format(argDay,argDay))
      .rdd
      .map {
        x =>
          val searchid = x.getAs[String](0)
          val trace_type = x.getAs[String](1)
          var load = 0
          var active = 0

          trace_type match {
            case "load" => load += 1
            case s if s.startsWith("active") => active += 1
            case "disactive" => active -= 1
            //            //case "press" => active += 1
            case _ =>
          }

          val info = UnionLogInfo(searchid, "", "", -1, 0, 0, -1, -1, -1, -1, -1, -1, -1, 0, 0, 0, -1, -1, -1, -1, -1, load, active)
          (info.searchid, (info))
      }
      .repartition(50)
      .cache()
    println("traceData count", traceData.count())

    val allData = unionLogData
      .union(traceData)
      .reduceByKey {
        (a, b) =>
          val mediaid = if (a.mediaid.length > 0) a.mediaid else b.mediaid
          val adslotid = if (a.adslotid.length > 0) a.adslotid else b.adslotid
          val adslot_type = if (a.adslot_type > 0) a.adslot_type else b.adslot_type
          val isshow = a.isshow + b.isshow
          val isclick = a.isclick + b.isclick
          val sex = if (a.sex != -1) a.sex else b.sex
          val age = if (a.age != -1) a.age else b.age
          val os = if (a.os != -1) a.os else b.os
          val province = if (a.province != -1) a.province else b.province
          val phone_level = if (a.phone_level != -1) a.phone_level else b.phone_level
          val hour = if (a.hour != -1) a.hour else b.hour
          val adclass = if (a.adclass != -1) a.adclass else b.adclass
          val req = a.req + b.req
          val isfull = a.isfull + b.isfull
          val price = a.price + b.price
          val network = if (a.network != -1) a.network else b.network
          val user_level = if (a.user_level != -1) a.user_level else b.user_level
          val city_level = if (a.city_level != -1) a.city_level else b.city_level
          val qu_adslot_type = if (a.qu_adslot_type != -1) a.qu_adslot_type else b.qu_adslot_type
          val ext_adslot_type = if (a.ext_adslot_type != -1) a.ext_adslot_type else b.ext_adslot_type
          val load = a.load + b.load
          val active = a.active + b.active
          val isStudent = if(a.mediaid.length > 0) a.isStudent else b.isStudent

          val info = UnionLogInfo(a.searchid, mediaid, adslotid, adslot_type, isshow, isclick, sex, age, os, province, phone_level,
            hour, adclass, req, isfull, price, network, user_level, city_level, qu_adslot_type, ext_adslot_type, load, active,isStudent)
          info
      }
      .filter {
        x =>
          x._2.mediaid.length > 0
      }
      .repartition(50)
      .cache()
    println("allData count", allData.count())

    val inputStudentData = allData
      .map {
        x =>
          val adclass = x._2.adclass
          val isshow = x._2.isshow
          val isclick = x._2.isclick
          val target_value = x._2.isStudent
          val load = x._2.load
          val active = x._2.active
          val req = x._2.req
          val isfull = x._2.isfull
          val price = x._2.price
          ("%d-%d".format(adclass, target_value), (adclass, isshow, isclick, target_value, load, active, req, isfull, price))
      }
    val studentData = getTargetData(inputStudentData, argDay, "student")
    //println("studentData count is", studentData.count())
    var insertData = studentData

    val inputMediaData = allData
      .map {
        x =>
          val adclass = x._2.adclass
          val isshow = x._2.isshow
          val isclick = x._2.isclick
          val target_value = x._2.mediaid.toInt
          val load = x._2.load
          val active = x._2.active
          val req = x._2.req
          val isfull = x._2.isfull
          val price = x._2.price
          ("%d-%d".format(adclass, target_value), (adclass, isshow, isclick, target_value, load, active, req, isfull, price))
      }
    val mediaData = getTargetData(inputMediaData, argDay, "media")
    //println("mediaData count is", mediaData.count())
    insertData = insertData.union(mediaData)

    val inputAdslotData = allData
      .map {
        x =>
          val adclass = x._2.adclass
          val isshow = x._2.isshow
          val isclick = x._2.isclick
          val target_value = x._2.adslotid.toInt
          val load = x._2.load
          val active = x._2.active
          val req = x._2.req
          val isfull = x._2.isfull
          val price = x._2.price
          ("%d-%d".format(adclass, target_value), (adclass, isshow, isclick, target_value, load, active, req, isfull, price))
      }
    val adslotData = getTargetData(inputAdslotData, argDay, "adslot")
    //println("adslotData count is", adslotData.count())
    insertData = insertData.union(adslotData).repartition(50)
    println("1",insertData.count())

    val inputAdslotTypeData = allData
      .map {
        x =>
          val adclass = x._2.adclass
          val isshow = x._2.isshow
          val isclick = x._2.isclick
          val target_value = x._2.adslot_type
          val load = x._2.load
          val active = x._2.active
          val req = x._2.req
          val isfull = x._2.isfull
          val price = x._2.price
          ("%d-%d".format(adclass, target_value), (adclass, isshow, isclick, target_value, load, active, req, isfull, price))
      }
    val adslotTypeData = getTargetData(inputAdslotTypeData, argDay, "adslot_type")
    //println("adslotTypeData count is", adslotTypeData.count())
    insertData = insertData.union(adslotTypeData)

    val inputSexData = allData
      .map {
        x =>
          val adclass = x._2.adclass
          val isshow = x._2.isshow
          val isclick = x._2.isclick
          val target_value = x._2.sex
          val load = x._2.load
          val active = x._2.active
          val req = x._2.req
          val isfull = x._2.isfull
          val price = x._2.price
          ("%d-%d".format(adclass, target_value), (adclass, isshow, isclick, target_value, load, active, req, isfull, price))
      }
    val sexData = getTargetData(inputSexData, argDay, "sex")
    //println("sexData count is", sexData.count())
    insertData = insertData.union(sexData)

    val inputAgeData = allData
      .map {
        x =>
          val adclass = x._2.adclass
          val isshow = x._2.isshow
          val isclick = x._2.isclick
          val target_value = x._2.age
          val load = x._2.load
          val active = x._2.active
          val req = x._2.req
          val isfull = x._2.isfull
          val price = x._2.price
          ("%d-%d".format(adclass, target_value), (adclass, isshow, isclick, target_value, load, active, req, isfull, price))
      }
    val ageData = getTargetData(inputAgeData, argDay, "age")
    //println("ageData count is", ageData.count())
    insertData = insertData.union(ageData)
    println("2",insertData.count())

    val inputOsData = allData
      .map {
        x =>
          val adclass = x._2.adclass
          val isshow = x._2.isshow
          val isclick = x._2.isclick
          val target_value = x._2.os
          val load = x._2.load
          val active = x._2.active
          val req = x._2.req
          val isfull = x._2.isfull
          val price = x._2.price
          ("%d-%d".format(adclass, target_value), (adclass, isshow, isclick, target_value, load, active, req, isfull, price))
      }
    val osData = getTargetData(inputOsData, argDay, "os")
    //println("osData count is", osData.count())
    insertData = insertData.union(osData).repartition(50)

    val inputProvinceData = allData
      .map {
        x =>
          val adclass = x._2.adclass
          val isshow = x._2.isshow
          val isclick = x._2.isclick
          val target_value = x._2.province
          val load = x._2.load
          val active = x._2.active
          val req = x._2.req
          val isfull = x._2.isfull
          val price = x._2.price
          ("%d-%d".format(adclass, target_value), (adclass, isshow, isclick, target_value, load, active, req, isfull, price))
      }
    val provinceData = getTargetData(inputProvinceData, argDay, "province")
    //println("provinceData count is", provinceData.count())
    insertData = insertData.union(provinceData)

    val inputPhoneLevelData = allData
      .map {
        x =>
          val adclass = x._2.adclass
          val isshow = x._2.isshow
          val isclick = x._2.isclick
          val target_value = x._2.phone_level
          val load = x._2.load
          val active = x._2.active
          val req = x._2.req
          val isfull = x._2.isfull
          val price = x._2.price
          ("%d-%d".format(adclass, target_value), (adclass, isshow, isclick, target_value, load, active, req, isfull, price))
      }
    val phoneLevelData = getTargetData(inputPhoneLevelData, argDay, "phone_level")
    //println("phoneLevelData count is", phoneLevelData.count())
    insertData = insertData.union(phoneLevelData)
    println("3",insertData.count())

    val inputHourData = allData
      .map {
        x =>
          val adclass = x._2.adclass
          val isshow = x._2.isshow
          val isclick = x._2.isclick
          val target_value = x._2.hour
          val load = x._2.load
          val active = x._2.active
          val req = x._2.req
          val isfull = x._2.isfull
          val price = x._2.price
          ("%d-%d".format(adclass, target_value), (adclass, isshow, isclick, target_value, load, active, req, isfull, price))
      }
    val hourData = getTargetData(inputHourData, argDay, "hour")
    //println("hourData count is", hourData.count())
    insertData = insertData.union(hourData)

    val inputNetworkData = allData
      .map {
        x =>
          val adclass = x._2.adclass
          val isshow = x._2.isshow
          val isclick = x._2.isclick
          val target_value = x._2.network
          val load = x._2.load
          val active = x._2.active
          val req = x._2.req
          val isfull = x._2.isfull
          val price = x._2.price
          ("%d-%d".format(adclass, target_value), (adclass, isshow, isclick, target_value, load, active, req, isfull, price))
      }
    val networkData = getTargetData(inputNetworkData, argDay, "network_type")
    //println("networkData count is", networkData.count())
    insertData = insertData.union(networkData)

    val inputUserLevelData = allData
      .map {
        x =>
          val adclass = x._2.adclass
          val isshow = x._2.isshow
          val isclick = x._2.isclick
          val target_value = x._2.user_level
          val load = x._2.load
          val active = x._2.active
          val req = x._2.req
          val isfull = x._2.isfull
          val price = x._2.price
          ("%d-%d".format(adclass, target_value), (adclass, isshow, isclick, target_value, load, active, req, isfull, price))
      }
    val userLevelData = getTargetData(inputUserLevelData, argDay, "user_level")
    //println("userLevelData count is", userLevelData.count())
    insertData = insertData.union(userLevelData)
    println("4",insertData.count())

    val inputCityLevelData = allData
      .map {
        x =>
          val adclass = x._2.adclass
          val isshow = x._2.isshow
          val isclick = x._2.isclick
          val target_value = x._2.city_level
          val load = x._2.load
          val active = x._2.active
          val req = x._2.req
          val isfull = x._2.isfull
          val price = x._2.price
          ("%d-%d".format(adclass, target_value), (adclass, isshow, isclick, target_value, load, active, req, isfull, price))
      }
    val cityLevelData = getTargetData(inputCityLevelData, argDay, "city_level")
    //println("cityLevelData count is", cityLevelData.count())
    insertData = insertData.union(cityLevelData).repartition(50)

    val inputQuAdslotTypeData = allData
      .filter(_._2.qu_adslot_type > 0)
      .map {
        x =>
          val adclass = x._2.adclass
          val isshow = x._2.isshow
          val isclick = x._2.isclick
          val target_value = x._2.qu_adslot_type
          val load = x._2.load
          val active = x._2.active
          val req = x._2.req
          val isfull = x._2.isfull
          val price = x._2.price
          ("%d-%d".format(adclass, target_value), (adclass, isshow, isclick, target_value, load, active, req, isfull, price))
      }
    val quAdslotTypeData = getTargetData(inputQuAdslotTypeData, argDay, "qu_adslot_type")
    //println("quAdslotTypeData count is", quAdslotTypeData.count())
    insertData = insertData.union(quAdslotTypeData)
    println("",insertData.count())

    val inputExtAdslotTypeData = allData
      .filter(_._2.ext_adslot_type > 0)
      .map {
        x =>
          val adclass = x._2.adclass
          val isshow = x._2.isshow
          val isclick = x._2.isclick
          val target_value = x._2.ext_adslot_type
          val load = x._2.load
          val active = x._2.active
          val req = x._2.req
          val isfull = x._2.isfull
          val price = x._2.price
          ("%d-%d".format(adclass, target_value), (adclass, isshow, isclick, target_value, load, active, req, isfull, price))
      }
    val extAdslotType = getTargetData(inputExtAdslotTypeData, argDay, "ext_adslot_type")
    //println("extAdslotType count is", extAdslotType.count())
    insertData = insertData.union(extAdslotType)

    println("insertData count", insertData.count())

    var insertDataFrame = ctx.createDataFrame(insertData)
      .toDF("category_id", "impression", "click", "target_type", "target_value", "load",
        "active", "date", "request", "served_request", "cost")

    clearReportCategoryTarget(argDay)

    insertDataFrame
      .write
      .mode(SaveMode.Append)
      .jdbc(mariadbUrl, "report.report_category_target", mariadbProp)
    println("report_category_target done")
  }

  def getTargetData(data: RDD[(String, (Int, Int, Int, Int, Int, Int, Int, Int, Int))], argDay: String, targetType: String): RDD[(Int, Int, Int, String, Int, Int, Int, String, Int, Int, Int)] = {
    data
      .reduceByKey {
        (a, b) =>
          val adclass = a._1
          val isshow = a._2 + b._2
          val isclick = a._3 + b._3
          val target_value = a._4
          val load = a._5 + b._5
          val active = a._6 + b._6
          val req = a._7 + b._7
          val isfull = a._8 + b._8
          val price = a._9 + b._9
          (adclass, isshow, isclick, target_value, load, active, req, isfull, price)
      }
      .map {
        x =>
          val adclass = x._2._1
          val isshow = x._2._2
          val isclick = x._2._3
          val target_value = x._2._4
          val load = x._2._5
          val active = x._2._6
          val target_type = targetType
          var date = argDay
          val req = x._2._7
          val isfull = x._2._8
          val price = x._2._9
          (adclass, isshow, isclick, target_type, target_value, load, active, date, req, isfull, price)
      }
  }

  def clearReportCategoryTarget(date: String): Unit = {
    try {
      Class.forName(mariadbProp.getProperty("driver"))
      val conn = DriverManager.getConnection(
        mariadbUrl,
        mariadbProp.getProperty("user"),
        mariadbProp.getProperty("password"))
      val stmt = conn.createStatement()
      val sql =
        """
          |delete from report.report_category_target where `date` = "%s"
        """.stripMargin.format(date)
      stmt.executeUpdate(sql);
    } catch {
      case e: Exception => println("exception caught: " + e);
    }
  }
}

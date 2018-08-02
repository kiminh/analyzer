package com.cpc.spark.small.tool

import java.sql.DriverManager
import java.util.Properties

import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.util.Success

/**
  * Created by wanli on 2017/11/14.
  */
object InsertReportAdslotTarget {

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

    val ctx = SparkSession.builder().appName("InsertReportAdslotTarget is run day is %s".format(argDay)).enableHiveSupport().getOrCreate()

    println("InsertReportAdslotTarget is run day is %s".format(argDay))


    val adslotData = ctx
      .sql(
        """
          |SELECT searchid,media_appsid,adslotid,adslot_type,isshow,isclick,sex,age,os,province,ext['phone_level'].int_value,
          |hour,ext['os_version'].string_value,ext["adclass"].int_value,isfill,price,interests
          |FROM dl_cpc.cpc_union_log
          |WHERE date="%s" and userid <>1501897
        """.stripMargin.format(argDay))
      .rdd
      .map {
        x =>
          val searchid = x.getString(0)
          val mediaid = x.getString(1)
          val adslotid = x.getString(2)
          val adslot_type = x.getInt(3)
          val isshow = x.getInt(4)
          val isclick = x.getInt(5)
          val sex = x.getInt(6)
          val age = x.getInt(7)
          val os = x.getInt(8)
          val province = x.getInt(9)
          val phone_level = if (x.get(10) == null) 0 else x.getInt(10)
          val hour = x.getString(11).toInt
          var os_version = if (x.getString(12) != null) x.getString(12) else ""
          val adclass = if (x.get(13) == null) 0 else x.getInt(13)
          val req = 1
          val isfull = x.getInt(14)
          val price = if (isclick > 0) x.getInt(15) else 0
          val interests = x.get(16).toString
          val isStudent = if(interests.contains("224=")) 1 else if(interests.contains("225=")) 2 else 0

          val load = 0
          val active = 0

          (searchid, (mediaid, adslotid, adslot_type, isshow, isclick, sex, age, os, province, phone_level, hour,
            os_version, adclass, load, active, req, isfull, price,isStudent))
      }
      .cache()
    println("adslotData count", adslotData.count())

    val traceData = ctx.sql(
      """
        |SELECT DISTINCT searchid,trace_type,duration
        |FROM dl_cpc.cpc_union_trace_log
        |WHERE date="%s"
      """.stripMargin.format(argDay))
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
            //case "press" => active += 1
            case "disactive" => active -= 1
            case _ =>
          }
          (searchid, ("", "", -1, -1, -1, -1, -1, -1, -1, -1, -1, "", -1, load, active, -1, -1, 0,0))
      }
      .cache()
    println("traceData count", traceData.count())

    val allData = adslotData
      .union(traceData)
      .reduceByKey {
        (a, b) =>
          val mediaid = if (a._1.length > 0) a._1 else b._1
          val adslotid = if (a._2.length > 0) a._2 else b._2
          val adslot_type = if (a._3 != -1) a._3 else b._3
          val isshow = if (a._4 != -1) a._4 else b._4
          val isclick = if (a._5 != -1) a._5 else b._5
          val sex = if (a._6 != -1) a._6 else b._6
          val age = if (a._7 != -1) a._7 else b._7
          val os = if (a._8 != -1) a._8 else b._8
          val province = if (a._9 != -1) a._9 else b._9
          val phone_level = if (a._10 != -1) a._10 else b._10
          val hour = if (a._11 != -1) a._11 else b._11

          val os_version = if (a._12.length > 0) a._12 else b._12
          val adclass = if (a._13 != -1) a._13 else b._13

          var load = 0
          if (a._14 > 0) {
            load = a._14
          }
          if (b._14 > 0) {
            load += b._14
          }

          var active = 0
          if (a._15 > 0) {
            active = a._15
          }
          if (b._15 > 0) {
            active += b._15
          }

          val req = if (a._16 != -1) a._16 else b._16
          val isfull = if (a._17 != -1) a._17 else b._17
          val price = a._18 + b._18 //if (a._18 != -1) a._18 else b._18
          val isStudent = if (a._1 != -1 ) a._19 else b._19
          (mediaid, adslotid, adslot_type, isshow, isclick, sex, age, os, province, phone_level, hour, os_version,
            adclass, load, active, req, isfull, price,isStudent)
      }
      .filter {
        x =>
          val adslotid = x._2._1
          (adslotid.length > 0)
      }
      .repartition(50)
      .cache()
    println("allData count", allData.count())

    val studentData = allData
      .map {
        x =>
          val mediaid = x._2._1
          val adslotid = x._2._2
          val adslot_type = x._2._3
          val isshow = x._2._4
          val isclick = x._2._5
          val student = x._2._19
          val load = x._2._14
          val active = x._2._15
          val req = x._2._16
          val isfull = x._2._17
          val price = x._2._18
          ("%s-%d".format(adslotid, student), (mediaid, adslotid, adslot_type, isshow, isclick, student, load, active, req, isfull, price))
      }
      .reduceByKey {
        (a, b) =>
          val mediaid = a._1
          val adslotid = a._2
          val adslot_type = a._3
          val isshow = a._4 + b._4
          val isclick = a._5 + b._5
          val student = a._6
          val load = a._7 + b._7
          val active = a._8 + b._8
          val req = a._9 + b._9
          val isfull = a._10 + b._10
          val price = a._11 + b._11
          (mediaid, adslotid, adslot_type, isshow, isclick, student, load, active, req, isfull, price)
      }
      .map {
        x =>
          val mediaid = x._2._1
          val adslotid = x._2._2
          val adslot_type = x._2._3
          val isshow = x._2._4
          val isclick = x._2._5
          val student = x._2._6
          val load = x._2._7
          val active = x._2._8
          val target_type = "student"
          var date = argDay
          val req = x._2._9
          val isfull = x._2._10
          val price = x._2._11
          (mediaid, adslotid, adslot_type, isshow, isclick, target_type, student, load, active, date, req, isfull, price)
      }
      .repartition(50)
      .cache()
    println("studentData count", studentData.count())


    val osVersionData = allData
      .map {
        x =>
          val mediaid = x._2._1
          val adslotid = x._2._2
          val adslot_type = x._2._3
          val isshow = x._2._4
          val isclick = x._2._5
          val os = x._2._8
          val osVersion = "%d,%s".format(os + 10, x._2._12)
          val load = x._2._14
          val active = x._2._15
          val req = x._2._16
          val isfull = x._2._17
          val price = x._2._18
          ("%s-%s".format(adslotid, osVersion), (mediaid, adslotid, adslot_type, isshow, isclick, osVersion, load, active, req, isfull, price))
      }
      .reduceByKey {
        (a, b) =>
          val mediaid = a._1
          val adslotid = a._2
          val adslot_type = a._3
          val isshow = a._4 + b._4
          val isclick = a._5 + b._5
          val osVersion = a._6
          val load = a._7 + b._7
          val active = a._8 + b._8
          val req = a._9 + b._9
          val isfull = a._10 + b._10
          val price = a._11 + b._11
          (mediaid, adslotid, adslot_type, isshow, isclick, osVersion, load, active, req, isfull, price)
      }
      .map {
        x =>
          val mediaid = x._2._1
          val adslotid = x._2._2
          val adslot_type = x._2._3
          val isshow = x._2._4
          val isclick = x._2._5
          val tmp = x._2._6.split(",")
          var osVersion = 0
          if (tmp.length == 2 && tmp(1).length < 9) {
            val os = tmp(0)
            var osVersionArr = tmp(1).split("\\.")
            if (osVersionArr.length == 4) {
              var tmpOsVersion = ""
              osVersionArr.slice(0, 3).foreach {
                y =>
                  if (y.length == 1) {
                    tmpOsVersion = tmpOsVersion + "0" + y
                  } else if (y.length == 2) {
                    tmpOsVersion = tmpOsVersion + y
                  }
              }
              if (tmpOsVersion.length > 0) {
                var tmpOsV = scala.util.Try("%s%s".format(os, tmpOsVersion).toInt)
                osVersion = tmpOsV match {
                  case Success(_) => "%s%s".format(os, tmpOsVersion).toInt;
                  case _ => 0
                }
              }
            }
          }

          val load = x._2._7
          val active = x._2._8
          val target_type = "os_version"
          var date = argDay
          val req = x._2._9
          val isfull = x._2._10
          val price = x._2._11
          (mediaid, adslotid, adslot_type, isshow, isclick, target_type, osVersion, load, active, date, req, isfull, price)
      }
      .repartition(50)
      .cache()
    println("osVersionData count", osVersionData.count())

    val sexData = allData
      .map {
        x =>
          val mediaid = x._2._1
          val adslotid = x._2._2
          val adslot_type = x._2._3
          val isshow = x._2._4
          val isclick = x._2._5
          val sex = x._2._6
          val load = x._2._14
          val active = x._2._15
          val req = x._2._16
          val isfull = x._2._17
          val price = x._2._18
          ("%s-%d".format(adslotid, sex), (mediaid, adslotid, adslot_type, isshow, isclick, sex, load, active, req, isfull, price))
      }
      .reduceByKey {
        (a, b) =>
          val mediaid = a._1
          val adslotid = a._2
          val adslot_type = a._3
          val isshow = a._4 + b._4
          val isclick = a._5 + b._5
          val sex = a._6
          val load = a._7 + b._7
          val active = a._8 + b._8
          val req = a._9 + b._9
          val isfull = a._10 + b._10
          val price = a._11 + b._11
          (mediaid, adslotid, adslot_type, isshow, isclick, sex, load, active, req, isfull, price)
      }
      .map {
        x =>
          val mediaid = x._2._1
          val adslotid = x._2._2
          val adslot_type = x._2._3
          val isshow = x._2._4
          val isclick = x._2._5
          val sex = x._2._6
          val load = x._2._7
          val active = x._2._8
          val target_type = "sex"
          var date = argDay
          val req = x._2._9
          val isfull = x._2._10
          val price = x._2._11
          (mediaid, adslotid, adslot_type, isshow, isclick, target_type, sex, load, active, date, req, isfull, price)
      }
      .repartition(50)
      .cache()
    println("sexData count", sexData.count())

    val ageData = allData
      .map {
        x =>
          val mediaid = x._2._1
          val adslotid = x._2._2
          val adslot_type = x._2._3
          val isshow = x._2._4
          val isclick = x._2._5
          val age = x._2._7
          val load = x._2._14
          val active = x._2._15
          val req = x._2._16
          val isfull = x._2._17
          val price = x._2._18

          ("%s-%d".format(adslotid, age), (mediaid, adslotid, adslot_type, isshow, isclick, age, load, active, req, isfull, price))
      }
      .reduceByKey {
        (a, b) =>
          val mediaid = a._1
          val adslotid = a._2
          val adslot_type = a._3
          val isshow = a._4 + b._4
          val isclick = a._5 + b._5
          val age = a._6
          val load = a._7 + b._7
          val active = a._8 + b._8
          val req = a._9 + b._9
          val isfull = a._10 + b._10
          val price = a._11 + b._11
          (mediaid, adslotid, adslot_type, isshow, isclick, age, load, active, req, isfull, price)
      }
      .map {
        x =>
          val mediaid = x._2._1
          val adslotid = x._2._2
          val adslot_type = x._2._3
          val isshow = x._2._4
          val isclick = x._2._5
          val sex = x._2._6
          val load = x._2._7
          val active = x._2._8
          val target_type = "age"
          var date = argDay
          val req = x._2._9
          val isfull = x._2._10
          val price = x._2._11
          (mediaid, adslotid, adslot_type, isshow, isclick, target_type, sex, load, active, date, req, isfull, price)
      }
      .repartition(50)
      .cache()
    println("ageData count", ageData.count())

    val osData = allData
      .map {
        x =>
          val mediaid = x._2._1
          val adslotid = x._2._2
          val adslot_type = x._2._3
          val isshow = x._2._4
          val isclick = x._2._5
          val os = x._2._8
          val load = x._2._14
          val active = x._2._15
          val req = x._2._16
          val isfull = x._2._17
          val price = x._2._18
          ("%s-%d".format(adslotid, os), (mediaid, adslotid, adslot_type, isshow, isclick, os, load, active, req, isfull, price))
      }
      .reduceByKey {
        (a, b) =>
          val mediaid = a._1
          val adslotid = a._2
          val adslot_type = a._3
          val isshow = a._4 + b._4
          val isclick = a._5 + b._5
          val os = a._6
          val load = a._7 + b._7
          val active = a._8 + b._8
          val req = a._9 + b._9
          val isfull = a._10 + b._10
          val price = a._11 + b._11
          (mediaid, adslotid, adslot_type, isshow, isclick, os, load, active, req, isfull, price)
      }
      .map {
        x =>
          val mediaid = x._2._1
          val adslotid = x._2._2
          val adslot_type = x._2._3
          val isshow = x._2._4
          val isclick = x._2._5
          val os = x._2._6
          val load = x._2._7
          val active = x._2._8
          val target_type = "os"
          var date = argDay
          val req = x._2._9
          val isfull = x._2._10
          val price = x._2._11
          (mediaid, adslotid, adslot_type, isshow, isclick, target_type, os, load, active, date, req, isfull, price)
      }
      .repartition(50)
      .cache()
    println("osData count", osData.count())

    val provinceData = allData
      .map {
        x =>
          val mediaid = x._2._1
          val adslotid = x._2._2
          val adslot_type = x._2._3
          val isshow = x._2._4
          val isclick = x._2._5
          val province = x._2._9
          val load = x._2._14
          val active = x._2._15
          val req = x._2._16
          val isfull = x._2._17
          val price = x._2._18
          ("%s-%d".format(adslotid, province), (mediaid, adslotid, adslot_type, isshow, isclick, province, load, active, req, isfull, price))
      }
      .reduceByKey {
        (a, b) =>
          val mediaid = a._1
          val adslotid = a._2
          val adslot_type = a._3
          val isshow = a._4 + b._4
          val isclick = a._5 + b._5
          val province = a._6
          val load = a._7 + b._7
          val active = a._8 + b._8
          val req = a._9 + b._9
          val isfull = a._10 + b._10
          val price = a._11 + b._11
          (mediaid, adslotid, adslot_type, isshow, isclick, province, load, active, req, isfull, price)
      }
      .map {
        x =>
          val mediaid = x._2._1
          val adslotid = x._2._2
          val adslot_type = x._2._3
          val isshow = x._2._4
          val isclick = x._2._5
          val province = x._2._6
          val load = x._2._7
          val active = x._2._8
          val target_type = "province"
          var date = argDay
          val req = x._2._9
          val isfull = x._2._10
          val price = x._2._11
          (mediaid, adslotid, adslot_type, isshow, isclick, target_type, province, load, active, date, req, isfull, price)
      }
      .repartition(50)
      .cache()
    println("provinceData count", provinceData.count())

    val phoneLevelData = allData
      .map {
        x =>
          val mediaid = x._2._1
          val adslotid = x._2._2
          val adslot_type = x._2._3
          val isshow = x._2._4
          val isclick = x._2._5
          val phoneLevel = x._2._10
          val load = x._2._14
          val active = x._2._15
          val req = x._2._16
          val isfull = x._2._17
          val price = x._2._18
          ("%s-%d".format(adslotid, phoneLevel), (mediaid, adslotid, adslot_type, isshow, isclick, phoneLevel, load, active, req, isfull, price))
      }
      .reduceByKey {
        (a, b) =>
          val mediaid = a._1
          val adslotid = a._2
          val adslot_type = a._3
          val isshow = a._4 + b._4
          val isclick = a._5 + b._5
          val phoneLevel = a._6
          val load = a._7 + b._7
          val active = a._8 + b._8
          val req = a._9 + b._9
          val isfull = a._10 + b._10
          val price = a._11 + b._11
          (mediaid, adslotid, adslot_type, isshow, isclick, phoneLevel, load, active, req, isfull, price)
      }
      .map {
        x =>
          val mediaid = x._2._1
          val adslotid = x._2._2
          val adslot_type = x._2._3
          val isshow = x._2._4
          val isclick = x._2._5
          val phoneLevel = x._2._6
          val load = x._2._7
          val active = x._2._8
          val target_type = "phone_level"
          var date = argDay
          val req = x._2._9
          val isfull = x._2._10
          val price = x._2._11
          (mediaid, adslotid, adslot_type, isshow, isclick, target_type, phoneLevel, load, active, date, req, isfull, price)
      }
      .repartition(50)
      .cache()
    println("phoneLevelData count", phoneLevelData.count())

    val hourData = allData
      .map {
        x =>
          val mediaid = x._2._1
          val adslotid = x._2._2
          val adslot_type = x._2._3
          val isshow = x._2._4
          val isclick = x._2._5
          val hour = x._2._11
          val load = x._2._14
          val active = x._2._15
          val req = x._2._16
          val isfull = x._2._17
          val price = x._2._18
          ("%s-%d".format(adslotid, hour), (mediaid, adslotid, adslot_type, isshow, isclick, hour, load, active, req, isfull, price))
      }
      .reduceByKey {
        (a, b) =>
          val mediaid = a._1
          val adslotid = a._2
          val adslot_type = a._3
          val isshow = a._4 + b._4
          val isclick = a._5 + b._5
          val hour = a._6
          val load = a._7 + b._7
          val active = a._8 + b._8
          val req = a._9 + b._9
          val isfull = a._10 + b._10
          val price = a._11 + b._11
          (mediaid, adslotid, adslot_type, isshow, isclick, hour, load, active, req, isfull, price)
      }
      .map {
        x =>
          val mediaid = x._2._1
          val adslotid = x._2._2
          val adslot_type = x._2._3
          val isshow = x._2._4
          val isclick = x._2._5
          val hour = x._2._6
          val load = x._2._7
          val active = x._2._8
          val target_type = "hour"
          var date = argDay
          val req = x._2._9
          val isfull = x._2._10
          val price = x._2._11
          (mediaid, adslotid, adslot_type, isshow, isclick, target_type, hour, load, active, date, req, isfull, price)
      }
      .repartition(50)
      .cache()
    println("hourData count", hourData.count())

    val categoryData = allData
      .map {
        x =>
          val mediaid = x._2._1
          val adslotid = x._2._2
          val adslot_type = x._2._3
          val isshow = x._2._4
          val isclick = x._2._5
          val adclass = x._2._13
          val load = x._2._14
          val active = x._2._15
          val req = x._2._16
          val isfull = x._2._17
          val price = x._2._18
          ("%s-%d".format(adslotid, adclass), (mediaid, adslotid, adslot_type, isshow, isclick, adclass, load, active, req, isfull, price))
      }
      .reduceByKey {
        (a, b) =>
          val mediaid = a._1
          val adslotid = a._2
          val adslot_type = a._3
          val isshow = a._4 + b._4
          val isclick = a._5 + b._5
          val category = a._6
          val load = a._7 + b._7
          val active = a._8 + b._8
          val req = a._9 + b._9
          val isfull = a._10 + b._10
          val price = a._11 + b._11
          (mediaid, adslotid, adslot_type, isshow, isclick, category, load, active, req, isfull, price)
      }
      .map {
        x =>
          val mediaid = x._2._1
          val adslotid = x._2._2
          val adslot_type = x._2._3
          val isshow = x._2._4
          val isclick = x._2._5
          val category = x._2._6
          val load = x._2._7
          val active = x._2._8
          val target_type = "category"
          var date = argDay
          val req = x._2._9
          val isfull = x._2._10
          val price = x._2._11
          (mediaid, adslotid, adslot_type, isshow, isclick, target_type, category, load, active, date, req, isfull, price)
      }
      .repartition(50)
      .cache()
    println("categoryData count", categoryData.count())

    //(mediaid, adslotid, adslot_type, isshow, isclick, sex, age, os, province, phone_level, hour, os_version, adclass, load, active,req,isfull,price)
    val insertData = sexData
      .union(ageData)
      .union(osData)
      .union(provinceData)
      .union(phoneLevelData)
      .union(hourData)
      .union(categoryData)
      .union(osVersionData)
      .union(studentData)
      .repartition(50)
      .cache()

    println("insertData count", insertData.count())
    //
    var insertDataFrame = ctx.createDataFrame(insertData)
      .toDF("media_id", "adslot_id", "adslot_type", "impression", "click", "target_type", "target_value", "load",
        "active", "date", "request", "served_request", "cost")

    clearReportUnitTarget(argDay)

    insertDataFrame
      .write
      .mode(SaveMode.Append)
      .jdbc(mariadbUrl, "report.report_adslot_target", mariadbProp)
  }

  def clearReportUnitTarget(date: String): Unit = {
    try {
      Class.forName(mariadbProp.getProperty("driver"))
      val conn = DriverManager.getConnection(
        mariadbUrl,
        mariadbProp.getProperty("user"),
        mariadbProp.getProperty("password"))
      val stmt = conn.createStatement()
      val sql =
        """
          |delete from report.report_adslot_target where `date` = "%s"
        """.stripMargin.format(date)
      stmt.executeUpdate(sql);
    } catch {
      case e: Exception => println("exception caught: " + e);
    }
  }
}

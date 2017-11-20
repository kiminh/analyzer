package com.cpc.spark.small.tool

import java.sql.DriverManager
import java.util.Properties

import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * Created by wanli on 2017/11/14.
  */
object InsertReportUnitTarget {

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

    val ctx = SparkSession.builder().appName("InsertReportUnitTarget is run day is %s".format(argDay)).enableHiveSupport().getOrCreate()

    println("InsertReportTargetSex is run day is %s".format(argDay))


    val ideaData = ctx
      .sql(
        """
          |SELECT searchid,userid,planid,unitid,isshow,isclick,sex,age,os,province,ext['phone_level'].int_value,hour
          |FROM dl_cpc.cpc_union_log
          |WHERE date="%s" AND userid>0 AND unitid>0 AND isshow>0
        """.stripMargin.format(argDay))
      .rdd
      .map {
        x =>
          val searchid = x.getString(0)
          val userid = x.getInt(1)
          val planid = x.getInt(2)
          val unitid = x.getInt(3)
          val isshow = x.getInt(4)
          val isclick = x.getInt(5)
          val sex = x.getInt(6)
          val age = x.getInt(7)
          val os = x.getInt(8)
          val province = x.getInt(9)
          val phone_level = x.getInt(10)
          val hour = x.getString(11).toInt
          val load = 0
          val active = 0
          (searchid, (userid, planid, unitid, isshow, isclick, sex, age, os, province, phone_level, hour, load, active))
      }
      .cache()
    println("ideaData count", ideaData.count())

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
            case "press" => active += 1
            case _ =>
          }
          (searchid, (-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, load, active))
      }
      .cache()
    println("traceData count", traceData.count())

    val allData = ideaData
      .union(traceData)
      .reduceByKey {
        (a, b) =>
          val userid = if (a._1 != -1) a._1 else b._1
          val planid = if (a._2 != -1) a._2 else b._2
          val unitid = if (a._3 != -1) a._3 else b._3
          val isshow = if (a._4 != -1) a._4 else b._4
          val isclick = if (a._5 != -1) a._5 else b._5
          val sex = if (a._6 != -1) a._6 else b._6
          val age = if (a._7 != -1) a._7 else b._7
          val os = if (a._8 != -1) a._8 else b._8
          val province = if (a._9 != -1) a._9 else b._9
          val phone_level = if (a._10 != -1) a._10 else b._10
          val hour = if (a._11 != -1) a._11 else b._11
          var load = 0
          if (a._12 > 0) {
            load = a._12
          }
          if (b._12 > 0) {
            load += b._12
          }
          var active = 0
          if (a._13 > 0) {
            active = a._13
          }
          if (b._13 > 0) {
            active += b._13
          }
          (userid, planid, unitid, isshow, isclick, sex, age, os, province, phone_level, hour, load, active)
      }
      .filter {
        x =>
          val user_id = x._2._1
          (user_id != -1)
      }
      .repartition(50)
      .cache()
    println("allData count", allData.count())

    val sexData = allData
      .map {
        x =>
          val userid = x._2._1
          val planid = x._2._2
          val unitid = x._2._3
          val isshow = x._2._4
          val isclick = x._2._5
          val sex = x._2._6
          val load = x._2._12
          val active = x._2._13
          ("%d-%d".format(unitid, sex), (userid, planid, unitid, isshow, isclick, sex, load, active))
      }
      .reduceByKey {
        (a, b) =>
          val userid = a._1
          val planid = a._2
          val unitid = a._3
          val isshow = a._4 + b._4
          val isclick = a._5 + b._5
          val sex = a._6
          val load = a._7 + b._7
          val active = a._8 + b._8
          (userid, planid, unitid, isshow, isclick, sex, load, active)
      }
      .map {
        x =>
          val userid = x._2._1
          val planid = x._2._2
          val unitid = x._2._3
          val isshow = x._2._4
          val isclick = x._2._5
          val sex = x._2._6
          val load = x._2._7
          val active = x._2._8
          val target_type = "sex"
          var date = argDay
          (userid, planid, unitid, isshow, isclick, target_type, sex, load, active, date)
      }
      .repartition(50)
      .cache()
    println("sexData count", sexData.count())

    val ageData = allData
      .map {
        x =>
          val userid = x._2._1
          val planid = x._2._2
          val unitid = x._2._3
          val isshow = x._2._4
          val isclick = x._2._5
          val age = x._2._7
          val load = x._2._12
          val active = x._2._13
          ("%d-%d".format(unitid, age), (userid, planid, unitid, isshow, isclick, age, load, active))
      }
      .reduceByKey {
        (a, b) =>
          val userid = a._1
          val planid = a._2
          val unitid = a._3
          val isshow = a._4 + b._4
          val isclick = a._5 + b._5
          val age = a._6
          val load = a._7 + b._7
          val active = a._8 + b._8
          (userid, planid, unitid, isshow, isclick, age, load, active)
      }
      .map {
        x =>
          val userid = x._2._1
          val planid = x._2._2
          val unitid = x._2._3
          val isshow = x._2._4
          val isclick = x._2._5
          val age = x._2._6
          val load = x._2._7
          val active = x._2._8
          val target_type = "age"
          var date = argDay
          (userid, planid, unitid, isshow, isclick, target_type, age, load, active, date)
      }
      .repartition(50)
      .cache()
    println("ageData count", ageData.count())

    val osData = allData
      .map {
        x =>
          val userid = x._2._1
          val planid = x._2._2
          val unitid = x._2._3
          val isshow = x._2._4
          val isclick = x._2._5
          val os = x._2._8
          val load = x._2._12
          val active = x._2._13
          ("%d-%d".format(unitid, os), (userid, planid, unitid, isshow, isclick, os, load, active))
      }
      .reduceByKey {
        (a, b) =>
          val userid = a._1
          val planid = a._2
          val unitid = a._3
          val isshow = a._4 + b._4
          val isclick = a._5 + b._5
          val os = a._6
          val load = a._7 + b._7
          val active = a._8 + b._8
          (userid, planid, unitid, isshow, isclick, os, load, active)
      }
      .map {
        x =>
          val userid = x._2._1
          val planid = x._2._2
          val unitid = x._2._3
          val isshow = x._2._4
          val isclick = x._2._5
          val os = x._2._6
          val load = x._2._7
          val active = x._2._8
          val target_type = "os"
          var date = argDay
          (userid, planid, unitid, isshow, isclick, target_type, os, load, active, date)
      }
      .repartition(50)
      .cache()
    println("osData count", osData.count())

    val provinceData = allData
      .map {
        x =>
          val userid = x._2._1
          val planid = x._2._2
          val unitid = x._2._3
          val isshow = x._2._4
          val isclick = x._2._5
          val province = x._2._9
          val load = x._2._12
          val active = x._2._13
          ("%d-%d".format(unitid, province), (userid, planid, unitid, isshow, isclick, province, load, active))
      }
      .reduceByKey {
        (a, b) =>
          val userid = a._1
          val planid = a._2
          val unitid = a._3
          val isshow = a._4 + b._4
          val isclick = a._5 + b._5
          val province = a._6
          val load = a._7 + b._7
          val active = a._8 + b._8
          (userid, planid, unitid, isshow, isclick, province, load, active)
      }
      .map {
        x =>
          val userid = x._2._1
          val planid = x._2._2
          val unitid = x._2._3
          val isshow = x._2._4
          val isclick = x._2._5
          val province = x._2._6
          val load = x._2._7
          val active = x._2._8
          val target_type = "province"
          var date = argDay
          (userid, planid, unitid, isshow, isclick, target_type, province, load, active, date)
      }
      .repartition(50)
      .cache()
    println("provinceData count", provinceData.count())

    val phoneLevelData = allData
      .map {
        x =>
          val userid = x._2._1
          val planid = x._2._2
          val unitid = x._2._3
          val isshow = x._2._4
          val isclick = x._2._5
          val phone_level = x._2._10
          val load = x._2._12
          val active = x._2._13
          ("%d-%d".format(unitid, phone_level), (userid, planid, unitid, isshow, isclick, phone_level, load, active))
      }
      .reduceByKey {
        (a, b) =>
          val userid = a._1
          val planid = a._2
          val unitid = a._3
          val isshow = a._4 + b._4
          val isclick = a._5 + b._5
          val phone_level = a._6
          val load = a._7 + b._7
          val active = a._8 + b._8
          (userid, planid, unitid, isshow, isclick, phone_level, load, active)
      }
      .map {
        x =>
          val userid = x._2._1
          val planid = x._2._2
          val unitid = x._2._3
          val isshow = x._2._4
          val isclick = x._2._5
          val phone_level = x._2._6
          val load = x._2._7
          val active = x._2._8
          val target_type = "phone_level"
          var date = argDay
          (userid, planid, unitid, isshow, isclick, target_type, phone_level, load, active, date)
      }
      .repartition(50)
      .cache()

    val hourData = allData
      .map {
        x =>
          val userid = x._2._1
          val planid = x._2._2
          val unitid = x._2._3
          val isshow = x._2._4
          val isclick = x._2._5
          val hour = x._2._11
          val load = x._2._12
          val active = x._2._13
          ("%d-%d".format(unitid, hour), (userid, planid, unitid, isshow, isclick, hour, load, active))
      }
      .reduceByKey {
        (a, b) =>
          val userid = a._1
          val planid = a._2
          val unitid = a._3
          val isshow = a._4 + b._4
          val isclick = a._5 + b._5
          val hour = a._6
          val load = a._7 + b._7
          val active = a._8 + b._8
          (userid, planid, unitid, isshow, isclick, hour, load, active)
      }
      .map {
        x =>
          val userid = x._2._1
          val planid = x._2._2
          val unitid = x._2._3
          val isshow = x._2._4
          val isclick = x._2._5
          val hour = x._2._6
          val load = x._2._7
          val active = x._2._8
          val target_type = "hour"
          var date = argDay
          (userid, planid, unitid, isshow, isclick, target_type, hour, load, active, date)
      }
      .repartition(50)
      .cache()
    println("hourData count", hourData.count())

    //(userid, planid, unitid, isshow, isclick, sex, age, os, regions, phone_level, hour, load, active)
    val insertData = sexData
      .union(ageData)
      .union(osData)
      .union(provinceData)
      .union(phoneLevelData)
      .union(hourData)
      .repartition(50)
      .cache()

    println("insertData count", insertData.count())

    var insertDataFrame = ctx.createDataFrame(insertData)
      .toDF("user_id", "plan_id", "unit_id", "impression", "click", "target_type", "target_value", "load", "active", "date")

    clearReportUnitTarget(argDay)

    insertDataFrame
      .write
      .mode(SaveMode.Append)
      .jdbc(mariadbUrl, "report.report_unit_target", mariadbProp)
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
          |delete from report.report_unit_target where `date` = "%s"
        """.stripMargin.format(date)
      stmt.executeUpdate(sql);
    } catch {
      case e: Exception => println("exception caught: " + e);
    }
  }
}

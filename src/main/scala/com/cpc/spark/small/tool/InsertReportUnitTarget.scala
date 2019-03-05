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
          |SELECT searchid,userid,planid,unitid,isshow,isclick,sex,age,os,province,ext['phone_level'].int_value,hour,
          |network,coin,ext['qukan_new_user'].int_value,adslot_type,media_appsid,interests,adslotid
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
          val network = x.getInt(12)
          val coin = x.getInt(13)
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

          val qukan_new_user = x.getInt(14)
          val adslotType = x.getInt(15)
          val mediaId = x.getString(16)
          val interests = x.get(17).toString
          val isStudent = if (interests.contains("224=")) 1 else if (interests.contains("225=")) 2 else 0

          val load = 0
          val active = 0

          val adslotid = x.get(18).toString.toInt

          (searchid, (userid, planid, unitid, isshow, isclick, sex, age, os, province, phone_level, hour,
            network, user_level, qukan_new_user, load, active, adslotType, mediaId, isStudent, adslotid, 0, 0))
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
            //case "press" => active += 1
            case "disactive" => active -= 1
            case _ =>
          }

          var active15 = 0
          if (trace_type == "active15") {
            active15 = 1
          }

          var active_third = 0
          if (trace_type == "active_third") {
            active_third = 1
          }

          (searchid, (-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, load, active, -1, "", 0, 0, active15, active_third))
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

          val network = if (a._12 != -1) a._12 else b._12
          val user_level = if (a._13 != -1) a._13 else b._13
          val qukan_new_user = if (a._14 != -1) a._14 else b._14

          var load = 0
          if (a._15 > 0) {
            load = a._15
          }
          if (b._15 > 0) {
            load += b._15
          }

          var active = 0
          if (a._16 > 0) {
            active = a._16
          }
          if (b._16 > 0) {
            active += b._16
          }

          val adslotType = if (a._17 != -1) a._17 else b._17
          val mediaId = if (a._18.length > 0) a._18 else b._18
          val isStudent = if (a._1 != -1) a._19 else b._19
          val adslotid = if (a._1 != -1) a._20 else b._20
          val active15 = a._21 + b._21
          val active_third = a._22 + b._22
          (userid, planid, unitid, isshow, isclick, sex, age, os, province, phone_level, hour, network, user_level, qukan_new_user, load, active,
            adslotType, mediaId, isStudent, adslotid, active15, active_third)
      }
      .filter {
        x =>
          val user_id = x._2._1
          (user_id != -1)
      }
      .repartition(50)
      .cache()
    println("allData count", allData.count())

    val inputStudentData = allData
      .map {
        x =>
          val userid = x._2._1
          val planid = x._2._2
          val unitid = x._2._3
          val isshow = x._2._4
          val isclick = x._2._5
          val isStudent = x._2._19
          val load = x._2._15
          val active = x._2._16
          val active15 = x._2._21
          val active_third = x._2._22
          ("%d-%d".format(unitid, isStudent), (userid, planid, unitid, isshow, isclick, isStudent, load, active, active15, active_third))
      }
    val studentData = getTargetData(inputStudentData, "student", argDay)

    var insertData = studentData

    val inputSexData = allData
      .map {
        x =>
          val userid = x._2._1
          val planid = x._2._2
          val unitid = x._2._3
          val isshow = x._2._4
          val isclick = x._2._5
          val sex = x._2._6
          val load = x._2._15
          val active = x._2._16
          val active15 = x._2._21
          val active_third = x._2._22
          ("%d-%d".format(unitid, sex), (userid, planid, unitid, isshow, isclick, sex, load, active, active15, active_third))
      }
    val sexData = getTargetData(inputSexData, "sex", argDay)
    insertData = insertData.union(sexData)

    val inputAgeData = allData
      .map {
        x =>
          val userid = x._2._1
          val planid = x._2._2
          val unitid = x._2._3
          val isshow = x._2._4
          val isclick = x._2._5
          val age = x._2._7
          val load = x._2._15
          val active = x._2._16
          val active15 = x._2._21
          val active_third = x._2._22
          ("%d-%d".format(unitid, age), (userid, planid, unitid, isshow, isclick, age, load, active, active15, active_third))
      }
    val ageData = getTargetData(inputAgeData, "age", argDay)
    insertData = insertData.union(ageData)

    println(insertData.count())

    val inputOsData = allData
      .map {
        x =>
          val userid = x._2._1
          val planid = x._2._2
          val unitid = x._2._3
          val isshow = x._2._4
          val isclick = x._2._5
          val os = x._2._8
          val load = x._2._15
          val active = x._2._16
          val active15 = x._2._21
          val active_third = x._2._22
          ("%d-%d".format(unitid, os), (userid, planid, unitid, isshow, isclick, os, load, active, active15, active_third))
      }
    val osData = getTargetData(inputOsData, "os", argDay)
    insertData = insertData.union(osData)

    val inputProvinceData = allData
      .map {
        x =>
          val userid = x._2._1
          val planid = x._2._2
          val unitid = x._2._3
          val isshow = x._2._4
          val isclick = x._2._5
          val province = x._2._9
          val load = x._2._15
          val active = x._2._16
          val active15 = x._2._21
          val active_third = x._2._22
          ("%d-%d".format(unitid, province), (userid, planid, unitid, isshow, isclick, province, load, active, active15, active_third))
      }
    val provinceData = getTargetData(inputProvinceData, "province", argDay)
    insertData = insertData.union(provinceData)

    val inputPhoneLevelData = allData
      .map {
        x =>
          val userid = x._2._1
          val planid = x._2._2
          val unitid = x._2._3
          val isshow = x._2._4
          val isclick = x._2._5
          val phone_level = x._2._10
          val load = x._2._15
          val active = x._2._16
          val active15 = x._2._21
          val active_third = x._2._22
          ("%d-%d".format(unitid, phone_level), (userid, planid, unitid, isshow, isclick, phone_level, load, active, active15, active_third))
      }
    val phoneLevelData = getTargetData(inputPhoneLevelData, "phone_level", argDay)
    insertData = insertData.union(phoneLevelData)

    val inputHourData = allData
      .map {
        x =>
          val userid = x._2._1
          val planid = x._2._2
          val unitid = x._2._3
          val isshow = x._2._4
          val isclick = x._2._5
          val hour = x._2._11
          val load = x._2._15
          val active = x._2._16
          val active15 = x._2._21
          val active_third = x._2._22
          ("%d-%d".format(unitid, hour), (userid, planid, unitid, isshow, isclick, hour, load, active, active15, active_third))
      }
    val hourData = getTargetData(inputHourData, "hour", argDay)
    insertData = insertData.union(hourData)

    println(insertData.count())

    val inputNetworkTypeData = allData
      .map {
        x =>
          val userid = x._2._1
          val planid = x._2._2
          val unitid = x._2._3
          val isshow = x._2._4
          val isclick = x._2._5
          val networkType = x._2._12
          val load = x._2._15
          val active = x._2._16
          val active15 = x._2._21
          val active_third = x._2._22
          ("%d-%d".format(unitid, networkType), (userid, planid, unitid, isshow, isclick, networkType, load, active, active15, active_third))
      }
    val networkTypeData = getTargetData(inputNetworkTypeData, "network_type", argDay)
    insertData = insertData.union(networkTypeData)

    //------------
    val inputUserLevelData = allData
      .map {
        x =>
          val userid = x._2._1
          val planid = x._2._2
          val unitid = x._2._3
          val isshow = x._2._4
          val isclick = x._2._5
          val user_level = x._2._13
          val load = x._2._15
          val active = x._2._16
          val active15 = x._2._21
          val active_third = x._2._22
          ("%d-%d".format(unitid, user_level), (userid, planid, unitid, isshow, isclick, user_level, load, active, active15, active_third))
      }
    val userLevelData = getTargetData(inputUserLevelData, "user_level", argDay)
    insertData = insertData.union(userLevelData)

    val inputQukanNewUserData = allData
      .map {
        x =>
          val userid = x._2._1
          val planid = x._2._2
          val unitid = x._2._3
          val isshow = x._2._4
          val isclick = x._2._5
          val userOrient = x._2._14
          val load = x._2._15
          val active = x._2._16
          val active15 = x._2._21
          val active_third = x._2._22
          ("%d-%d".format(unitid, userOrient), (userid, planid, unitid, isshow, isclick, userOrient, load, active, active15, active_third))
      }
    val qukanNewUserData = getTargetData(inputQukanNewUserData, "user_orient", argDay)
    insertData = insertData.union(qukanNewUserData)

    val inputAdslotTypeData = allData
      .map {
        x =>
          val userid = x._2._1
          val planid = x._2._2
          val unitid = x._2._3
          val isshow = x._2._4
          val isclick = x._2._5
          val adslotType = x._2._17
          val load = x._2._15
          val active = x._2._16
          val active15 = x._2._21
          val active_third = x._2._22
          ("%d-%d".format(unitid, adslotType), (userid, planid, unitid, isshow, isclick, adslotType, load, active, active15, active_third))
      }
    val adslotTypeData = getTargetData(inputAdslotTypeData, "adslot_type", argDay)
    insertData = insertData.union(adslotTypeData)
    println(insertData.count())

    val inputAdslotTypeMediaData = allData
      .filter {
        x =>
          val mediaId = x._2._18
          (mediaId == "80000001") || (mediaId == "80000002")
      }
      .map {
        x =>
          val userid = x._2._1
          val planid = x._2._2
          val unitid = x._2._3
          val isshow = x._2._4
          val isclick = x._2._5
          val adslotType = x._2._17
          val load = x._2._15
          val active = x._2._16
          val active15 = x._2._21
          val active_third = x._2._22
          ("%d-%d".format(unitid, adslotType), (userid, planid, unitid, isshow, isclick, adslotType, load, active, active15, active_third))
      }
    val adslotTypeMediaData = getTargetData(inputAdslotTypeMediaData, "adslot_type_media", argDay)
    insertData = insertData.union(adslotTypeMediaData)

    val inputAdslotIdData = allData
      .map {
        x =>
          val userid = x._2._1
          val planid = x._2._2
          val unitid = x._2._3
          val isshow = x._2._4
          val isclick = x._2._5
          val adslotType = x._2._20
          val load = x._2._15
          val active = x._2._16
          val active15 = x._2._21
          val active_third = x._2._22
          ("%d-%d".format(unitid, adslotType), (userid, planid, unitid, isshow, isclick, adslotType, load, active, active15, active_third))
      }
    val adslotidData = getTargetData(inputAdslotIdData, "adslotid", argDay)

    insertData = insertData.union(adslotidData).repartition(50)

    //    //(userid, planid, unitid, isshow, isclick, sex, age, os, province, phone_level, hour, network,user_level,qukan_new_user,load, active)
//    val insertData = sexData
//      .union(ageData)
//      .union(osData)
//      .union(provinceData)
//      .union(phoneLevelData)
//      .union(hourData)
//      .union(networkTypeData)
//      .union(userLevelData)
//      .union(qukanNewUserData)
//      .union(adslotTypeData)
//      .union(adslotTypeMediaData)
//      .union(studentData)
//      .repartition(50)
//      .cache()

    println("insertData count", insertData.count())

    var insertDataFrame = ctx.createDataFrame(insertData)
      .toDF("user_id", "plan_id", "unit_id", "impression", "click", "target_type", "target_value", "load", "active",
        "date", "active15", "active_third")
    insertDataFrame.show(50)

    clearReportUnitTarget(argDay)

    insertDataFrame
      .write
      .mode(SaveMode.Append)
      .jdbc(mariadbUrl, "report.report_unit_target", mariadbProp)
  }

  def getTargetData(data: RDD[(String, (Int, Int, Int, Int, Int, Int, Int, Int, Int, Int))],
                    target_type: String, date: String): (RDD[(Int, Int, Int, Int, Int, String, Int, Int, Int, String, Int, Int)]) = {
    data
      .reduceByKey {
        (a, b) =>
          val userid = a._1
          val planid = a._2
          val unitid = a._3
          val isshow = a._4 + b._4
          val isclick = a._5 + b._5
          val target_value = a._6
          val load = a._7 + b._7
          val active = a._8 + b._8
          val active15 = a._9 + b._9
          val active_third = a._10 + b._10
          (userid, planid, unitid, isshow, isclick, target_value, load, active, active15, active_third)
      }
      .map {
        x =>
          val userid = x._2._1
          val planid = x._2._2
          val unitid = x._2._3
          val isshow = x._2._4
          val isclick = x._2._5
          val target_value = x._2._6
          val load = x._2._7
          val active = x._2._8
          val active15 = x._2._9
          val active_third = x._2._10
          (userid, planid, unitid, isshow, isclick, target_type, target_value, load, active, date, active15, active_third)
      }
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

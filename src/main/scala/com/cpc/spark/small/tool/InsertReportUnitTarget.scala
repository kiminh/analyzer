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

  case class Info(
                   userid: Int = 0,
                   planid: Int = 0,
                   unitid: Int = 0,
                   isshow: Int = 0,
                   isclick: Int = 0,
                   sex: Int = 0,
                   age: Int = 0,
                   os: Int = 0,
                   province: Int = 0,
                   phoneLevel: Int = 0,
                   hour: Int = 0,
                   network: Int = 0,
                   userLevel: Int = 0,
                   qukanNewUser: Int = 0,
                   load: Int = 0,
                   active: Int = 0,
                   adslotType: Int = 0,
                   mediaid: String = "",
                   isStudent: Int = 0, //0未知，1学生，2非学生
                   adslotid: Int = 0,
                   active15: Int = 0,
                   active_third: Int = 0,
                   ctsite_active2: Int = 0,
                   ctsite_active15: Int = 0
                 )


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
          |SELECT searchid,userid,planid,unitid,isshow,isclick,sex,age,os,province,phone_level,hour,
          |network,coin,qukan_new_user,adslot_type,media_appsid,interests,adslot_id
          |FROM dl_cpc.cpc_basedata_union_events
          |WHERE day="%s" AND userid>0 AND unitid>0 AND isshow>0
        """.stripMargin.format(argDay))
      .rdd
      .map {
        x =>
          val searchid = x.getAs[String](0)
          val userid = x.getAs[Int](1)
          val planid = x.getAs[Int](2)
          val unitid = x.getAs[Int](3)
          val isshow = x.getAs[Int](4)
          val isclick = x.getAs[Int](5)
          val sex = x.getAs[Int](6)
          val age = x.getAs[Int](7)
          val os = x.getAs[Int](8)
          val province = x.getAs[Int](9)
          val phone_level = x.getAs[Int](10)
          val hour = x.getAs[String](11).toInt
          val network = x.getAs[Int](12)
          val coin = x.getAs[Int](13)
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

          val qukan_new_user = x.getAs[Int](14)
          val adslotType = x.getAs[Int](15)
          val mediaId = x.getAs[String](16)

          val isStudent = 0

          val load = 0
          val active = 0

          val adslotid = x.getAs[String](18).toInt

          (searchid, (Info(userid, planid, unitid, isshow, isclick, sex, age, os, province, phone_level, hour, network, user_level,
            qukan_new_user, load, active, adslotType, mediaId, isStudent, adslotid, 0, 0, 0, 0)))
      }
      .repartition(50)
      .cache()
    //println("ideaData count", ideaData.count())

    val traceData = ctx.sql(
      """
        |SELECT DISTINCT cutl.searchid,cutl.trace_type,cutl.duration
        |FROM dl_cpc.cpc_basedata_trace_event cutl
        |INNER JOIN dl_cpc.cpc_basedata_union_events cul ON cutl.searchid=cul.searchid
        |WHERE cutl.day="%s" AND cul.day="%s" AND cul.isclick>0 AND cul.ideaid>0
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

          var ctsite_active2 = 0
          if (trace_type == "ctsite_active2") {
            ctsite_active2 = 1
          }

          var ctsite_active15 = 0
          if (trace_type == "ctsite_active15") {
            ctsite_active15 = 1
          }

          (searchid, (Info(-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, load, active, -1, "", 0, 0, active15,
            active_third, ctsite_active2, ctsite_active15)))
      }
      .repartition(50)
      .cache()
    //println("traceData count", traceData.count())

    val allData = ideaData
      .union(traceData)
      .reduceByKey {
        (a, b) =>
          val userid = if (a.userid != -1) a.userid else b.userid
          val planid = if (a.userid != -1) a.planid else b.planid
          val unitid = if (a.userid != -1) a.unitid else b.unitid
          val isshow = if (a.userid != -1) a.isshow else b.isshow
          val isclick = if (a.userid != -1) a.isclick else b.isclick
          val sex = if (a.userid != -1) a.sex else b.sex
          val age = if (a.userid != -1) a.age else b.age
          val os = if (a.userid != -1) a.os else b.os
          val province = if (a.userid != -1) a.province else b.province
          val phone_level = if (a.userid != -1) a.phoneLevel else b.phoneLevel
          val hour = if (a.userid != -1) a.hour else b.hour

          val network = if (a.userid != -1) a.network else b.network
          val user_level = if (a.userid != -1) a.userLevel else b.userLevel
          val qukan_new_user = if (a.userid != -1) a.qukanNewUser else b.qukanNewUser

          var load = a.load+b.load

          var active = a.active+b.active

          val adslotType = if (a.userid != -1) a.adslotType else b.adslotType
          val mediaId = if (a.mediaid.length > 0) a.mediaid else b.mediaid
          val isStudent = if (a.userid != -1) a.isStudent else b.isStudent
          val adslotid = if (a.userid != -1) a.adslotid else b.adslotid
          val active15 = a.active15 + b.active15
          val active_third = a.active_third + b.active_third

          val ctsite_active2 = a.ctsite_active2 + b.ctsite_active2

          val ctsite_active15 = a.ctsite_active15 + b.ctsite_active15
          Info(userid, planid, unitid, isshow, isclick, sex, age, os, province, phone_level, hour, network, user_level, qukan_new_user, load, active,
            adslotType, mediaId, isStudent, adslotid, active15, active_third, ctsite_active2, ctsite_active15)
      }
      .filter {
        x =>
          x._2.userid>0
      }
      .repartition(50)
      .cache()
    //println("allData count", allData.count())

    val inputStudentData = allData
      .map {
        x =>
          val userid = x._2.userid
          val planid = x._2.planid
          val unitid = x._2.unitid
          val isshow = x._2.isshow
          val isclick = x._2.isclick
          val isStudent = x._2.isStudent
          val load = x._2.load
          val active = x._2.active
          val active15 = x._2.active15
          val active_third = x._2.active_third
          val ctsite_active2 = x._2.ctsite_active2
          val ctsite_active15 = x._2.ctsite_active15
          ("%d-%d".format(unitid, isStudent), (userid, planid, unitid, isshow, isclick, isStudent, load, active, active15,
              active_third,ctsite_active2,ctsite_active15))
      }
    val studentData = getTargetData(inputStudentData, "student", argDay)

    var insertData = studentData

    val inputSexData = allData
      .map {
        x =>
          val userid = x._2.userid
          val planid = x._2.planid
          val unitid = x._2.unitid
          val isshow = x._2.isshow
          val isclick = x._2.isclick
          val sex = x._2.sex
          val load = x._2.load
          val active = x._2.active
          val active15 = x._2.active15
          val active_third = x._2.active_third
          val ctsite_active2 = x._2.ctsite_active2
          val ctsite_active15 = x._2.ctsite_active15
          ("%d-%d".format(unitid, sex), (userid, planid, unitid, isshow, isclick, sex, load, active, active15, active_third,ctsite_active2,ctsite_active15))
      }
    val sexData = getTargetData(inputSexData, "sex", argDay)
    insertData = insertData.union(sexData)

    val inputAgeData = allData
      .map {
        x =>
          val userid = x._2.userid
          val planid = x._2.planid
          val unitid = x._2.unitid
          val isshow = x._2.isshow
          val isclick = x._2.isclick
          val age = x._2.age
          val load = x._2.load
          val active = x._2.active
          val active15 = x._2.active15
          val active_third = x._2.active_third
          val ctsite_active2 = x._2.ctsite_active2
          val ctsite_active15 = x._2.ctsite_active15
          ("%d-%d".format(unitid, age), (userid, planid, unitid, isshow, isclick, age, load, active, active15, active_third,ctsite_active2,ctsite_active15))
      }
    val ageData = getTargetData(inputAgeData, "age", argDay)
    insertData = insertData.union(ageData)

    println(insertData.count())

    val inputOsData = allData
      .map {
        x =>
          val userid = x._2.userid
          val planid = x._2.planid
          val unitid = x._2.unitid
          val isshow = x._2.isshow
          val isclick = x._2.isclick
          val os = x._2.os
          val load = x._2.load
          val active = x._2.active
          val active15 = x._2.active15
          val active_third = x._2.active_third
          val ctsite_active2 = x._2.ctsite_active2
          val ctsite_active15 = x._2.ctsite_active15
          ("%d-%d".format(unitid, os), (userid, planid, unitid, isshow, isclick, os, load, active, active15, active_third,ctsite_active2,ctsite_active15))
      }
    val osData = getTargetData(inputOsData, "os", argDay)
    insertData = insertData.union(osData).repartition(50)

    val inputProvinceData = allData
      .map {
        x =>
          val userid = x._2.userid
          val planid = x._2.planid
          val unitid = x._2.unitid
          val isshow = x._2.isshow
          val isclick = x._2.isclick
          val province = x._2.province
          val load = x._2.load
          val active = x._2.active
          val active15 = x._2.active15
          val active_third = x._2.active_third
          val ctsite_active2 = x._2.ctsite_active2
          val ctsite_active15 = x._2.ctsite_active15
          ("%d-%d".format(unitid, province), (userid, planid, unitid, isshow, isclick, province, load, active, active15, active_third,ctsite_active2,ctsite_active15))
      }
    val provinceData = getTargetData(inputProvinceData, "province", argDay)
    insertData = insertData.union(provinceData)

    val inputPhoneLevelData = allData
      .map {
        x =>
          val userid = x._2.userid
          val planid = x._2.planid
          val unitid = x._2.unitid
          val isshow = x._2.isshow
          val isclick = x._2.isclick
          val phone_level = x._2.phoneLevel
          val load = x._2.load
          val active = x._2.active
          val active15 = x._2.active15
          val active_third = x._2.active_third
          val ctsite_active2 = x._2.ctsite_active2
          val ctsite_active15 = x._2.ctsite_active15
          ("%d-%d".format(unitid, phone_level), (userid, planid, unitid, isshow, isclick, phone_level, load, active, active15, active_third,ctsite_active2,ctsite_active15))
      }
    val phoneLevelData = getTargetData(inputPhoneLevelData, "phone_level", argDay)
    insertData = insertData.union(phoneLevelData)

    val inputHourData = allData
      .map {
        x =>
          val userid = x._2.userid
          val planid = x._2.planid
          val unitid = x._2.unitid
          val isshow = x._2.isshow
          val isclick = x._2.isclick
          val hour = x._2.hour
          val load = x._2.load
          val active = x._2.active
          val active15 = x._2.active15
          val active_third = x._2.active_third
          val ctsite_active2 = x._2.ctsite_active2
          val ctsite_active15 = x._2.ctsite_active15
          ("%d-%d".format(unitid, hour), (userid, planid, unitid, isshow, isclick, hour, load, active, active15, active_third,ctsite_active2,ctsite_active15))
      }
    val hourData = getTargetData(inputHourData, "hour", argDay)
    insertData = insertData.union(hourData)

    println(insertData.count())

    val inputNetworkTypeData = allData
      .map {
        x =>
          val userid = x._2.userid
          val planid = x._2.planid
          val unitid = x._2.unitid
          val isshow = x._2.isshow
          val isclick = x._2.isclick
          val networkType = x._2.network
          val load = x._2.load
          val active = x._2.active
          val active15 = x._2.active15
          val active_third = x._2.active_third
          val ctsite_active2 = x._2.ctsite_active2
          val ctsite_active15 = x._2.ctsite_active15
          ("%d-%d".format(unitid, networkType), (userid, planid, unitid, isshow, isclick, networkType, load, active, active15, active_third,ctsite_active2,ctsite_active15))
      }
    val networkTypeData = getTargetData(inputNetworkTypeData, "network_type", argDay)
    insertData = insertData.union(networkTypeData)

    //------------
    val inputUserLevelData = allData
      .map {
        x =>
          val userid = x._2.userid
          val planid = x._2.planid
          val unitid = x._2.unitid
          val isshow = x._2.isshow
          val isclick = x._2.isclick
          val user_level = x._2.userLevel
          val load = x._2.load
          val active = x._2.active
          val active15 = x._2.active15
          val active_third = x._2.active_third
          val ctsite_active2 = x._2.ctsite_active2
          val ctsite_active15 = x._2.ctsite_active15
          ("%d-%d".format(unitid, user_level), (userid, planid, unitid, isshow, isclick, user_level, load, active, active15, active_third,ctsite_active2,ctsite_active15))
      }
    val userLevelData = getTargetData(inputUserLevelData, "user_level", argDay)
    insertData = insertData.union(userLevelData)

    val inputQukanNewUserData = allData
      .map {
        x =>
          val userid = x._2.userid
          val planid = x._2.planid
          val unitid = x._2.unitid
          val isshow = x._2.isshow
          val isclick = x._2.isclick
          val userOrient = x._2.qukanNewUser
          val load = x._2.load
          val active = x._2.active
          val active15 = x._2.active15
          val active_third = x._2.active_third
          val ctsite_active2 = x._2.ctsite_active2
          val ctsite_active15 = x._2.ctsite_active15
          ("%d-%d".format(unitid, userOrient), (userid, planid, unitid, isshow, isclick, userOrient, load, active, active15, active_third,ctsite_active2,ctsite_active15))
      }
    val qukanNewUserData = getTargetData(inputQukanNewUserData, "user_orient", argDay)
    insertData = insertData.union(qukanNewUserData)

    val inputAdslotTypeData = allData
      .map {
        x =>
          val userid = x._2.userid
          val planid = x._2.planid
          val unitid = x._2.unitid
          val isshow = x._2.isshow
          val isclick = x._2.isclick
          val adslotType = x._2.adslotType
          val load = x._2.load
          val active = x._2.active
          val active15 = x._2.active15
          val active_third = x._2.active_third
          val ctsite_active2 = x._2.ctsite_active2
          val ctsite_active15 = x._2.ctsite_active15
          ("%d-%d".format(unitid, adslotType), (userid, planid, unitid, isshow, isclick, adslotType, load, active, active15, active_third,ctsite_active2,ctsite_active15))
      }
    val adslotTypeData = getTargetData(inputAdslotTypeData, "adslot_type", argDay)
    insertData = insertData.union(adslotTypeData).repartition(50)
    println(insertData.count())

    val inputAdslotTypeMediaData = allData
      .filter {
        x =>
          val mediaId = x._2.mediaid
          (mediaId == "80000001") || (mediaId == "80000002")
      }
      .map {
        x =>
          val userid = x._2.userid
          val planid = x._2.planid
          val unitid = x._2.unitid
          val isshow = x._2.isshow
          val isclick = x._2.isclick
          val adslotType = x._2.adslotType
          val load = x._2.load
          val active = x._2.active
          val active15 = x._2.active15
          val active_third = x._2.active_third
          val ctsite_active2 = x._2.ctsite_active2
          val ctsite_active15 = x._2.ctsite_active15
          ("%d-%d".format(unitid, adslotType), (userid, planid, unitid, isshow, isclick, adslotType, load, active, active15, active_third,ctsite_active2,ctsite_active15))
      }
    val adslotTypeMediaData = getTargetData(inputAdslotTypeMediaData, "adslot_type_media", argDay)
    insertData = insertData.union(adslotTypeMediaData)

    val inputAdslotIdData = allData
      .map {
        x =>
          val userid = x._2.userid
          val planid = x._2.planid
          val unitid = x._2.unitid
          val isshow = x._2.isshow
          val isclick = x._2.isclick
          val adslotid = x._2.adslotid
          val load = x._2.load
          val active = x._2.active
          val active15 = x._2.active15
          val active_third = x._2.active_third
          val ctsite_active2 = x._2.ctsite_active2
          val ctsite_active15 = x._2.ctsite_active15
          ("%d-%d".format(unitid, adslotid), (userid, planid, unitid, isshow, isclick, adslotid, load, active, active15, active_third,ctsite_active2,ctsite_active15))
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
        "date", "active15", "active_third", "ctsite_active2", "ctsite_active15")
    insertDataFrame.show(50)

    clearReportUnitTarget(argDay)

    insertDataFrame
      .write
      .mode(SaveMode.Append)
      .jdbc(mariadbUrl, "report.report_unit_target", mariadbProp)
  }

  def getTargetData(data: RDD[(String, (Int, Int, Int, Int, Int, Int, Int, Int, Int, Int,Int,Int))],
                    target_type: String, date: String): (RDD[(Int, Int, Int, Int, Int, String, Int, Int, Int, String, Int, Int,Int,Int)]) = {
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
          val ctsite_active2 = a._11+b._11
          val ctsite_active15 = a._12+b._12
          (userid, planid, unitid, isshow, isclick, target_value, load, active, active15, active_third,ctsite_active2,ctsite_active15)
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
          val ctsite_active2 = x._2._11
          val ctsite_active15 = x._2._12
          (userid, planid, unitid, isshow, isclick, target_type, target_value, load, active, date, active15, active_third,ctsite_active2,ctsite_active15)
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

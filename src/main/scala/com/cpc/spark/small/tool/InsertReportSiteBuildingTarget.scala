package com.cpc.spark.small.tool

import java.sql.DriverManager
import java.text.SimpleDateFormat
import java.util.{Date, Properties}

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

  var mariaAmateurdbUrl = ""
  val mariaAmateurdbProp = new Properties()

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
                   active: Long = 0,
                   landpage_ok: Long = 0,
                   stayinwx: Long = 0,
                   adslotid: Int = 0,
                   brand: String = "",
                   browserType: Int = 0,
                   isStudent: Int = 0, //0未知，1学生，2非学生,
                   active1: Long = 0,
                   active2: Long = 0,
                   activexc: Long = 0,
                   activexd: Long = 0,
                   activexf: Long = 0,
                   activexg: Long = 0,
                   activexh: Long = 0
                 )


  def main(args: Array[String]): Unit = {
    Logger.getRootLogger.setLevel(Level.WARN)
    val argDay = args(0).toString
    val argIsHour = args(1).toInt

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

    val confAmateur = ConfigFactory.load()
    mariaAmateurdbUrl = confAmateur.getString("mariadb.amateur_write.url")
    mariaAmateurdbProp.put("user", confAmateur.getString("mariadb.amateur_write.user"))
    mariaAmateurdbProp.put("password", confAmateur.getString("mariadb.amateur_write.password"))
    mariaAmateurdbProp.put("driver", confAmateur.getString("mariadb.amateur_write.driver"))

    val ctx = SparkSession
      .builder()
      .config("spark.debug.maxToStringFields", "2000")
      .appName("InsertReportSiteBuildingTarget is run day is %s".format(argDay))
      .enableHiveSupport()
      .getOrCreate()

    println("InsertReportSiteBuildingTarget is run day is %s".format(argDay))

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

    val unionData = ctx
      .sql(
        """
          |SELECT searchid,ideaid,isshow,isclick,sex,age,os,province,ext['phone_level'].int_value,hour,
          |network,coin,ext['qukan_new_user'].int_value,adslot_type,media_appsid,adslotid,brand,ext_int["browser_type"],
          |ext_int["siteid"],interests
          |FROM dl_cpc.cpc_union_log
          |WHERE date="%s" AND (isshow+isclick)>0 AND ext_int["siteid"]>0
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
          val adslotid = x.getString(15).toInt
          val brand = if (x.get(16) != null) x.get(16).toString else ""
          val browserType = x.get(17).toString.toInt

          val siteid = x.get(18).toString.toInt
          val interests = x.get(19).toString
          val isStudent = if (interests.contains("224=")) 1 else if (interests.contains("225=")) 2 else 0

          (searchid, (Info(siteid, ideaid, isshow, isclick, sex, age, os, province, phoneLevel, hour,
            network, userLevel, qukanNewUser, adslotType, mediaId, 0, 0, 0, 0, adslotid, brand, browserType, isStudent)))
      }
      .repartition(50)
    //println("unionData count", unionData.count())


    val traceData = ctx.sql(
      """
        |SELECT DISTINCT cutl.searchid,cutl.trace_type,cutl.duration,cutl.trace_op1,cul.ext_int["siteid"]
        |FROM dl_cpc.cpc_union_trace_log cutl
        |INNER JOIN dl_cpc.cpc_union_log cul ON cutl.searchid=cul.searchid
        |WHERE cutl.date="%s" AND cul.date="%s" AND cul.isclick>0 AND cul.ideaid>0 AND cul.userid>0 AND cul.ext_int["siteid"]>0
      """.stripMargin.format(argDay, argDay))
      .rdd
      .map {
        x =>
          val searchid = x.getString(0)
          val trace_type = x.getString(1)
          var load = 0
          var active = 0
          if ((trace_type == "active1") || (trace_type == "active2") || (trace_type == "active3") || (trace_type == "active4") || (trace_type == "active5")) {
            active += 1
          }

          trace_type match {
            case "load" => load += 1
            //            case s if s.startsWith("active") => active += 1
            case "disactive" => active -= 1
            //            //case "press" => active += 1
            case _ =>
          }

          val active1 = if (trace_type == "active1") 1 else 0
          val active2 = if (trace_type == "active2") 1 else 0

          var traceOp1 = x.getString(3)
          var landpage_ok = 0
          var stayinwx = 0
          if (trace_type == "lpload") {
            if (traceOp1 == "REPORT_USER_STAYINWX") {
              stayinwx = 1
            } else if (traceOp1 == "REPORT_LANDPAGE_OK") {
              landpage_ok = 1
            }
          }
          val siteid = x.get(4).toString.toInt

          (searchid, (Info(siteid, -1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, load, active, landpage_ok, stayinwx, 0, "", 0, 0, active1, active2)))
      }
      .repartition(50)
    //println("traceData count", traceData.count())


    val allData = unionData
      .union(traceData)
      .reduceByKey {
        (a, b) =>
          val siteId = if (a.ideaid != -1) a.siteId else b.siteId
          val ideaid = if (a.ideaid != -1) a.ideaid else b.ideaid
          val isshow = if (a.ideaid != -1) a.isshow else b.isshow
          val isclick = if (a.ideaid != -1) a.isclick else b.isclick
          val sex = if (a.ideaid != -1) a.sex else b.sex
          val age = if (a.ideaid != -1) a.age else b.age
          val os = if (a.ideaid != -1) a.os else b.os
          val province = if (a.ideaid != -1) a.province else b.province
          val phoneLevel = if (a.ideaid != -1) a.phoneLevel else b.phoneLevel
          val hour = if (a.ideaid != -1) a.hour else b.hour
          val network = if (a.ideaid != -1) a.network else b.network
          val userLevel = if (a.ideaid != -1) a.userLevel else b.userLevel
          val qukanNewUser = if (a.ideaid != -1) a.qukanNewUser else b.qukanNewUser
          val adslotType = if (a.ideaid != -1) a.adslotType else b.adslotType
          val mediaid = if (a.ideaid != -1) a.mediaid else b.mediaid
          val load = a.load + b.load
          val active = a.active + b.active
          val landpage_ok = a.landpage_ok + b.landpage_ok
          val stayinwx = a.stayinwx + b.stayinwx
          val adslotid = if (a.ideaid != -1) a.adslotid else b.adslotid
          val brand = if (a.ideaid != -1) a.brand else b.brand
          val browserType = if (a.ideaid != -1) a.browserType else b.browserType
          val isStudent = if (a.ideaid != -1) a.isStudent else b.isStudent
          val active1 = a.active1 + b.active1
          val active2 = a.active2 + b.active2
          Info(siteId, ideaid, isshow, isclick, sex, age, os, province, phoneLevel, hour, network, userLevel, qukanNewUser, adslotType,
            mediaid, load, active, landpage_ok, stayinwx, adslotid, brand, browserType, isStudent, active1, active2)
      }
      .map {
        x =>
          val info = x._2
          (0, Info(info.siteId, info.ideaid, info.isshow, info.isclick, info.sex, info.age, info.os, info.province, info.phoneLevel, info.hour,
            info.network, info.userLevel, info.qukanNewUser, info.adslotType, info.mediaid, info.load, info.active, info.landpage_ok, info.stayinwx,
            info.adslotid, info.brand, info.browserType, info.isStudent, info.active1, info.active2))
      }
      .filter(_._2.siteId > 0)
      .repartition(50)
      .cache()

    val inputStudentData = allData
      //.repartition(50)
      .map {
      x =>
        val info = x._2
        val siteId = info.siteId
        val isshow = info.isshow
        val isclick = info.isclick
        val typeVal = info.isStudent
        val load = info.load
        val active = info.active
        val landpage_ok = info.landpage_ok
        val stayinwx = info.stayinwx
        val active1 = info.active1
        val active2 = info.active2
        ((siteId, typeVal), (siteId, isshow, isclick, typeVal, load, active, landpage_ok, stayinwx, active1, active2))
    }
    //val studentData = getTargetData(inputStudentData, "student", argDay)
    var insertAllData = getTargetData(inputStudentData, "student", argDay)

    val inputBrandData = allData
      .repartition(50)
      .filter {
        x =>
          val mediaId = x._2.mediaid
          val adslotType = x._2.adslotType
          ((mediaId == 80000001) || (mediaId == 80000002)) && (adslotType == 1)
      }
      .map {
        x =>
          val info = x._2
          val siteId = info.siteId
          val isshow = info.isshow
          val isclick = info.isclick
          var typeVal = if (broadcastBrandMaps.value.contains(info.brand.toLowerCase)) broadcastBrandMaps.value(info.brand.toLowerCase) else 0
          val load = info.load
          val active = info.active
          val landpage_ok = info.landpage_ok
          val stayinwx = info.stayinwx
          val active1 = info.active1
          val active2 = info.active2
          ((siteId, typeVal), (siteId, isshow, isclick, typeVal, load, active, landpage_ok, stayinwx, active1, active2))
      }

    val brandData = getTargetData(inputBrandData, "brand", argDay)
    //println("brandData count is", brandData.count())
    insertAllData = insertAllData.union(brandData)


    val inputBrowserTypeData = allData
      //.repartition(50)
      .filter {
      x =>
        val mediaId = x._2.mediaid
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
          val info = x._2
          val siteId = info.siteId
          val isshow = info.isshow
          val isclick = info.isclick
          var typeVal = info.browserType
          val load = info.load
          val active = info.active
          val landpage_ok = info.landpage_ok
          val stayinwx = info.stayinwx
          val active1 = info.active1
          val active2 = info.active2
          ((siteId, typeVal), (siteId, isshow, isclick, typeVal, load, active, landpage_ok, stayinwx, active1, active2))
      }

    val browserTypeData = getTargetData(inputBrowserTypeData, "browser_type", argDay)
    //println("browserTypeData count is", browserTypeData.count())
    insertAllData = insertAllData.union(browserTypeData)

    val inputAdslotIdData = allData
      //.repartition(50)
      .map {
      x =>
        val info = x._2
        val siteId = info.siteId
        val isshow = info.isshow
        val isclick = info.isclick
        val typeVal = info.adslotid
        val load = info.load
        val active = info.active
        val landpage_ok = info.landpage_ok
        val stayinwx = info.stayinwx
        val active1 = info.active1
        val active2 = info.active2
        ((siteId, typeVal), (siteId, isshow, isclick, typeVal, load, active, landpage_ok, stayinwx, active1, active2))
    }
    val adslotIdData = getTargetData(inputAdslotIdData, "adslot_id", argDay)
    insertAllData = insertAllData.union(adslotIdData)

    val inputSexData = allData
      //.repartition(50)
      .map {
      x =>
        val info = x._2
        val siteId = info.siteId
        val isshow = info.isshow
        val isclick = info.isclick
        val typeVal = info.sex
        val load = info.load
        val active = info.active
        val landpage_ok = info.landpage_ok
        val stayinwx = info.stayinwx
        val active1 = info.active1
        val active2 = info.active2
        ((siteId, typeVal), (siteId, isshow, isclick, typeVal, load, active, landpage_ok, stayinwx, active1, active2))
    }
    val sexData = getTargetData(inputSexData, "sex", argDay)
    insertAllData = insertAllData.union(sexData)


    val inputAgeData = allData
      //.repartition(50)
      .map {
      x =>
        val info = x._2
        val siteId = info.siteId
        val isshow = info.isshow
        val isclick = info.isclick
        val typeVal = info.age
        val load = info.load
        val active = info.active
        val landpage_ok = info.landpage_ok
        val stayinwx = info.stayinwx
        val active1 = info.active1
        val active2 = info.active2
        ((siteId, typeVal), (siteId, isshow, isclick, typeVal, load, active, landpage_ok, stayinwx, active1, active2))
    }
    val ageData = getTargetData(inputAgeData, "age", argDay)
    insertAllData = insertAllData.union(ageData)

    val inputOsData = allData
      //.repartition(50)
      .map {
      x =>
        val info = x._2
        val siteId = info.siteId
        val isshow = info.isshow
        val isclick = info.isclick
        val typeVal = info.os
        val load = info.load
        val active = info.active
        val landpage_ok = info.landpage_ok
        val stayinwx = info.stayinwx
        val active1 = info.active1
        val active2 = info.active2
        ((siteId, typeVal), (siteId, isshow, isclick, typeVal, load, active, landpage_ok, stayinwx, active1, active2))
    }
    val osData = getTargetData(inputOsData, "os", argDay)
    insertAllData = insertAllData.union(osData)

    val inputProvinceData = allData
      //.repartition(50)
      .map {
      x =>
        val info = x._2
        val siteId = info.siteId
        val isshow = info.isshow
        val isclick = info.isclick
        val typeVal = info.province
        val load = info.load
        val active = info.active
        val landpage_ok = info.landpage_ok
        val stayinwx = info.stayinwx
        val active1 = info.active1
        val active2 = info.active2
        ((siteId, typeVal), (siteId, isshow, isclick, typeVal, load, active, landpage_ok, stayinwx, active1, active2))
    }
    val provinceData = getTargetData(inputProvinceData, "province", argDay)
    insertAllData = insertAllData.union(provinceData)

    val inputPhoneLevelData = allData
      //.repartition(50)
      .map {
      x =>
        val info = x._2
        val siteId = info.siteId
        val isshow = info.isshow
        val isclick = info.isclick
        val typeVal = info.phoneLevel
        val load = info.load
        val active = info.active
        val landpage_ok = info.landpage_ok
        val stayinwx = info.stayinwx
        val active1 = info.active1
        val active2 = info.active2
        ((siteId, typeVal), (siteId, isshow, isclick, typeVal, load, active, landpage_ok, stayinwx, active1, active2))
    }

    val phoneLevelData = getTargetData(inputPhoneLevelData, "phone_level", argDay)
    //println("phone_level count is", phoneLevelData.count())
    insertAllData = insertAllData.union(phoneLevelData)


    val inputHourData = allData
      //.repartition(50)
      .map {
      x =>
        val info = x._2
        val siteId = info.siteId
        val isshow = info.isshow
        val isclick = info.isclick
        val typeVal = info.hour
        val load = info.load
        val active = info.active
        val landpage_ok = info.landpage_ok
        val stayinwx = info.stayinwx
        val active1 = info.active1
        val active2 = info.active2
        ((siteId, typeVal), (siteId, isshow, isclick, typeVal, load, active, landpage_ok, stayinwx, active1, active2))
    }

    val hourData = getTargetData(inputHourData, "hour", argDay)
    //println("hour count is", hourData.count())
    insertAllData = insertAllData.union(hourData)


    val inputNetworkData = allData
      //.repartition(50)
      .map {
      x =>
        val info = x._2
        val siteId = info.siteId
        val isshow = info.isshow
        val isclick = info.isclick
        val typeVal = info.network
        val load = info.load
        val active = info.active
        val landpage_ok = info.landpage_ok
        val stayinwx = info.stayinwx
        val active1 = info.active1
        val active2 = info.active2
        ((siteId, typeVal), (siteId, isshow, isclick, typeVal, load, active, landpage_ok, stayinwx, active1, active2))
    }

    val networkData = getTargetData(inputNetworkData, "network_type", argDay).cache()
    //println("network_type count is", networkData.count())
    insertAllData = insertAllData.union(networkData)


    val inputUserLevelData = allData
      //.repartition(50)
      .map {
      x =>
        val info = x._2
        val siteId = info.siteId
        val isshow = info.isshow
        val isclick = info.isclick
        val typeVal = info.userLevel
        val load = info.load
        val active = info.active
        val landpage_ok = info.landpage_ok
        val stayinwx = info.stayinwx
        val active1 = info.active1
        val active2 = info.active2
        ((siteId, typeVal), (siteId, isshow, isclick, typeVal, load, active, landpage_ok, stayinwx, active1, active2))
    }

    val userLevelData = getTargetData(inputUserLevelData, "user_level", argDay)
    //println("user_level count is", userLevelData.count())
    insertAllData = insertAllData.union(userLevelData)

    val inputQukanNewUserData = allData
      //.repartition(50)
      .map {
      x =>
        val info = x._2
        val siteId = info.siteId
        val isshow = info.isshow
        val isclick = info.isclick
        val typeVal = info.qukanNewUser
        val load = info.load
        val active = info.active
        val landpage_ok = info.landpage_ok
        val stayinwx = info.stayinwx
        val active1 = info.active1
        val active2 = info.active2
        ((siteId, typeVal), (siteId, isshow, isclick, typeVal, load, active, landpage_ok, stayinwx, active1, active2))
    }

    val qukanNewUserData = getTargetData(inputQukanNewUserData, "user_orient", argDay)
    //println("user_orient count is", qukanNewUserData.count())
    insertAllData = insertAllData.union(qukanNewUserData)

    val inputAdslotTypeData = allData
      //.repartition(50)
      .map {
      x =>
        val info = x._2
        val siteId = info.siteId
        val isshow = info.isshow
        val isclick = info.isclick
        val typeVal = info.adslotType
        val load = info.load
        val active = info.active
        val landpage_ok = info.landpage_ok
        val stayinwx = info.stayinwx
        val active1 = info.active1
        val active2 = info.active2
        ((siteId, typeVal), (siteId, isshow, isclick, typeVal, load, active, landpage_ok, stayinwx, active1, active2))
    }

    val adslotTypeData = getTargetData(inputAdslotTypeData, "adslot_type", argDay)
    //println("adslot_type count is", adslotTypeData.count())
    insertAllData = insertAllData.union(adslotTypeData)

    val inputQuAdslotTypeData = allData
      //.repartition(50)
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
          val landpage_ok = info.landpage_ok
          val stayinwx = info.stayinwx
          val active1 = info.active1
          val active2 = info.active2
          ((siteId, typeVal), (siteId, isshow, isclick, typeVal, load, active, landpage_ok, stayinwx, active1, active2))
      }
    val quAdslotTypeData = getTargetData(inputQuAdslotTypeData, "adslot_type_media", argDay)
    //println("adslot_type_media count is", quAdslotTypeData.count())
    insertAllData = insertAllData.union(quAdslotTypeData).repartition(50)

    allData.unpersist()

    //    val insertAllData = sexData
    //      .union(ageData)
    //      .union(osData)
    //      .union(provinceData)
    //      .union(phoneLevelData)
    //      .union(hourData)
    //      .union(networkData)
    //      .union(userLevelData)
    //      .union(qukanNewUserData)
    //      .union(adslotTypeData)
    //      .union(quAdslotTypeData)
    //      .union(adslotIdData)
    //      .union(brandData)
    //      .union(browserTypeData)
    //      .union(studentData)
    //      .repartition(50)


    var insertDataFrame = ctx.createDataFrame(insertAllData)
      .toDF("site_id", "impression", "click", "target_type", "target_value", "load", "active", "date", "sdk_ok",
        "stayinwx", "activexa", "activexb", "activexc", "activexd", "activexf", "activexg", "activexh")
      .repartition(50)
    //insertDataFrame.show(100)

    if (insertDataFrame.count() > 0) {
      val now: Date = new Date()
      val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
      val toDay = dateFormat.format(now)

      if ((toDay != argDay) && (argIsHour == 1)) {
        println("toDay != argDay is", toDay != argDay, toDay, argDay, argIsHour)
        return
      }

      //report
      clearReportSiteBuildingTarget(argDay)

      insertDataFrame
        .write
        .mode(SaveMode.Append)
        .jdbc(mariaReportdbUrl, "report.report_site_building_target", mariaReportdbProp)
      println("report over!")

      //amateur
      clearReportSiteBuildingTargetByAmateur(argDay)

      insertDataFrame
        .write
        .mode(SaveMode.Append)
        .jdbc(mariaAmateurdbUrl, "report.report_site_building_target", mariaAmateurdbProp)
      println("Amateur over!")

    }

    //    //report
    //    clearReportSiteBuildingTarget(argDay)
    //
    //    insertDataFrame
    //      .write
    //      .mode(SaveMode.Append)
    //      .jdbc(mariaReportdbUrl, "report.report_site_building_target", mariaReportdbProp)
    //    println("report over!")
    //
    //    //amateur
    //    clearReportSiteBuildingTargetByAmateur(argDay)
    //
    //    insertDataFrame
    //      .write
    //      .mode(SaveMode.Append)
    //      .jdbc(mariaAmateurdbUrl, "report.report_site_building_target", mariaAmateurdbProp)
    //    println("Amateur over!")

    //insertDataFrame.unpersist()

  }

  def getTargetData(data: RDD[((Int, Int), (Int, Long, Long, Int, Long, Long, Long, Long, Long, Long))],
                    target_type: String, argDay: String): (RDD[(Int, Long, Long, String, Int, Long, Long, String, Long, Long, Long, Long, Long, Long, Long, Long, Long)]) = {
    data
      .reduceByKey {
        (a, b) =>
          val siteId = a._1
          val isshow = a._2 + b._2
          val isclick = a._3 + b._3
          val typeVal = a._4
          val load = a._5 + b._5
          val active = a._6 + b._6
          val landpage_ok = a._7 + b._7
          val stayinwx = a._8 + b._8
          val active1 = a._9 + b._9
          val active2 = a._10 + b._10
          (siteId, isshow, isclick, typeVal, load, active, landpage_ok, stayinwx, active1, active2)
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
          val landpage_ok = x._2._7
          val stayinwx = x._2._8
          val active1 = x._2._9
          val active2 = x._2._10
          (siteId, isshow, isclick, targetType, typeVal, load, active, argDay, landpage_ok, stayinwx, active1, active2, 0, 0, 0, 0, 0)
      }
    //.repartition(50)
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

  def clearReportSiteBuildingTargetByAmateur(date: String): Unit = {
    try {
      Class.forName(mariaAmateurdbProp.getProperty("driver"))
      val conn = DriverManager.getConnection(
        mariaAmateurdbUrl,
        mariaAmateurdbProp.getProperty("user"),
        mariaAmateurdbProp.getProperty("password"))
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

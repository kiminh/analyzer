package com.cpc.spark.small.tool

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

/**
  * Created by wanli on 2017/8/4.
  */
object GetCvrInfo {
  def main(args: Array[String]): Unit = {
    Logger.getRootLogger().setLevel(Level.WARN)

    //"ideaid=0;adslotid=;media_appsid=;adclass=0;xModel=;dateDay=2017-08-15"
    var ideaid = 0
    var adslotid = ""
    var mediaAppsid = ""
    var adclass = 0
    var xModel = ""
    var dateDay = ""
    var path = ""

    args(0).split(";").map(_.split("=")).foreach {
      x =>
        x(0) match {
          case "ideaid" => ideaid = x(1).toInt
          case "adslotid" => adslotid = x(1)
          case "mediaAppsid" => mediaAppsid = x(1)
          case "adclass" => adclass = x(1).toInt
          case "xModel" => xModel = x(1)
          case "dateDay" => dateDay = x(1)
          case "path" => path = x(1)
          case _ =>
        }
    }

    val ctx = SparkSession.builder()
      .appName("small tool GetCvrInfo")
      .enableHiveSupport()
      .getOrCreate()

    val traceRdd = ctx.sql(
      """
        |SELECT DISTINCT
        |  searchid
        |  , trace_type
        |  , duration
        |FROM dl_cpc.cpc_basedata_trace_event
        |WHERE day='%s'
      """.stripMargin.format(dateDay)).rdd
      .map {
        x =>
          var trace_type = ""
          var load, active, buttonClick, press, stay1, stay5, stay10, stay30, stay60, stay120 = 0

          x.getString(1) match {
            case "load" => load += 1
            case s if s.startsWith("active") => active += 1
            case "buttonClick" => buttonClick += 1
            //case "press" => press += 1
            case "stay" => x.getInt(2) match {
              case 1 => stay1 += 1
              case 5 => stay5 += 1
              case 10 => stay10 += 1
              case 30 => stay30 += 1
              case 60 => stay60 += 1
              case 120 => stay120 += 1
              case _ =>
            }
            case _ =>
          }
          (x.getString(0), (x.getString(0), load, active, buttonClick, press, stay1, stay5, stay10, stay30, stay60, stay120))
      }
      .reduceByKey {
        (a, b) =>
          (a._1, a._2 + b._2, a._3 + b._3, a._4 + b._4, a._5 + b._5, a._6 + b._6, a._7 + b._7, a._8 + b._8, a._9 + b._9, a._10 + b._10, a._11 + b._11)
      }
      .cache()

    val xActionRdd = traceRdd.map {
      x =>
        var xAction = 0
        if ((x._2._3 + x._2._4 + x._2._5) > 0) {
          xAction = 1
        }
        (x._2._1, xAction)
    }
      .reduceByKey {
        (a, b) =>
          var xAction = 0
          if ((a + b) > 0) {
            xAction = 1
          }
          (1)
      }
      .map {
        x =>
          (x._1, (x._1, 0, 0, 0, "", "", 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, "", "", 0, x._2))
      }


    val traceData = traceRdd.map {
      x =>
        (x._2._1, (x._2._1, 0, 0, 0, "", "", 0, x._2._2, x._2._3, x._2._4, x._2._5, x._2._6, x._2._7, x._2._8, x._2._9, x._2._10, x._2._11, "", "", 0, 0))
    }.union(xActionRdd)
    var dynamicSql = ""
    if (ideaid > 0) {
      dynamicSql += " AND ideaid IN (%d) ".format(ideaid)
    }

    if (adslotid.length > 0) {
      dynamicSql += " AND adslotid IN (%s) ".format(adslotid)
    }

    if (mediaAppsid.length > 0) {
      dynamicSql += " AND media_appsid IN (%s) ".format(mediaAppsid)
    }

    if (adclass > 0) {
      dynamicSql += " AND ext['adclass'].int_value IN (%d) ".format(adclass)
    }

    val unionLogSql =
      """
        |SELECT DISTINCT
        |  searchid
        |  , isshow
        |  , isclick
        |  , price
        |  , media_appsid
        |  , adslotid
        |  , adslot_type
        |  , hour
        |  , exptags
        |  , adclass
        |FROM dl_cpc.cpc_basedata_union_events
        |WHERE day="%s"
        |  AND (coalesce(isshow, 0)+coalesce(isclick, 0))>0 %s
      """.stripMargin.format(dateDay, dynamicSql)

    val unionLogRdd = ctx.sql(unionLogSql).rdd
      .map {
        x =>
          var exptags = ""
          if (xModel.length > 0) {
            exptags = x.getString(8).split(",").find(_.startsWith(xModel)).getOrElse("base")
          }

          var tmpAdclass = 0
          if (adclass > 0) {
            tmpAdclass = x.getInt(9)
          }

          (x.getString(0), (x.getString(0), x.getInt(1), x.getInt(2), x.getInt(3), x.getString(4), x.getString(5), x.getInt(6),
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, x.getString(7), exptags, tmpAdclass, 0))
      }
      .union(traceData)
      .reduceByKey {
        //searchid
        (a, b) =>
          var xa = b
          if ((a._2 + a._3) > 0) {
            xa = a
          }
          var price = 0
          if (a._3 > 0) {
            price += a._4
          }
          if (b._3 > 0) {
            price += b._4
          }

          (a._1, a._2 + b._2, a._3 + b._3, price, xa._5, xa._6, xa._7, a._8 + b._8, a._9 + b._9, a._10 + b._10, a._11 + b._11, a._12 + b._12, a._13 + b._13,
            a._14 + b._14, a._15 + b._15, a._16 + b._16, a._17 + b._17, xa._18, xa._19, xa._20, a._21 + b._21)
      }
      .map {
        x =>
          //hour+mediaAppsid+adslotid
          var key = x._2._18 + x._2._5 + x._2._6

          if (xModel.length > 0) {
            key += x._2._19
          }

          if (adclass > 0) {
            key += x._2._20.toString
          }

          (key, (x._2))
      }
      .reduceByKey {
        (a, b) =>
          var price = 0
          if (a._3 > 0) {
            price += a._4
          }
          if (b._3 > 0) {
            price += b._4
          }
          (a._1, a._2 + b._2, a._3 + b._3, price, a._5, a._6, a._7, a._8 + b._8, a._9 + b._9, a._10 + b._10, a._11 + b._11, a._12 + b._12, a._13 + b._13,
            a._14 + b._14, a._15 + b._15, a._16 + b._16, a._17 + b._17, a._18, a._19, a._20, a._21 + b._21)
      }
      .filter(_._2._6.length > 0)
      .map {
        x =>
          //mediaid,adslotid,adslot_type,hour,price,isshow,isclick,load,xaction,xaction/load,active, buttonClick,
          // press, stay1, stay5, stay10, stay30, stay60, stay120,exptags,media_class
          //          (x._2._5, x._2._6, x._2._7, x._2._18, x._2._4, x._2._2, x._2._3, x._2._8, x._2._9, x._2._10, x._2._11, x._2._12, x._2._13, x._2._14, x._2._15,
          //            x._2._16, x._2._17, x._2._19, x._2._20)
          var loadXaction = ""
          if (x._2._8 > 0) {
            loadXaction = ((x._2._21.toDouble / x._2._8.toDouble) * 100).formatted("%.3f")
          }

          "%s,%s,%d,%s,%d,%d,%d,%d,%d,%s,%d,%d,%d,%d,%d,%d,%d,%d,%d,%s,%d".format(x._2._5, x._2._6, x._2._7,
            x._2._18, x._2._4, x._2._2, x._2._3, x._2._8, x._2._21, loadXaction, x._2._9, x._2._10, x._2._11, x._2._12, x._2._13, x._2._14,
            x._2._15, x._2._16, x._2._17, x._2._19, x._2._20)
      }
      .saveAsTextFile("/user/cpc/wl/test/small-tool-GetCvrInfo/" + path)
  }
}
package com.cpc.spark.ml.antimodel.v2

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

/**
  * Created by roydong on 06/07/2017.
  */
object AntispamUserAnal {

  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      System.err.println(
        s"""
           |Usage: day <date>
        """.stripMargin)
      System.exit(1)
    }
    val date = args(0)
    Logger.getRootLogger.setLevel(Level.WARN)
    val ctx = SparkSession.builder()
      .appName("model user anal" + date)
      .enableHiveSupport()
      .getOrCreate()
    var sql1 = " select uid,hour,adslot_type,ext['antispam'].int_value, sum(isshow), sum(isclick) FROM dl_cpc.cpc_union_log where `date` ='%s'  group by uid,hour,adslot_type,ext['antispam'].int_value".format(date)


    println("sql1:"+ sql1)

   var union =  ctx.sql(sql1).rdd.map{
      x =>
        val device = x(0).toString()
        val hour = x(1).toString()
        val adslotType = x(2).toString().toInt
        val antispam = x(3).toString().toInt
        val show = x(4).toString().toInt
        val click = x(5).toString().toInt
        ((device, hour, adslotType,antispam), (show, click))
    }
    var sql2 =  """
                   |SELECT DISTINCT tr.searchid,tr.trace_type,tr.duration, un.uid,un.hour,un.adslot_type,un.ext['antispam'].int_value
                   |from dl_cpc.cpc_union_trace_log as tr left join dl_cpc.cpc_union_log as un on tr.searchid = un.searchid
                   |WHERE  tr.`date` = '%s'  and un.`date` = '%s'
                 """.stripMargin.format(date,date)


    println("sql2:"+sql2)
    val traceRdd = ctx.sql(sql2).rdd
      .map {
        x =>
          var trace_type = ""
          var load, active, buttonClick, press, stay1, stay5, stay10, stay30, stay60, stay120 = 0

          x.getString(1) match {
            case "load" => load += 1
            case s if s.startsWith("active") => active += 1
            case "buttonClick" => buttonClick += 1
            case "press" => press += 1
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
          var device = x.getString(3)
          var hour = x.getString(4)
          var adslotType = x.getInt(5)
          var antispam = x.getInt(6)
          ((device,hour,adslotType,antispam),(load, active, buttonClick, press, stay1, stay5, stay10, stay30, stay60, stay120))
      }
      .reduceByKey {
        (a, b) =>
          (a._1 + b._1,a._2 + b._2, a._3 + b._3, a._4 + b._4, a._5 + b._5, a._6 + b._6, a._7 + b._7, a._8 + b._8, a._9 + b._9, a._10 + b._10)
      }
      .cache()
      union.leftOuterJoin(traceRdd).map{
        case ((device, hour, adslotType,antispam),((show, click),y:Option[(Int,Int,Int,Int,Int,Int,Int,Int,Int,Int)])) =>
          val trace =  y.getOrElse((0,0,0,0,0,0,0,0,0,0))
          (( hour, adslotType,antispam), (1,show, click, trace._1,trace._2,trace._3,trace._4,trace._5,trace._6,trace._7,trace._8,trace._9,trace._10))
      }.reduceByKey((x, y) => (x._1+y._1,x._2+y._2,x._3+y._3,x._4+y._4,
        x._5+y._5,x._6+y._6,x._7+y._7,x._8+y._8,x._9+y._9,x._10+y._10,x._11+y._11,x._12+y._12,x._13+y._13))
        .map{
          case   (( hour, adslotType,antispam), (usercount,show, click, load, active, buttonClick, press, stay1, stay5, stay10, stay30, stay60, stay120))
          =>
            var result = "" +  hour+","+usercount++","+adslotType+"," + antispam+ ","+show+","+click+","+load+","+active+","+buttonClick+","+press+","+stay1+","+stay5+","+stay10+","+stay30+","+ stay60+","+stay120
            result += ","+click.toFloat/show.toFloat*100 +","+active.toFloat/load.toFloat*100
            result
        }.collect().foreach(x =>println(x))
  }
}



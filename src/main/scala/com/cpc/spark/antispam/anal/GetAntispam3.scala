package com.cpc.spark.antispam.anal

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

/**
  * Created by wanli on 2017/8/4.
  */
object GetAntispam3 {
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
    val predict = args(1).toFloat
    val  isTrainModel= args(2)
    Logger.getRootLogger.setLevel(Level.WARN)
    val ctx = SparkSession.builder()
      .appName("model user anal" + date)
      .enableHiveSupport()
      .getOrCreate()
    /* var sql1 = (" SELECT DISTINCT searchid ,hour,adslot_type from" +
      " dl_cpc.cpc_click_log where `date` =\"%s\"  and  ext['antispam_predict'].float_value > %s ").format(date,predict)*/

    val sql1 =
      s"""
         |SELECT DISTINCT
         |  searchid
         |  , hour
         |  , adslot_type
         |  from dl_cpc.cpc_basedata_click_event
         |where day='$date'
         |  and antispam_rules like "%TRAIN_MODEL%"
       """.stripMargin

    /*if(isTrainModel == "train"){
      sql1 = ("SELECT DISTINCT searchid ,hour,adslot_type from" +
        " dl_cpc.cpc_click_log where `date` =\"%s\"  and antispam_rules like \"TRAIN_MODEL\"").format(date)
    }*/
    println("sql1:"+ sql1)
    var union =  ctx.sql(sql1).rdd.map{
      x =>
        val searchid = x(0).toString()
        val hour = x(1).toString()
        val adslotType= x(2).toString().toInt
        ((searchid,hour,adslotType), 1)
    }


    val sql2 =
      s"""
         |SELECT DISTINCT
         |  tr.searchid
         |  , tr.trace_type
         |  , tr.duration
         |  , un.uid
         |  , un.hour
         |  , un.adslot_type
         |  , un.spam_click
         |from dl_cpc.cpc_basedata_trace_event as tr
         |left join dl_cpc.cpc_basedata_union_events as un on tr.searchid = un.searchid
         |WHERE tr.day="$date"
         |  and un.day="$date"
       """.stripMargin


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
          var searchId = x.getString(0)
          var device = x.getString(3)
          var hour = x.getString(4)
          var adslotType = x.getInt(5)
          var antispam = x.getInt(6)
          ((searchId,hour,adslotType),(load, active, buttonClick, press, stay1, stay5, stay10, stay30, stay60, stay120))
      }
      .reduceByKey {
        (a, b) =>
          (a._1 + b._1,a._2 + b._2, a._3 + b._3, a._4 + b._4, a._5 + b._5, a._6 + b._6, a._7 + b._7, a._8 + b._8, a._9 + b._9, a._10 + b._10)
      }
      .cache()



    val xx = union.leftOuterJoin(traceRdd)
      xx.map{
      case ((searchId,hour,adslotType),(num, y:Option[(Int,Int,Int,Int,Int,Int,Int,Int,Int,Int)])) =>
        val trace =  y.getOrElse((0,0,0,0,0,0,0,0,0,0))
        (( hour,adslotType), (num, trace._1, trace._2, trace._3,trace._4, trace._5, trace._6, trace._7, trace._8, trace._9, trace._10))
    }.reduceByKey((x, y) => (x._1+y._1,x._2+y._2,x._3+y._3,x._4+y._4,
    x._5+y._5,x._6+y._6,x._7+y._7,x._8+y._8,x._9+y._9,x._10+y._10,x._11+y._11))
    .map{
      case   (( hour,adslotType), (num,load, active, buttonClick, press, stay1, stay5, stay10, stay30, stay60, stay120))
      =>
        var result = "" +  hour+","+adslotType+"," +num+"," +load+","+active+","+buttonClick+","+press+","+stay1+","+stay5+","+stay10+","+stay30+","+ stay60+","+stay120
        if(load>0 ){
          result +=  ","+active.toFloat/load.toFloat*100
        }else{
          result +=  ",0"
        }

        result
    }.collect().foreach(x =>println(x))
println("-------------------")
    traceRdd.map{
      case  ((searchId,hour,adslotType),(load, active, buttonClick, press, stay1, stay5, stay10, stay30, stay60, stay120)) =>
        ((hour,adslotType), (load, active, buttonClick, press, stay1, stay5, stay10, stay30, stay60, stay120))
    }.reduceByKey((x, y) => (x._1+y._1,x._2+y._2,x._3+y._3,x._4+y._4,
      x._5+y._5,x._6+y._6,x._7+y._7,x._8+y._8,x._9+y._9,x._10+y._10))
      .map{
        case   ((hour,adslotType), (load, active, buttonClick, press, stay1, stay5, stay10, stay30, stay60, stay120))
        =>
          var result = "" +  hour+","+adslotType+"," +load+","+active+","+buttonClick+","+press+","+stay1+","+stay5+","+stay10+","+stay30+","+ stay60+","+stay120
          result +=  ","+active.toFloat/load.toFloat*100
          result
      }.collect().foreach(x =>println(x))
  }
}
package com.cpc.spark.ml.antimodel.v2

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * Created by roydong on 06/07/2017.
  */
object ModelUserAnal {

  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      System.err.println(
        s"""
           |Usage: day <date>
        """.stripMargin)
      System.exit(1)
    }
    val date = args(0)
    val adslotType = args(1).toInt
    val isFilterAntispam = args(2).toInt
    println("adslotType:" + adslotType)
    Logger.getRootLogger.setLevel(Level.WARN)
    val ctx = SparkSession.builder()
      .appName("model user anal" + date)
      .enableHiveSupport()
      .getOrCreate()
    import ctx.implicits._

    var modelUser = ctx.sparkContext.textFile("/user/cpc/antispam/v2/device/" + date).map{
      case x =>
        val lines = x.split(" ")
        if(lines.length == 3){
          //UserInfo(lines(0), lines(1).toInt, lines(2).toInt, lines(3).toInt, lines(4).toInt)
          var label = lines(2).toDouble
          if(label != 0 && label != 1){
            label = 0
          }
          (lines(0), (lines(1).toDouble, label))
        }else{
          null
        }
    }.filter(x => x != null).cache()


    if(isFilterAntispam >= 1){
       modelUser = modelUser.filter(x => x._2._2 ==0).cache()
    }
    println("model User:" + modelUser.count())
    var sql1 = ""
    if(adslotType ==1 || adslotType == 2){
      sql1 = " select uid, sum(isshow), sum(isclick) FROM dl_cpc.cpc_union_log where `date` ='%s' and adslot_type =%s group by uid".format(date,adslotType)
    }else{
      sql1 = " select uid, sum(isshow), sum(isclick) FROM dl_cpc.cpc_union_log where `date` ='%s' group by uid".format(date)

    }
    println("sql1:"+ sql1)

   var union =  ctx.sql(sql1).rdd.map{
      x =>
        val device = x(0).toString()
        val show = x(1).toString().toInt
        val click = x(2).toString().toInt
        (device, (show, click))
    }

    var sql2 =  ""
    if(adslotType ==1 || adslotType == 2){
      sql2 = """
               |SELECT DISTINCT tr.searchid,tr.trace_type,tr.duration, un.uid
               |from dl_cpc.cpc_union_trace_log as tr left join dl_cpc.cpc_union_log as un on tr.searchid = un.searchid
               |WHERE  tr.`date` = '%s'  and un.`date` = '%s' and un.adslot_type = %s
             """.stripMargin.format(date,date, adslotType)
    }else{
      sql2 = """
               |SELECT DISTINCT tr.searchid,tr.trace_type,tr.duration, un.uid
               |from dl_cpc.cpc_union_trace_log as tr left join dl_cpc.cpc_union_log as un on tr.searchid = un.searchid
               |WHERE  tr.`date` = '%s'  and un.`date` = '%s'
             """.stripMargin.format(date,date)

    }
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
          (x.getString(3) ,(load, active, buttonClick, press, stay1, stay5, stay10, stay30, stay60, stay120))
      }
      .reduceByKey {
        (a, b) =>
          (a._1 + b._1,a._2 + b._2, a._3 + b._3, a._4 + b._4, a._5 + b._5, a._6 + b._6, a._7 + b._7, a._8 + b._8, a._9 + b._9, a._10 + b._10)
      }
      .cache()

    modelUser.join(union).leftOuterJoin(traceRdd).map{
      case (uid,(((predict,label),(show,click)),y:Option[(Int, Int, Int, Int, Int, Int, Int, Int, Int, Int)]))
       =>
        val trace =  y.getOrElse((0,0,0,0,0,0,0,0,0,0))
        var predict2 = predict * 100
        (predict2.toInt, (1,label, show, click, trace._1,trace._2,trace._3,trace._4,trace._5,trace._6,trace._7,trace._8,trace._9,trace._10))
    }.reduceByKey((x, y) => (x._1+y._1,x._2+y._2,x._3+y._3,x._4+y._4,
      x._5+y._5,x._6+y._6,x._7+y._7,x._8+y._8,x._9+y._9,x._10+y._10,x._11+y._11,x._12+y._12,x._13+y._13,x._14+y._14))
    .map{
     case (predict, (usercount,labelcount, show, click, load, active, buttonClick, press, stay1, stay5, stay10, stay30, stay60, stay120)) =>
      var result =  "" + predict.toDouble/100 + ","+usercount+","+labelcount+","+show+","+click+","+load+","+active+","+buttonClick+","+press+","+stay1+","+stay5+","+stay10+","+stay30+","+ stay60+","+stay120
        result += ","+click.toFloat/show.toFloat*100 +","+active.toFloat/load.toFloat*100
       result
    }.collect().foreach(x =>println(x))

    union.leftOuterJoin(traceRdd).map{
      case (uid, ((show,click), y:Option[(Int, Int, Int, Int, Int, Int, Int, Int, Int, Int)])) =>
        val trace =  y.getOrElse((0,0,0,0,0,0,0,0,0,0))
        (1, (1,0,show, click, trace._1,trace._2,trace._3,trace._4,trace._5,trace._6,trace._7,trace._8,trace._9,trace._10))
    }.reduceByKey((x, y) => (x._1+y._1,x._2+y._2,x._3+y._3,x._4+y._4,
      x._5+y._5,x._6+y._6,x._7+y._7,x._8+y._8,x._9+y._9,x._10+y._10,x._11+y._11,x._12+y._12,x._13+y._13,x._14+y._14))
    .map{
      case (x, (usercount,labelcount, show, click, load, active, buttonClick, press, stay1, stay5, stay10, stay30, stay60, stay120)) =>
        var result = "" +  x+","+usercount+","+labelcount+","+show+","+click+","+load+","+active+","+buttonClick+","+press+","+stay1+","+stay5+","+stay10+","+stay30+","+ stay60+","+stay120
        result += ","+click.toFloat/show.toFloat*100 +","+active.toFloat/load.toFloat*100
        result
    }.collect().foreach(x =>println(x))
  }
}



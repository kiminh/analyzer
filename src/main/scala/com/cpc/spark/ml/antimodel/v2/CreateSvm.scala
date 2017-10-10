package com.cpc.spark.ml.antimodel.v2

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

/*
样本
 */
object CreateSvm {

  def main(args: Array[String]): Unit = {
    if (args.length < 6) {
      System.err.println(
        s"""
           |Usage: create svm <version:string> <daybefore:int>
           | <rate:int> <ttRate:float> <saveFull:int>
           | <hour:string> <updatedict>
           |
        """.stripMargin)
      System.exit(1)
    }
    // v1 1 1 10/100 0.5 1 true ""
    Logger.getRootLogger.setLevel(Level.WARN)
    val version = args(0)
    val dayBefore = args(1).toInt
    val rate = args(2).split("/").map(_.toInt)
    val ttRate = args(3).toFloat  //train/test rate
    val saveFull = args(4).toInt
    val days = args(5).toInt
    val ctx = SparkSession.builder()
      .appName("create antispam svm data code:v2 data:" + version)
      .enableHiveSupport()
      .getOrCreate()
    import ctx.implicits._

    val dayStr = getDates(dayBefore,days)

    val dayStr1 = getDates(3,2)
    val dayStr2 = getDates(1,1)

    println("dayStr:" + dayStr)
    println("dayStr1:" + dayStr1)
    println("dayStr2:" + dayStr2)

    val cal = Calendar.getInstance()
    cal.add(Calendar.DATE, -dayBefore)
    val date = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime)
    cal.add(Calendar.DATE, -3)
    val date2 = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime)

    println("get date1 " + date)
    println("get date2 " + date2)

    val sql1 = "SELECT device_code,member_id from gobblin.qukan_p_member_info where day  in (%s) group by device_code,member_id ".format(dayStr)
    val sql2 = "SELECT member_id,type, sum(coin) from gobblin.qukan_p_gift_v2 where  day in (%s) group by member_id,type".format(dayStr)
    val sql3 = "SELECT member_id,cmd,count(content_id) from rpt_qukan.qukan_log_cmd where thedate = '%s' group by member_id,cmd".format(date)
    val sql4 = "SELECT member_id,cmd,count(content_id) from rpt_qukan.qukan_log_cmd where thedate  = '%s' group by member_id,cmd".format(date2)
    println("sql1:" +sql1)
    println("sql2:" +sql2)
    println("sql3:" +sql3)
    println("sql4:" +sql4)
    val rdd1 = ctx.sql(sql1).rdd.map{
      x =>
        val device = x(0).toString()
        val member = x(1).toString()
        (member, device)
    }.filter(x => x._2 !=  null && x._2.length >0 && x._1 !=  null && x._1.length > 0)
    val rdd2 = ctx.sql(sql2).rdd.map{
      x =>
        val member = x(0).toString()
        var coinType = 0
        var coin = 0
        try{
          coin = x(2).toString().toInt
          coinType = x(1).toString().toInt
        } catch {
          case t: Throwable =>
        }
        (member,(coinType , coin))
    }.filter(x => x._2 !=  null && x._1 !=  null && x._1.length >0)
    val rdd3 = ctx.sql(sql3).rdd.map{
      x =>
        var member = ""
        var cmdType = 0
        var contentNum = 0
        try{
          member = x(0).toString()
          cmdType = x(1).toString().toInt
          contentNum = x(2).toString().toInt
        } catch {
          case t: Throwable =>
        }
        (member, (cmdType, contentNum))
    }.filter(x => x._2 !=  null && x._1 !=  null && x._1.length >0)
    val rdd4 = ctx.sql(sql4).rdd.map{
      x =>
        var member = ""
        var cmdType = 0
        var contentNum = 0
        try{
          member = x(0).toString()
          cmdType = x(1).toString().toInt
          contentNum = x(2).toString().toInt
        } catch {
          case t: Throwable =>
        }
        (member, (cmdType, contentNum))
    }.filter(x => x._2 !=  null && x._1 !=  null && x._1.length >0)
    val rdd5 = rdd3.join(rdd4).map{
      case (member, ((cmdType, contentNum),(cmdType2, contentNum2))) =>
        (member, (cmdType - cmdType2, contentNum - contentNum2))
    }.filter(x => x._2._1 >= 0 &&  x._2._2 >= 0 )

    val qukanData = rdd2.join(rdd5).join(rdd1).map{
      case (member,(((coinType,coin),(cmdType, contentNum)),device)) =>
        (device, (coin,contentNum))
    }.reduceByKey((x,y) => (x._1 + y._1, x._2 + y._2))


    val unionSql = "SELECT uid,count(*),sum(isshow),sum(isclick),count(DISTINCT ip) ,sum(ext['antispam'].int_value) from " +
      "dl_cpc.cpc_union_log where media_appsid in ('80000001', '80000002') and `date` in(%s) GROUP BY uid".format(dayStr)
    val traceSql = s""" select un.uid,tr.trace_type,tr.duration
                   from dl_cpc.cpc_union_trace_log as tr
                   left join dl_cpc.cpc_union_log as un on
                   tr.searchid = un.searchid where
                   tr.`date` in(%s)   and un.`date`  in(%s) """.stripMargin.format(dayStr, dayStr)
    println("unionSql:" + unionSql)
    println("traceSql:" + traceSql)
    val trace = ctx.sql(traceSql).rdd.map{
      x =>
        val uid : String =  x(0).toString()
        val traceType = x(1).toString()
        val duration : Int = x(2).toString().toInt
        var load, active, buttonClick, press, stay1, stay5, stay10, stay30, stay60, stay120 = 0
        traceType match {
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
        (uid,(load, active, buttonClick, press, stay1, stay5, stay10, stay30, stay60, stay120))
    }.reduceByKey((x,y) => (x._1+y._1,x._2+y._2,x._3+y._3,x._4+y._4,x._5+y._5,x._6+y._6,x._7+y._7,x._8+y._8,x._9+y._9,x._10+y._10))

    val union = ctx.sql(unionSql).rdd.map {
      x =>
        val uid: String = x(0).toString()
        val request = x(1).toString().toInt
        val show = x(2).toString().toInt
        val click = x(3).toString().toInt
        val ip = x(4).toString().toInt
        val isAntispam1 = x(5).toString().toInt
        val ctr = (click.toDouble/ show.toDouble *1000).toInt
        var isAtispam = 0
        if(isAntispam1 > 0){
          isAtispam =1
        }
        (uid,(request,show,click,ip,ctr, isAtispam))
    }

    val traceUnion = union.leftOuterJoin(trace).map{
      case  (uid, ((request, show, click, ip, ctr, isAtispam), x:Option[(Int, Int, Int, Int, Int, Int, Int, Int, Int, Int)]))
      =>
        val trace:(Int, Int, Int, Int, Int, Int, Int, Int, Int, Int) =  x.getOrElse((0,0,0,0,0,0,0,0,0,0))
        (uid,(request,show,click,ip,ctr,isAtispam ,trace._1,trace._2,trace._3,trace._4,trace._5,trace._6,trace._7,trace._8,trace._9,trace._10))
    }.leftOuterJoin(qukanData).map{
      case  (uid,((request,show,click,ip,ctr,isAtispam ,load, active,
      buttonClick, press, stay1, stay5, stay10, stay30, stay60, stay120), qukan:Option[(Int,Int)])) =>
        var qukanData = qukan.getOrElse((0,0))
        Antispam(uid,request,show,click,ip,ctr,isAtispam ,load, active,
          buttonClick, press, stay1, stay5, stay10, stay30, stay60, stay120, qukanData._1,qukanData._2)
    }
      .cache()
    traceUnion.take(10).foreach(println)

    val ulog = traceUnion.randomSplit(Array(ttRate, 1 - ttRate), seed = new Date().getTime)

    val train = ulog(0)
      .mapPartitions {
        p =>
          p.map {
            x =>
              FeatureParser.parseLog(x)
          }
      }
    train.take(10).foreach(println)

    train.toDF()
      .write
      .mode(SaveMode.Overwrite)
      .text("/user/cpc/antispam/v2/svm/train/" + date)

    println("done", train.filter(_.startsWith("1")).count(), train.count())

    if (saveFull > 0) {
      println("save full data")
      traceUnion
        .mapPartitions {
          p =>
            p.map {
              x =>
                FeatureParser.parseLog(x)
            }
        }
        .toDF()
        .write
        .mode(SaveMode.Overwrite)
        .text("/user/cpc/antispam/v2/svm/test/" + date)
      println("done", traceUnion.count())
    }
    traceUnion.unpersist()

    ctx.stop()
  }
  //uid,request,show,click,ip,ctr,isAtispam ,load, active, buttonClick, press, stay1, stay5, stay10, stay30, stay60, stay120)
  case class Antispam(
                       uid: String = "",
                       request: Int = 0,
                       show: Int = 0,
                       click: Int = 0 ,
                       ipNum: Int = 0,
                       ctr: Int = 0,
                       isAtispam: Int = 0,
                       load: Int = 0,
                       active: Int = 0,
                       buttonClick: Int = 0,
                       press: Int = 0,
                       stay1: Int = 0,
                       stay5: Int = 0,
                       stay10: Int = 0,
                       stay30: Int = 0,
                       stay60: Int = 0,
                       stay120: Int = 0,
                       coin: Int = 0,
                       contentNum: Int = 0
                     )

  def getDates(dayBefore: Int, days: Int): String = {
    val cal = Calendar.getInstance()
    val parts = new Array[String](days)
    cal.add(Calendar.DATE, -dayBefore)
    val partitionPathFormat = new SimpleDateFormat("yyyy-MM-dd")
    for (day <- 0 until days) {
      parts(day) = partitionPathFormat.format(cal.getTime)
      cal.add(Calendar.DATE, -1)
    }
    "'" + parts.mkString("','") + "'"
  }
}


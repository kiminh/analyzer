package com.cpc.spark.ml.antimodel.v1

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import com.cpc.spark.log.parser.UnionLog
import com.cpc.spark.ml.common.FeatureDict
import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.util.Random

/*
样本
 */
object CreateSvm {

  def main(args: Array[String]): Unit = {
    if (args.length < 5) {
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
    val ctx = SparkSession.builder()
      .appName("create antispam svm data code:v1 data:" + version)
      .enableHiveSupport()
      .getOrCreate()
    import ctx.implicits._

    val cal = Calendar.getInstance()
    cal.add(Calendar.DATE, -dayBefore)
    val date = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime)
    println("get data " + date)
    val userTextRDD = ctx.sparkContext.textFile("/user/cpc/antispamsvm/userinfo/" + date)
    val userInfoRDD = userTextRDD.map{
      case x =>
        val lines = x.split(",")
        if(lines.length == 5){
          //UserInfo(lines(0), lines(1).toInt, lines(2).toInt, lines(3).toInt, lines(4).toInt)
          (lines(0), (lines(2).toInt, lines(4).toInt))
        }else{
          null
        }
    }.filter(x => x != null).reduceByKey((x,y) => (x._1+y._1, x._2+y._2)).map{
      case (device,(coin, contentNum)) =>
        (device, UserInfo("", 0, coin, 0, contentNum))
    }
    userInfoRDD.take(10).foreach(x => println("userInfo:" + x))

    val sqlunion = s"""
                   |select * from dl_cpc.cpc_union_log where `date` = "%s" and isfill = 1 and adslotid > 0
                   |and media_appsid in ("80000001", "80000002")
      """.stripMargin.format(date)
    println("sqlunion:" + sqlunion)

    val union = ctx.sql(sqlunion).as[UnionLog].rdd
    val sqltrace = s""" select searchid,max(duration) from dl_cpc.cpc_union_trace_log where `date` = "%s" group by searchid
      """.stripMargin.format(date)
    println("sqltrace:" + sqltrace)

    val trace = ctx.sql(sqltrace).rdd.map{
      x =>
        val searchid : String =  x(0).toString()
        val duration : Int = x(1).toString().toInt
        (searchid,duration)
    }
    val unionTrace= union.map(x => (x.searchid, x)).leftOuterJoin(trace).map{
      case (searchid,(union, duration)) =>
        (union.uid, (union, duration))
    }
    val joinRdd = unionTrace.join(userInfoRDD)
    val ulog = joinRdd.randomSplit(Array(ttRate, 1 - ttRate), seed = new Date().getTime)
    FeatureDict.loadData()
    val bdict = ctx.sparkContext.broadcast(FeatureDict.dict)
    val train = ulog(0).filter {
        u =>
          var ret = false
          if (u._2._1._1.isclick == 1 || Random.nextInt(rate(1)) < rate(0)) {
            ret = true
          }
          ret
      }
      .mapPartitions {
        p =>
          val dict = bdict.value
          p.map {
            x =>
              FeatureParser.parseUnionLog(x, dict)
          }
      }
    train.take(1).foreach(println)
    train.toDF()
      .write
      .mode(SaveMode.Overwrite)
      .text("/user/cpc/antispamsvm/" + version + "/" + date)
    println("done", train.filter(_.startsWith("1")).count(), train.count())
    if (saveFull > 0) {
      println("save full data")
      ulog(1)
        .mapPartitions {
          p =>
            val dict = bdict.value
            p.map {
              x =>
                FeatureParser.parseUnionLog(x, dict)
            }
        }
        .toDF()
        .write
        .mode(SaveMode.Overwrite)
        .text("/user/cpc/antispamsvm/" + version + "_full/" + date)
      println("done", ulog(1).count())
    }
    train.unpersist()
    cal.add(Calendar.DATE, 1)

    ctx.stop()
  }

  case class TLog(
                   searchid: String = "",
                   search_timestamp: Int = 0,
                   trace_type: String = "",
                   trace_os: String = "",
                   trace_refer: String = "",
                   trace_version: String = "",
                   trace_click_count: Int = 0,
                   device_orientation: Int = 0,
                   client_w: Float = 0,
                   client_h: Float = 0,
                   screen_w: Float = 0,
                   screen_h: Float = 0,
                   client_x: Float = 0,
                   client_y: Float = 0,
                   page_x: Float = 0,
                   page_y: Float = 0,
                   trace_ttl: Int = 0,
                   scroll_top: Float = 0,
                   trace_op1: String = "",
                   trace_op2: String = "",
                   trace_op3: String = "",
                   duration: Int = 0,
                   date: String = "",
                   hour: String = ""
                 ) {

  }
}


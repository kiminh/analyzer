package com.cpc.spark.ml.antimodel.v1

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

/*
样本
 */
object CreateQukanUserInfo {
  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      System.err.println(
        s"""
           |Usage: create svm <version:string> <daybefore:int>
        """.stripMargin)
      System.exit(1)
    }
    val version = args(0)
    val dayBefore = args(1).toInt
    val cal = Calendar.getInstance()
    cal.add(Calendar.DATE, -dayBefore)
    val date = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime)
    cal.add(Calendar.DATE, -1)
    val date2 = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime)

    Logger.getRootLogger.setLevel(Level.WARN)
    val ctx = SparkSession.builder()
      .appName("create antispam userinfo data code:v1 data:" )
      .enableHiveSupport()
      .getOrCreate()
    import ctx.implicits._
    val sql1 = "SELECT device_code,member_id from gobblin.qukan_p_member_info where day  ='%s' group by device_code,member_id ".format(date)
    val sql2 = "SELECT member_id,type, sum(coin) from gobblin.qukan_p_gift_v2 where  day  ='%s' group by member_id,type".format(date)
    val sql3 = "SELECT member_id,cmd,count(content_id) from rpt_qukan.qukan_log_cmd where thedate ='%s' group by member_id,cmd".format(date)
    val sql4 = "SELECT member_id,cmd,count(content_id) from rpt_qukan.qukan_log_cmd where thedate ='%s' group by member_id,cmd".format(date2)
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
        ((device, coinType, cmdType), (coin,contentNum))
    }.reduceByKey((x,y) => (x._1 + y._1, x._2 + y._2)).filter(x  => x._1._1.length > 0)
    val toResult = qukanData.map{
        case ((device, coinType, cmdType), (coin,contentNum)) =>
          device + ","+ coinType + ","+ coin+ ","+ cmdType+ ","+contentNum
      }
    toResult.toDF()
        .write
        .mode(SaveMode.Overwrite)
        .text("/user/cpc/antispamsvm/userinfo/" + date)
      println("done", qukanData.count())
    ctx.stop()
  }
}


package com.cpc.spark.qukan.userprofile

import java.text.SimpleDateFormat
import java.util.Calendar

import com.redis.RedisClient
import com.redis.serialization.Parse.Implicits._
import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import userprofile.Userprofile.UserProfile

/**
  * Created by Roy on 2017/5/17.
  */
object GetReportIncreaseCoin {
  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      System.err.println(
        s"""
           |Usage: GetUserIncreaseCoin <day_before>
           |
        """.stripMargin)
      System.exit(1)
    }
    Logger.getRootLogger.setLevel(Level.WARN)
    val dayBefore = args(0).toInt

    val conf = ConfigFactory.load()
    val ctx = SparkSession.builder()
      .appName("cpc get user profile increase coin ")
      .enableHiveSupport()
      .getOrCreate()
    val cal = Calendar.getInstance()
    cal.add(Calendar.DATE, -dayBefore)
    val day = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime)
    val dayStr = getDates(dayBefore,7)
    val sqltext =  "select member_id as member,sum(coin) as coin from  gobblin.qukan_p_gift_v2 where type = 210 " +
      "and day in( "+dayStr+ ")group by member_id "
    val sqltext2 =  "SELECT member, device from rpt_qukan.device_member_coin where thedate in(" + dayStr + ")"
    val sqltext3 =  "SELECT uid, isclick,isshow, isfill FROM  dl_cpc.cpc_union_log where `date` = '"+day+"'"

    println("sqltext:" + sqltext)
    println("sqltext2:" + sqltext2)
    println("sqltext2:" + sqltext3)
    val result1 = ctx.sql(sqltext).rdd.map{
      x =>
        val member : String = x(0).toString()
        val coin : Long = x(1).toString().toLong
        (member,coin)
    }.cache()
    val result2 = ctx.sql(sqltext2).rdd.map{
      x =>
        val member : String = x(0).toString()
        val device : String = x(1).toString()
        (device,member)
    }.reduceByKey((x,y) => x).map(x => (x._2,x._1)).reduceByKey((x,y) => x).cache()
    val result3 = ctx.sql(sqltext3).rdd.map{
      x =>
        val device : String = x(0).toString()
        val isclick : Int = x(1).toString().toInt
        val isshow : Int = x(2).toString().toInt
        val isfill : Int = x(3).toString().toInt
        (device,(isfill, isshow, isclick))
    }.cache()
    val toResult= result1.join(result2).map{
      case (member,(coin,device)) =>
        (device,coin)
    }.reduceByKey((x,y) => x).cache()
    val result4 = result3.join(toResult).map(x => x._2._1)
    val total = result4.count()
    val fill = result4.filter{
      case (isfill, isshow, isclick) => isfill == 1
    }.count()
    val click = result4.filter{
      case (isfill, isshow, isclick) => isclick == 1
    }.count()
    val show = result4.filter{
      case (isfill, isshow, isclick) => isshow == 1
    }.count()
    println("total:%d isfill: %d click: %d  show: %d".format(total, fill, click, show))
    ctx.stop()
  }

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


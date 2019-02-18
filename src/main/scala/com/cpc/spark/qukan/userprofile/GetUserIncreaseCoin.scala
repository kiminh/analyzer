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
@deprecated
object GetUserIncreaseCoin {
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
    //val sqltext2 =  "SELECT member, device from rpt_qukan.device_member_coin where thedate in(" + dayStr + ")"
    val sqltext2 =  "SELECT member_id ,device_code from gobblin.qukan_p_member_info where day =\""+day+"\""

    println("sqltext:" + sqltext)
    println("sqltext2:" + sqltext2)
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

    val toResult= result1.join(result2).map{
      case (member,(coin,device)) =>
        (device,coin)
    }.reduceByKey((x,y) => x).cache()

    result1.map {
      x =>
        val y = x._2
        if(y<0){
          ("<0",1)
        }else if (y == 0){
          ("0", 1)
        }else if (y<=10){
          ("0-10",1)
        }else if (y <= 20){
          ("11-20",1)
        }else if (y <= 30){
          ("21-30",1)
        }else if (y <= 40){
          ("31-40",1)
        }else if (y <= 50){
          ("41-50",1)
        }else if (y <= 60){
          ("51-60",1)
        }else if (y <= 70){
          ("61-70",1)
        }else if (y <= 80){
          ("71-80",1)
        }else if (y <= 90){
          ("81-90",1)
        }else if (y<= 100) {
          ("91-100",1)
        } else if (y<= 200) {
          ("101-200",1)
        } else if (y<= 300) {
          ("201-300",1)
        } else if (y<= 400) {
          ("301-400",1)
        }else if (y <= 500){
          ("401-500",1)
        } else if (y<=1000) {
          ("501-1000",1)
        } else if (y<=2000) {
          ("1001-2000",1)
        } else if (y<=5000) {
          ("2001-5000",1)
        } else if (y<=10000) {
          ("5001-10000",1)
        }else {
          (">10000",1)
        }
    }.reduceByKey {
      (x,y) =>x+y
    }.collect().foreach(println)
    toResult.take(10).foreach(x => println("result:"+x))
    val sum = toResult.filter(x => x._2 > 0).mapPartitions{
      p =>
        var n1 = 0
        var n2 = 0
        var n3 = 0
        val redis = new RedisClient(conf.getString("redis.host"), conf.getInt("redis.port"))
        p.foreach {
          x =>
            n1 = n1 + 1
            val key = x._1 + "_UPDATA"
            val buffer = redis.get[Array[Byte]](key).getOrElse(null)
            if (buffer != null) {
              val userProfile = UserProfile.parseFrom(buffer)
              if (userProfile.getShareCoin != x._2 ) {
                n2 = n2 + 1
                val user = userProfile.toBuilder.setShareCoin(x._2.toInt)
                redis.setex(key, 3600 * 24 * 7, user.build().toByteArray)
              }
            }else{
              n3 = n3 + 1
              val user = UserProfile.newBuilder().setDevid(x._1).setShareCoin(x._2.toInt)
              redis.setex(key, 3600 * 24 * 7, user.build().toByteArray)
            }
        }
        Seq((0, n1), (1, n2),(2, n3)).iterator
    }
    //统计新增数据
    var n1 = 0
    var n2 = 0
    var n3 = 0
    sum.reduceByKey((x, y) => x + y)
      .take(3)
      .foreach {
        x =>
          if (x._1 == 0) {
            n1 = x._2
          } else if(x._1 == 1){
            n2 = x._2
          }else {
            n3 = x._2
          }
      }
    val count = toResult.count()
    println("total:%d no-zero-total: %d updated: %d  add: %d".format(count, n1, n2, n3))
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


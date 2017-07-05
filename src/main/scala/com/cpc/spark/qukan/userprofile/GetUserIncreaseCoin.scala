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

    val cal = Calendar.getInstance()
    cal.add(Calendar.DATE, -dayBefore)
    val date1 = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime)
    cal.add(Calendar.DATE, -1)
    val date2 = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime)

    println("Date1:" + date1)
    println("Date2:" + date2)

    val conf = ConfigFactory.load()

    val ctx = SparkSession.builder()
      .appName("cpc get user profile increase coin ")
      .enableHiveSupport()
      .getOrCreate()
    val sqltext =  "select uid, coin from dl_cpc.cpc_union_log where `date` = \"" + date1 + "\""
    val sqltext2 =  "select uid, coin from dl_cpc.cpc_union_log where `date` = \"" + date2 + "\""

    println("sqltext:" + sqltext)
    println("sqltext2:" + sqltext2)

    val result = ctx.sql(sqltext).rdd.map{
      x =>
        val uid : String = x(0).toString()
        val coin : Int = x(1).toString().toInt
        (uid,(coin,0))
    }
    val result2 = ctx.sql(sqltext2).rdd.map {
      x =>
        val uid : String = x(0).toString()
        val coin : Int = x(1).toString().toInt
        (uid, (0, coin))
    }
    val result3 = result.union(result2).reduceByKey{
      (x,y) =>
        var xx = x._1
        var yy = x._2
        if (x._1 == 0 ) {
          xx =  y._1
        }
        if (x._2 == 0 ) {
          yy = y._2
        }
        (xx,yy)
    }.filter(x => x._2._1 != 0 && x._2._2 != 0 ).map {
      x =>
        val incr = x._2._2 - x._2._1
        (x._1, incr)
    }.filter(x=> x._2 >0).cache()

    result3.take(100).foreach(println)

    val sum = result3.mapPartitions{
      p =>
        var n1 = 0
        var n2 = 0
        val redis = new RedisClient(conf.getString("redis.host"), conf.getInt("redis.port"))
        p.foreach {
          x =>
            n1 = n1 + 1
            val key = x._1 + "_UPDATA"
            val buffer = redis.get[Array[Byte]](key).getOrElse(null)
            if (buffer != null) {
              val userProfile = UserProfile.parseFrom(buffer)
              if (userProfile.getIncrCoin != x._2 ) {
                n2 = n2 + 1
                val user = userProfile.toBuilder.setIncrCoin(x._2)
                redis.setex(key, 3600 * 24 * 7, user.build().toByteArray)
              }
            }
        }
        Seq((0, n1), (1, n2)).iterator
    }
    //统计新增数据
    var n1 = 0
    var n2 = 0
    sum.reduceByKey((x, y) => x + y)
      .take(3)
      .foreach {
        x =>
          if (x._1 == 0) {
            n1 = x._2
          } else {
            n2 = x._2
          }
      }
    println("total: %d updated: %d".format(n1, n2))
    ctx.stop()
  }
}


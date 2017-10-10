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
  * Created by
  */
object GetUserAntispamV3 {

  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      System.err.println(
        s"""
           |Usage: GetUserAntispam <dayBefore> <rate>
        """.stripMargin)
      System.exit(1)
    }
    Logger.getRootLogger.setLevel(Level.WARN)
    val conf = ConfigFactory.load()
    val dayBefore = args(0).toInt
    val rate = args(1).toFloat
    val cal = Calendar.getInstance()
    cal.add(Calendar.DATE, -dayBefore)
    val day1 = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime)
    println(day1, rate)
    val ctx = SparkSession.builder()
      .appName("cpc get user antispam v3  day [%s] rate [%s] ".format(day1, rate))
      .enableHiveSupport()
      .getOrCreate()
    println("/user/cpc/antispam/v2/device/" + day1)
    var modelUser = ctx.sparkContext.textFile("/user/cpc/antispam/v2/device/" + day1).map{
      case x =>
        val lines = x.split(" ")
        if(lines.length == 3){
          var label = lines(2).toDouble
          if(label != 0 && label != 1){
            label = 0
          }
          (lines(0), lines(1).toDouble, label)
        }else{
          null
        }
    }.filter(x => x != null && x._2 > rate).cache()
    val sum =  modelUser.mapPartitions {
      p =>
        var n1 = 0
        var n2 = 0
        var n3 = 0
        var n4 = 0
        val redis = new RedisClient(conf.getString("redis.host"), conf.getInt("redis.port"))
        p.foreach {
          case (uid, predict, label) =>
            var flag = true
            n1 = n1 + 1
            var user : UserProfile.Builder = null
            val key = uid + "_UPDATA"
            val buffer = redis.get[Array[Byte]](key).getOrElse(null)
            if (buffer == null) {
              user = UserProfile.newBuilder().setDevid(uid)
              n2 = n2 + 1
            }else {
              user = UserProfile.parseFrom(buffer).toBuilder
              if(user.getAntispam == 1){
                flag = false
                n3 = n3 + 1
              }else if(user.getAntispam == 2){
                n4 = n4 + 1
              }else{
                n2 = n2 + 1
              }
            }
            if(flag){
              user.setAntispam(2)
              redis.setex(key, 3600 * 24 * 7, user.build().toByteArray)
            }
        }
        Seq((0, n1), (1, n2),(2, n3), (3, n4)).iterator
    }

    //统计新增数据
    var n1 = 0
    var n2 = 0
    var n3 = 0
    var n4 = 0
    sum.reduceByKey((x, y) => x + y)
    .collect()
    .foreach {
      x =>
        if (x._1 == 0) {
          n1 = x._2
        } else if(x._1 == 1) {
          n2 = x._2
        }else if(x._1 == 2) {
          n3 = x._2
        }else if(x._1 == 3) {
          n4 = x._2
        }
    }
    println("total: %d updated: %d antispam1: %d hasmodelantispam: %d ".format(n1, n2, n3, n4))
    ctx.stop()
  }
}



package com.cpc.spark.qukan.userprofile


import com.redis.RedisClient
import com.redis.serialization.Parse.Implicits._
import com.typesafe.config.ConfigFactory
import userprofile.Userprofile.UserProfile
/**
  * Created by Roy on 2017/5/17.
  */
object TestGetUserProfile {
  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      System.err.println(
        s"""
           |Usage: deviceId <device>
           |
        """.stripMargin)
      System.exit(1)
    }
    val device = args(0)
    val conf = ConfigFactory.load()
    val redis = new RedisClient(conf.getString("redis.host"), conf.getInt("redis.port"))
    val key = device+ "_UPDATA"
    val buffer = redis.get[Array[Byte]](key).getOrElse(null)
    if (buffer != null) {
      val u = UserProfile.parseFrom(buffer)
      println("user: " + u)
    }else{
      println("no data in redis")
    }
  }
}

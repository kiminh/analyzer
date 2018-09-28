package com.cpc.spark.ocpc

import java.text.SimpleDateFormat
import java.util.Calendar
import java.util.zip

import com.redis.RedisClient
import com.redis.serialization.Parse.Implicits._
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._
import userprofile.Userprofile.UserProfile
import org.apache.spark.sql.functions.rand

import scala.collection.mutable.ListBuffer
//import UseridDataOcpc._
//import userprofile.Userprofile.SingleUser2
//import userprofile.Userprofile.{SingleUser, UserOcpc}
import userocpc.userocpc._
//import userocpc.User
import java.io.FileOutputStream

import org.apache.spark.TaskContext


object OcpcRedisTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()

    // calculate time period for historical data
    val dataset = spark.table("test.test_redis_table_20180928").orderBy(rand(11)).limit(20)
    for (row <- dataset.collect()) {
      val key = row.get(0).toString
      val data = row.get(3).toString
      val kValue = key + "_UPDATA"
      println(s"$key, $data")
      testPbRedis(kValue)
    }

  }


  def testPbRedis(key: String): Unit = {
    println("testPbRedis function: " + key)
    val conf = ConfigFactory.load()
    val redis = new RedisClient(conf.getString("redis.host"), conf.getInt("redis.port"))
    val buffer = redis.get[Array[Byte]](key).orNull
    var user: UserProfile.Builder = null
    if (buffer != null) {
      user = UserProfile.parseFrom(buffer).toBuilder
      val u = user.build()
      println(u.getAge)
      println(u.getCtrcnt)
      println(u.getCvrcnt)
      //      redis.setex(key, 3600 * 24 * 7, user.build().toByteArray)
    }
    redis.disconnect
  }
}



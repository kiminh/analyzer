package com.cpc.spark.ocpc

import com.redis.RedisClient
import com.redis.serialization.Parse.Implicits._
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._
import userprofile.Userprofile.UserProfile
import org.apache.spark.sql.functions.rand



object OcpcRedisTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()

    // calculate time period for historical data
    val randSeed = args(0).toInt
    val totalData = spark.table("test.uid_userporfile_ctr_cvr")

    // test 20 records from the temperary table randomly
    val dataset = totalData.orderBy(rand(randSeed)).limit(20)
    for (row <- dataset.collect()) {
      val key = row.get(0).toString
      val ctrCnt = row.getLong(1)
      val cvrCnt = row.getLong(2)
      val kValue = key + "_UPDATA"
      println(s"$key, $ctrCnt, $cvrCnt")
      testPbRedis(kValue)
    }

    // test the complete temperary table
    testPbRedisTotal(totalData, spark)

  }


  def testPbRedis(key: String): Unit = {
    println("testPbRedis function: " + key)
    val conf = ConfigFactory.load()
    val redis = new RedisClient(conf.getString("redis.host"), conf.getInt("redis.port"))
    val buffer = redis.get[Array[Byte]](key).orNull
    var user: UserProfile.Builder = null
    if (buffer != null) {
      user = UserProfile.parseFrom(buffer).toBuilder
      println(user.getAge)
      println(user.getCtrcnt)
      println(user.getCvrcnt)
    }
    redis.disconnect
  }



  def testPbRedisTotal(dataset: Dataset[Row], spark: SparkSession): Unit = {
    var cnt = spark.sparkContext.longAccumulator
    var cvrResultAcc = spark.sparkContext.longAccumulator
    var ctrResultAcc = spark.sparkContext.longAccumulator
    println("###############1")
    println(s"accumulator before partition loop")
    println("redis hit number: " + cnt.value.toString)
    println("correct ctr number: " + ctrResultAcc.value.toString)
    println("correct cvr number: " + cvrResultAcc.value.toString)
    val conf = ConfigFactory.load()
    println(conf.getString("redis.host"))
    println(conf.getInt("redis.port"))
    dataset.foreachPartition(iterator => {

      val redis = new RedisClient(conf.getString("redis.host"), conf.getInt("redis.port"))

      iterator.foreach{
        record => {
          val uid = record.get(0).toString
          var key = uid + "_UPDATA"
          val ctrCnt = record.getLong(1)
          val cvrCnt = record.getLong(2)

          val buffer = redis.get[Array[Byte]](key).orNull
          var user: UserProfile.Builder = null
          if (buffer != null) {
            cnt.add(1)
            user = UserProfile.parseFrom(buffer).toBuilder
            val currentCtr = user.getCtrcnt
            val currentCvr = user.getCvrcnt
            if (currentCtr == ctrCnt) {
              ctrResultAcc.add(1)
            }
            if (currentCvr == cvrCnt) {
              cvrResultAcc.add(1)
            }
          }
        }
      }
      redis.disconnect
    })


    println("####################2")
    println(s"accumulator after partition loop")
    println("redis hit number: " + cnt.value.toString)
    println("correct ctr number: " + ctrResultAcc.value.toString)
    println("correct cvr number: " + cvrResultAcc.value.toString)
  }
}



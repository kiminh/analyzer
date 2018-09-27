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

import scala.collection.mutable.ListBuffer
//import UseridDataOcpc._
//import userprofile.Userprofile.SingleUser2
//import userprofile.Userprofile.{SingleUser, UserOcpc}
import userocpc.userocpc._
//import userocpc.User
import java.io.FileOutputStream

import org.apache.spark.TaskContext


object OcpcSampleToRedis {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()

    // calculate time period for historical data
    val end_date = args(0)
    val hour = args(1)
    val sdf = new SimpleDateFormat("yyyy-MM-dd")
    val date = sdf.parse(end_date)
    val calendar = Calendar.getInstance
    calendar.setTime(date)
    calendar.add(Calendar.DATE, -1)
    val dt1 = calendar.getTime
    val date1 = sdf.format(dt1)
    calendar.add(Calendar.DATE, -1)
    val dt2 = calendar.getTime
    val date2 = sdf.format(dt2)
    val selectCondition1 = s"`date`='$date2' and hour > '$hour'"
    val selectCondition2 = s"`date`='$date1'"
    val selectCondition3 = s"`date`='$end_date' and hour <= '$hour'"

    // read data and set redis configuration
    val sqlRequest =
      s"""
         |SELECT
         |  userid,
         |  uid,
         |  SUM(cost) as cost,
         |  SUM(ctr_cnt) as ctr_cnt,
         |  SUM(cvr_cnt) as cvr_cnt,
         |  SUM(total_cnt) as total_cnt
         |FROM
         |  test.temperate_roi_track
         |WHERE ($selectCondition1) OR
         |($selectCondition2) OR
         |($selectCondition3)
         |GROUP BY userid, uid
       """.stripMargin
    println(sqlRequest)

    val base = spark.sql(sqlRequest)

    // calculation for ratio: adslotid, uid
    val uidData = base
      .groupBy("uid")
      .agg(sum("ctr_cnt").alias("ctr_cnt"), sum("cvr_cnt").alias("cvr_cnt"))
      .limit(1000)

//    uidData.write.mode("overwrite").saveAsTable("test.uid_historical_data")
    println("save to table: test.uid_historical_data")


    // calculation for bid and ROI: userid
    val userData = base
      .groupBy("userid")
      .agg(sum("cost").alias("cost"), sum("cvr_cnt").alias("cvr_cnt"), sum("ctr_cnt").alias("ctr_cnt"))


//    userData.write.mode("overwrite").saveAsTable("test.userid_historical_data")
    println("save to table: test.userid_historical_data")
    // save into redis
    savePbRedis(uidData, spark)
//    savePbPack(userData)

  }


  def savePbRedis(dataset: Dataset[Row], spark: SparkSession): Unit = {
//    val conf = ConfigFactory.load()
//    println("test svaPbRedis function:")
//    println(conf.getString("redis.host"))
//    println(conf.getInt("redis.port"))
//    val redis = new RedisClient(conf.getString("redis.host"), conf.getInt("redis.port"))
    println("size of dataset")
    println(dataset.count())
    var cnt = spark.sparkContext.longAccumulator
    var changeCnt = spark.sparkContext.longAccumulator
    println("###############1")
    println(cnt)
    println(changeCnt)
    dataset.repartition(50).foreachPartition(iterator => {
      val conf = ConfigFactory.load()
//      println("test svaPbRedis function:")
//      println(conf.getString("redis.host"))
//      println(conf.getInt("redis.port"))
      val redis = new RedisClient(conf.getString("redis.host"), conf.getInt("redis.port"))
      iterator.foreach(record => {
        val uid = record.get(0).toString
        var key = uid + "_UPDATA"
        cnt.add(1)
        val ctrCnt = record.getInt(1)
        val cvrCnt = record.getInt(2)
        val buffer = redis.get[Array[Byte]](key).orNull
        if (buffer != null) {
          var user = UserProfile.parseFrom(buffer).toBuilder
          user.setCtrcnt(ctrCnt)
          user.setCvrcnt(cvrCnt)
          redis.setex(key, 3600 * 24 * 7, user.build().toByteArray)
          changeCnt.add(1)
        }
      })
//      redis.disconnect
    })
    println("####################2")
    println(s"complete partition loop")
    println(cnt)
    println(changeCnt)
    val test = dataset.first()
    println(test.get(0).toString)

  }


  def savePbPack(dataset: Dataset[Row]): Unit = {
    var list = new ListBuffer[SingleUser]
    val filename = s"/home/cpc/wangjun/ocpc_userid/UseridDataOcpc.pb"
    println("size of the dataframe")
    println(dataset.count)
    for (record <- dataset.collect()) {
      // todo: use toInt to replace toString
      val kValue = record.get(0).toString
      val costValue = record.get(1).toString
      val ctrCntValue = record.get(2).toString
      val cvrCntValue = record.get(3).toString
      val currentItem = SingleUser(
        userid = kValue,
        cost = costValue,
        ctrcnt = ctrCntValue,
        cvrcnt = cvrCntValue
      )
      list += currentItem
    }
    val result = list.toArray[SingleUser]
    val useridData = UserOcpc(
      user = result
    )
    println("length of the array")
    println(result.length)
    useridData.writeTo(new FileOutputStream(filename))
    println("complete save data into protobuffer")

  }
}

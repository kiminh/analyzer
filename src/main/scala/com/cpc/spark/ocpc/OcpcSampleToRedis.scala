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
      .withColumn("data", concat_ws(",", col("ctr_cnt"), col("cvr_cnt")))
//      .filter("ctr_cnt>0")
//      .limit(20)

//    uidData.write.mode("overwrite").saveAsTable("test.uid_historical_data")
    println("save to table: test.uid_historical_data")


    // calculation for bid and ROI: userid
    val userData = base
      .groupBy("userid")
      .agg(sum("cost").alias("cost"), sum("cvr_cnt").alias("cvr_cnt"), sum("ctr_cnt").alias("ctr_cnt"))


//    userData.write.mode("overwrite").saveAsTable("test.userid_historical_data")
    println("save to table: test.userid_historical_data")

    // save into redis
    val tmpData = uidData.filter("ctr_cnt>0").limit(20)
//    val tmpData = spark.table("test.test_redis_table_20180927")
    savePbRedis(tmpData, spark)
//    savePbPack(userData)
//    val keyList = keys.split(",")
//    for (key <- keyList)
//      testPbRedis(key)
  }


  def savePbRedis(dataset: Dataset[Row], spark: SparkSession): Unit = {
    var cnt = spark.sparkContext.longAccumulator
    var changeCnt = spark.sparkContext.longAccumulator
    var cvrResultAcc = spark.sparkContext.longAccumulator
    var ctrResultAcc = spark.sparkContext.longAccumulator
    println("###############1")
    println(cnt)
    println(changeCnt)
    println(ctrResultAcc)
    println(cvrResultAcc)
//    var resultList = new ListBuffer[String]

//    var loopCnt = 1
//    for (row <- dataset.collect()) {
//      val uid = row.get(0).toString
//      val ctrCnt = row.getLong(1)
//      val cvrCnt = row.getLong(2)
//      loopCnt = loopCnt + 1
//      var key = uid + "_UPDATA"
//      resultList += key
//      println(s"########### loop: $loopCnt")
//      println(s"$key, $ctrCnt, $cvrCnt")
//
//    }
//    val returnValue = resultList.mkString(",")
//    println(returnValue)

    dataset.write.mode("overwrite").saveAsTable("test.test_redis_table_20180927")

    val conf = ConfigFactory.load()

    val data = dataset.select("uid", "data")

    dataset.repartition(1).foreachPartition(iterator => {

        val redis = new RedisClient(conf.getString("redis.host"), conf.getInt("redis.port"))

        iterator.foreach{
          record => {
            val uid = record.get(0).toString
            var key = uid + "_UPDATA"
            cnt.add(1)
//            val cnts = data.split(",")
            val rowData = record.get(3).toString
            val ctrCnt = rowData.split(",")(0).toLong
            val cvrCnt = rowData.split(",")(1).toLong
//            val ctrCnt = record.getString(1).toLong
//            val cvrCnt = record.getString(2).toLong

            val buffer = redis.get[Array[Byte]](key).orNull
            var user: UserProfile.Builder = null
            if (buffer != null) {
              user = UserProfile.parseFrom(buffer).toBuilder
              val u = user.build()
              //if (u.getCtrcnt != ctrCnt)
              user = user.setCtrcnt(1)
              user = user.setCtrcnt(ctrCnt)
              //if (u.getCvrcnt != cvrCnt)
              user = user.setCvrcnt(1)
              user = user.setCvrcnt(cvrCnt)
              ctrResultAcc.add(ctrCnt)
              cvrResultAcc.add(cvrCnt)
              redis.setex(key, 3600 * 24 * 7, user.build().toByteArray)
              changeCnt.add(1)
            }
//            val bufferNew = redis.get[Array[Byte]](key).orNull
//            var userNew: UserProfile.Builder = null
//            userNew = UserProfile.parseFrom(bufferNew).toBuilder
//            val uNew = userNew.build()
//            ctrResultAcc.add(uNew.getCtrcnt)
//            cvrResultAcc.add(uNew.getCvrcnt)
          }
        }
        redis.disconnect
      })
    println("####################2")
    println(s"complete partition loop")
    println(cnt)
    println(changeCnt)
    println(ctrResultAcc)
    println(cvrResultAcc)
  }

  def testPbRedis(key: String): Unit ={
    println("testPbRedis function: " + key)
    val conf = ConfigFactory.load()
    val redis = new RedisClient(conf.getString("redis.host"), conf.getInt("redis.port"))
    val buffer = redis.get[Array[Byte]](key).orNull
    if (buffer != null) {
      var user = UserProfile.parseFrom(buffer)
      println(user.getAge)
      println(user.getCtrcnt)
      println(user.getCvrcnt)
//      redis.setex(key, 3600 * 24 * 7, user.build().toByteArray)
    }
    redis.disconnect
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

package com.cpc.spark.ocpc

import java.text.SimpleDateFormat
import java.util.Calendar
import java.util.zip

import com.redis.RedisClient
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._
//import UseridDataOcpc._
//import userprofile.Userprofile.SingleUser2
//import userprofile.Userprofile.{SingleUser, UserOcpc}
import userocpc.userocpc._
//import userocpc.User
import java.io.FileOutputStream


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

//    uidData.write.mode("overwrite").saveAsTable("test.uid_historical_data")
    println("save to table: test.uid_historical_data")


    // calculation for bid and ROI: userid
    val userData = base
      .groupBy("userid")
      .agg(sum("cost").alias("cost"), sum("cvr_cnt").alias("cvr_cnt"), sum("ctr_cnt").alias("ctr_cnt"))
      .withColumn("data", concat_ws(",", col("cost"), col("ctr_cnt"), col("cvr_cnt")))

//    userData.write.mode("overwrite").saveAsTable("test.userid_historical_data")
    println("save to table: test.userid_historical_data")
    // save into redis
//    dataToRedis(adslotData, "uid", "data", "ocpc.uid:")
//    dataToRedis(userData, "userid", "data", "ocpc.userid:")
    savePbPack(userData.select("userid", "cost", "ctr_cnt", "cvr_cnt"))

  }


//  def dataToRedis(dataset: Dataset[Row], key: String, value: String, prefix: String): Unit = {
//    val redis = new RedisClient("r-2ze5dd7d4f0c6364.redis.rds.aliyuncs.com", 6379)
//    redis.auth("J9Q4wJTZbCk4McdiO8U5rIJW")
//    // get specific column from the dataframe
//    val data = dataset.select(key, value)
//    data.foreachPartition(iterator => {
//      iterator.foreach(record => {
//        val kValue = record.get(0).toString
//        val vValue = record.get(1)
//        val kk = s"$prefix$kValue"
//        redis.setex(kk, 24 * 60 * 60, vValue)
//      })
//    })
//    // disconnect
//    redis.disconnect
//  }
//
//  def saveToProtoBuffer(dataset: Dataset[Row]): Unit ={
//      case class useridData(userid: String, cost: String, ctrCnt: String, cvrCnt: String)
//
//      dataset.foreachPartition(iterator => {
//        iterator.foreach(record => {
//          val kValue = record.get(0).toString
//          val costValue = record.get(1).toString
//          val ctrCntValue = record.get(2).toString
//          val cvrCntValue = record.get(3).toString
//          val currentItem = useridData(kValue, costValue, ctrCntValue, cvrCntValue)
//        })
//      })
//    }


  def savePbPack(dataset: Dataset[Row]): Unit = {
    val useridData = UserOcpc()
    val filename = s"/home/cpc/wangjun/test_userid/UseridDataOcpc.pb"
//    val test = dataset.first()
//    val result = SingleUser(
//      userid = test.get(0).toString,
//      cost = test.get(1).toString,
//      ctrcnt = test.get(2).toString,
//      cvrcnt = test.get(3).toString
//    )
//    useridData.addUser(result)
    dataset.foreachPartition(iterator => {
      iterator.foreach(record => {
        val kValue = record.get(0).toString
        val costValue = record.get(1).toString
        val ctrCntValue = record.get(2).toString
        val cvrCntValue = record.get(3).toString
        val currentItem = UseridSingleData(
          userid = kValue,
          cost = costValue,
          ctrcnt = ctrCntValue,
          cvrcnt = cvrCntValue
        )
        useridData.addUser(currentItem)
      })
    })
    val result = useridData.build()
    result.writeTo(new FileOutputStream(filename))
    println("complete save data into protobuffer")

  }
}

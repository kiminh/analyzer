package com.cpc.spark.ocpc

import java.text.SimpleDateFormat
import java.util.Calendar

import com.redis.RedisClient
import com.redis.serialization.Parse.Implicits._
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._
import userprofile.Userprofile.UserProfile

import scala.collection.mutable.ListBuffer
import userocpc.userocpc._
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
         |  dl_cpc.ocpc_uid_userid_track
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


    // calculation for bid and ROI: userid
    val userData = base
      .groupBy("userid")
      .agg(sum("cost").alias("cost"), sum("cvr_cnt").alias("cvr_cnt"), sum("ctr_cnt").alias("ctr_cnt"))

    // save into redis
//    val tmpData = uidData.limit(10000)
    uidData.write.mode("overwrite").saveAsTable("test.uid_userporfile_ctr_cvr")
    savePbRedis("test.uid_userporfile_ctr_cvr", spark)
    testSavePbRedis("test.uid_userporfile_ctr_cvr", spark)
    savePbPack(userData)
  }


  def savePbRedis(tableName: String, spark: SparkSession): Unit = {
    var cnt = spark.sparkContext.longAccumulator
    var changeCnt = spark.sparkContext.longAccumulator
    var succSetCnt = spark.sparkContext.longAccumulator
    var cvrResultAcc = spark.sparkContext.longAccumulator
    var ctrResultAcc = spark.sparkContext.longAccumulator
    println("###############1")
    println(s"accumulator before partition loop: total loop cnt, redis retrieve cnt, redis save cnt, ctrcnt, cvrcnt")
    println(cnt)
    println(changeCnt)
    println(succSetCnt)
    println(ctrResultAcc)
    println(cvrResultAcc)
    val dataset = spark.table(tableName)
    val conf = ConfigFactory.load()
    println(conf.getString("redis.host"))
    println(conf.getInt("redis.port"))

    dataset.foreachPartition(iterator => {

        val redis = new RedisClient(conf.getString("redis.host"), conf.getInt("redis.port"))

        iterator.foreach{
          record => {
            val uid = record.get(0).toString
            var key = uid + "_UPDATA"
            cnt.add(1)
            val ctrCnt = record.getLong(1)
            val cvrCnt = record.getLong(2)
            ctrResultAcc.add(ctrCnt)
            cvrResultAcc.add(cvrCnt)

            val buffer = redis.get[Array[Byte]](key).orNull
            var user: UserProfile.Builder = null
            if (buffer != null) {
              user = UserProfile.parseFrom(buffer).toBuilder
              val u = user.build()
              user = user.setCtrcnt(ctrCnt)
              user = user.setCvrcnt(cvrCnt)

              val isSuccess = redis.setex(key, 3600 * 24 * 30, user.build().toByteArray)
              if (isSuccess) {
                succSetCnt.add(1)
              }
              changeCnt.add(1)
            }
          }
        }
        redis.disconnect
      })

    println("####################2")
    println(s"accumulator before partition loop: total loop cnt, redis retrieve cnt, redis save cnt, ctrcnt, cvrcnt")
    println(cnt)
    println(changeCnt)
    println(succSetCnt)
    println(ctrResultAcc)
    println(cvrResultAcc)
  }

  def testSavePbRedis(tableName: String, spark: SparkSession): Unit = {
    var cnt = spark.sparkContext.longAccumulator
    var cvrResultAcc = spark.sparkContext.longAccumulator
    var ctrResultAcc = spark.sparkContext.longAccumulator
    println("###############1")
    println(s"accumulator before partition loop")
    println(cnt)
    println(ctrResultAcc)
    println(cvrResultAcc)
    val conf = ConfigFactory.load()
    //    redis-cli -h 192.168.80.19 -p 6379
    println(conf.getString("redis.host"))
    println(conf.getInt("redis.port"))

    val dataset = spark.table(tableName)
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
    println(s"accumulator before partition loop")
    println(cnt)
    println(ctrResultAcc)
    println(cvrResultAcc)
  }

  def savePbPack(dataset: Dataset[Row]): Unit = {
    var list = new ListBuffer[SingleUser]
    val filename = s"/home/cpc/wangjun/ocpc_userid/UseridDataOcpc.pb"
    println("size of the dataframe")
    println(dataset.count)
    for (record <- dataset.collect()) {
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

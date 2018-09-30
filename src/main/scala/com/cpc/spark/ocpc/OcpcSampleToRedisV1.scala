
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




object OcpcSampleToRedisV1 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()

    // calculate time period for historical data
    args.foreach(println)
    val end_date = args(0)
    val hour = args(1)
    val threshold = args(2).toInt  //default: 20
    val sdf = new SimpleDateFormat("yyyy-MM-dd")
    val date = sdf.parse(end_date)
    val calendar = Calendar.getInstance
    calendar.setTime(date)
    calendar.add(Calendar.DATE, -7)
    val dt = calendar.getTime
    val start_date = sdf.format(dt)
    val selectCondition1 = s"`date`='$start_date' and hour > '$hour'"
    val selectCondition2 = s"`date`>'$start_date' and `date`<'$end_date'"
    val selectCondition3 = s"`date`='$end_date' and hour <= '$hour'"

    // read data and set redis configuration
    val sqlRequest =
      s"""
         |SELECT
         |  userid,
         |  uid,
         |  adclass,
         |  SUM(cost) as cost,
         |  SUM(ctr_cnt) as ctr_cnt,
         |  SUM(cvr_cnt) as cvr_cnt,
         |  SUM(total_cnt) as total_cnt
         |FROM
         |  dl_cpc.ocpc_uid_userid_track_v1
         |WHERE ($selectCondition1) OR
         |($selectCondition2) OR
         |($selectCondition3)
         |GROUP BY userid, uid, adclass
       """.stripMargin
    println(sqlRequest)

    val base = spark.sql(sqlRequest)

    // calculation by uid
    val uidData = base
      .groupBy("uid")
      .agg(sum("ctr_cnt").alias("ctr_cnt"), sum("cvr_cnt").alias("cvr_cnt"))
      .withColumn("data", concat_ws(",", col("ctr_cnt"), col("cvr_cnt")))


    // calculation by userid
    val userData = base
      .groupBy(col("userid"), col("adclass"))
      .agg(sum("cost").alias("cost"), sum("ctr_cnt").alias("user_ctr_cnt"), sum("cvr_cnt").alias("user_cvr_cnt"))

    // calculate by adclass
    val adclassData = base
      .groupBy("adclass")
      .agg(sum("ctr_cnt").alias("adclass_ctr_cnt"), sum("cvr_cnt").alias("adclass_cvr_cnt"))

    // connect adclass and userid
    val useridAdclassData = userData.join(adclassData, Seq("adclass")).select("userid", "cost", "user_ctr_cnt", "user_cvr_cnt", "adclass_ctr_cnt", "adclass_cvr_cnt")

    // save into redis and pb file
    // write data into a temperary table
    uidData.write.mode("overwrite").saveAsTable("test.test_uid_userporfile_ctr_cvr")
//     save data into redis
    savePbRedis("test.test_uid_userporfile_ctr_cvr", spark)
//     check redis
    testSavePbRedis("test.test_uid_userporfile_ctr_cvr", spark)
//     save data into pb file
    savePbPack(useridAdclassData, threshold)
  }


  def savePbRedis(tableName: String, spark: SparkSession): Unit = {
    var cnt = spark.sparkContext.longAccumulator
    var changeCnt = spark.sparkContext.longAccumulator
    var succSetCnt = spark.sparkContext.longAccumulator
    var cvrResultAcc = spark.sparkContext.longAccumulator
    var ctrResultAcc = spark.sparkContext.longAccumulator
    println("###############1")
    println(s"accumulator before partition loop")
    println("total loop cnt: " + cnt.value.toString)
    println("redis retrieve cnt: " + changeCnt.value.toString)
    println("redis save cnt: " + succSetCnt.value.toString)
    println("ctrcnt: " + ctrResultAcc.value.toString)
    println("cvrcnt: " + cvrResultAcc.value.toString)
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
    println(s"accumulator after partition loop")
    println("total loop cnt: " + cnt.value.toString)
    println("redis retrieve cnt: " + changeCnt.value.toString)
    println("redis save cnt: " + succSetCnt.value.toString)
    println("ctrcnt: " + ctrResultAcc.value.toString)
    println("cvrcnt: " + cvrResultAcc.value.toString)
  }

  def testSavePbRedis(tableName: String, spark: SparkSession): Unit = {
    var cnt = spark.sparkContext.longAccumulator
    var cvrResultAcc = spark.sparkContext.longAccumulator
    var ctrResultAcc = spark.sparkContext.longAccumulator
    println("###############1")
    println(s"accumulator before partition loop")
    println("redis hit number: " + cnt.value.toString)
    println("correct ctr number: " + ctrResultAcc.value.toString)
    println("correct cvr number: " + cvrResultAcc.value.toString)
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
    println(s"accumulator after partition loop")
    println("redis hit number: " + cnt.value.toString)
    println("correct ctr number: " + ctrResultAcc.value.toString)
    println("correct cvr number: " + cvrResultAcc.value.toString)
  }

  def savePbPack(dataset: Dataset[Row], threshold: Int): Unit = {
    var list = new ListBuffer[SingleUser]
    val filename = s"UseridDataOcpc.pb"
    println("size of the dataframe")
    println(dataset.count)
    var exchangeCnt = 0
    for (record <- dataset.collect()) {
      val kValue = record.get(0).toString
      val costValue = record.get(1).toString
      val userCtr = record.getLong(2)
      val userCvr = record.getLong(3)
      val adClassCtr = record.getLong(4)
      val adClassCvr = record.getLong(5)
      var ctrCntValue: String = ""
      var cvrCntValue: String = ""
      if (userCvr < threshold) {
        exchangeCnt = exchangeCnt + 1
        ctrCntValue = adClassCtr.toString
        cvrCntValue = adClassCvr.toString
      } else {
        ctrCntValue = userCtr.toString
        cvrCntValue = userCvr.toString
      }

      val currentItem = SingleUser(
        userid = kValue,
        cost = costValue,
        ctrcnt = ctrCntValue,
        cvrcnt = cvrCntValue
      )
      list += currentItem
    }
    println("Total number of replaced users is %d".format(exchangeCnt))
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

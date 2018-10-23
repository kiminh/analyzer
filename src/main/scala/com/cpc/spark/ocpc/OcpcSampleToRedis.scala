

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
//    val threshold = args(2).toInt  //default: 20
    val threshold = 20
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
         |  ideaid,
         |  adclass,
         |  date,
         |  hour,
         |  SUM(cost) as cost,
         |  SUM(ctr_cnt) as ctr_cnt,
         |  SUM(cvr_cnt) as cvr_cnt,
         |  SUM(total_cnt) as total_cnt
         |FROM
         |  dl_cpc.ocpc_uid_userid_track_label2
         |WHERE ($selectCondition1) OR
         |($selectCondition2) OR
         |($selectCondition3)
         |GROUP BY ideaid, adclass, date, hour
       """.stripMargin
    println(sqlRequest)

    val rawBase = spark.sql(sqlRequest)

    rawBase.show(10)

    rawBase.createOrReplaceTempView("base_table")

    val sqlRequestNew1 =
      s"""
         |SELECT
         |  a.ideaid,
         |  a.adclass,
         |  a.cost,
         |  a.ctr_cnt,
         |  a.cvr_cnt,
         |  a.total_cnt,
         |  a.date,
         |  a.hour,
         |  (case when b.update_date is null then '$start_date' else b.update_date end) as update_date,
         |  (case when b.update_hour is null then '$hour' else b.update_hour end) as update_hour,
         |  (case when b.update_date is null or b.update_hour is null then 1
         |        when b.update_date < date then 1
         |        when b.update_date = date and b.update_hour <= hour then 1
         |        else 0 end) as flag
         |FROM
         |  base_table as a
         |LEFT JOIN
         |  test.ocpc_idea_update_time as b
         |ON
         |  a.ideaid=b.ideaid
       """.stripMargin

    println(sqlRequestNew1)

    val rawData = spark.sql(sqlRequestNew1)
    rawData.printSchema()


    val base = rawData.filter("flag=1").select("ideaid", "adclass", "cost", "ctr_cnt", "cvr_cnt", "total_cnt")

    // calculation by userid
    val userData = base
      .groupBy("userid", "ideaid", "adclass")
      .agg(sum("cost").alias("cost"), sum("ctr_cnt").alias("user_ctr_cnt"), sum("cvr_cnt").alias("user_cvr_cnt"))
      .select("ideaid", "userid", "adclass", "cost", "user_ctr_cnt", "user_cvr_cnt")

    userData.write.mode("overwrite").saveAsTable("test.ocpc_data_userdata")


    // calculate by adclass
    val adclassData = userData
      .groupBy("adclass")
      .agg(sum("cost").alias("adclass_cost"), sum("user_ctr_cnt").alias("adclass_ctr_cnt"), sum("user_cvr_cnt").alias("adclass_cvr_cnt"))
      .select("adclass", "adclass_cost", "adclass_ctr_cnt", "adclass_cvr_cnt")

    adclassData.write.mode("overwrite").saveAsTable("test.ocpc_data_adclassdata")


    // connect adclass and userid
    val useridAdclassData = spark.sql(
      s"""
         |SELECT
         |    a.ideaid,
         |    a.userid,
         |    a.adclass,
         |    a.cost,
         |    (case when a.user_cvr_cnt<$threshold then b.adclass_ctr_cnt else a.user_ctr_cnt end) as ctr_cnt,
         |    (case when a.user_cvr_cnt<$threshold then b.adclass_cvr_cnt else a.user_cvr_cnt end) as cvr_cnt,
         |    b.adclass_cost,
         |    b.adclass_ctr_cnt,
         |    b.adclass_cvr_cnt
         |FROM
         |    test.ocpc_data_userdata a
         |INNER JOIN
         |    test.ocpc_data_adclassdata b
         |ON
         |    a.adclass=b.adclass
       """.stripMargin)

    useridAdclassData.createOrReplaceTempView("useridTable")

    val sqlRequest2 =
      s"""
         |SELECT
         |    ideaid,
         |    userid,
         |    adclass,
         |    cost,
         |    (case when cvr_cnt=0 then ctr_cnt+1 else ctr_cnt end) as ctr_cnt,
         |    (case when cvr_cnt=0 then 1 else cvr_cnt end) as cvr_cnt,
         |    adclass_cost,
         |    (case when adclass_cvr_cnt=0 then adclass_ctr_cnt+1 else adclass_ctr_cnt end) as adclass_ctr_cnt,
         |    (case when adclass_cvr_cnt=0 then 1 else adclass_cvr_cnt end) as adclass_cvr_cnt,
         |    '$end_date' as date,
         |    '$hour' as hour
         |FROM
         |    useridTable
       """.stripMargin


    val userFinalData = spark.sql(sqlRequest2)
    userFinalData.write.mode("overwrite").insertInto("dl_cpc.ocpc_pb_result_table")

    val sqlRequest3 =
      s"""
         |SELECT
         |  a.ideaid,
         |  a.userid,
         |  a.adclass,
         |  a.cost,
         |  a.ctr_cnt,
         |  a.cvr_cnt,
         |  a.adclass_cost,
         |  a.adclass_ctr_cnt,
         |  a.adclass_cvr_cnt,
         |  (case when b.k_value is null then 1.0
         |        when b.k_value > 2.0 then 2.0
         |        when b.k_value < 0.2 then 0.2
         |        else b.k_value end) as k_value
         |FROM
         |  (SELECT
         |    *
         |   FROM
         |    dl_cpc.ocpc_pb_result_table
         |   WHERE
         |    `date`='$end_date'
         |   and
         |    `hour`='$hour') a
         |LEFT JOIN
         |   test.ocpc_k_value_table b
         |ON
         |   a.ideaid=b.ideaid
       """.stripMargin

    val userFinalData2 = spark.sql(sqlRequest3)

    userFinalData2.show(10)

    userFinalData2.write.mode("overwrite").saveAsTable("test.test_new_pb_ocpc")

    userFinalData2
      .withColumn("date", lit(end_date))
      .withColumn("hour", lit(hour))
      .write.mode("overwrite")
      .insertInto("dl_cpc.ocpc_pb_result_table_v1")


    // save data into pb file
    savePbPack(userFinalData2)
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

  def savePbPack(dataset: Dataset[Row]): Unit = {
    var list = new ListBuffer[SingleUser]
    val filename = s"UseridDataOcpc.pb"
    println("size of the dataframe")
    println(dataset.count)
    dataset.show(10)
    for (record <- dataset.collect()) {

      val ideaid = record.get(0).toString
      val userId = record.get(1).toString
      val adclassId = record.get(2).toString
      val costValue = record.get(3).toString
      val ctrValue = record.getLong(4).toString
      val cvrValue = record.getLong(5).toString
      val adclassCost = record.get(6).toString
      val adclassCtr = record.getLong(7).toString
      val adclassCvr = record.getLong(8).toString
      val k = record.get(9).toString

      val currentItem = SingleUser(
        ideaid = ideaid,
        userid = userId,
        cost = costValue,
        ctrcnt = ctrValue,
        cvrcnt = cvrValue,
        adclass = adclassId,
        adclassCost = adclassCost,
        adclassCtrcnt = adclassCtr,
        adclassCvrcnt = adclassCvr,
        kvalue = k
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

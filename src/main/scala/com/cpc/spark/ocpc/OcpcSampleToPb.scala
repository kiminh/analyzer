


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




object OcpcSampleToPb {
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
         |  uid,
         |  ideaid,
         |  adclass,
         |  SUM(cost) as cost,
         |  SUM(ctr_cnt) as ctr_cnt,
         |  SUM(cvr_cnt) as cvr_cnt,
         |  SUM(total_cnt) as total_cnt
         |FROM
         |  dl_cpc.ocpc_uid_userid_track_label2
         |WHERE ($selectCondition1) OR
         |
         |($selectCondition2) OR
         |($selectCondition3)
         |GROUP BY userid, uid, ideaid, adclass
       """.stripMargin
    println(sqlRequest)

    val base = spark.sql(sqlRequest)

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
         |  (case when b.k_value is null then 1.0 else b.k_value end) as k_value
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

    // save into redis and pb file
    // write data into a temperary table
    uidData.write.mode("overwrite").saveAsTable("test.uid_userporfile_ctr_cvr")

    // save data into pb file
    savePbPack(userFinalData2)
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

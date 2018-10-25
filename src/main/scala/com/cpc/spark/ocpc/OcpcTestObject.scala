

package com.cpc.spark.ocpc

import java.text.SimpleDateFormat
import java.util.Calendar

import com.redis.RedisClient
import com.redis.serialization.Parse.Implicits._
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.{Column, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._
import userprofile.Userprofile.UserProfile

import scala.collection.mutable.ListBuffer
import userocpc.userocpc._
import java.io.FileOutputStream
import org.apache.spark.sql.functions._



object OcpcTestObject {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()

    // calculate time period for historical data
    val end_date = args(0)
    val hour = args(1)
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
         |      ($selectCondition2) OR
         |      ($selectCondition3)
         |GROUP BY userid, ideaid, adclass, date, hour
       """.stripMargin
    println(sqlRequest)

    val rawBase = spark.sql(sqlRequest)

    rawBase.createOrReplaceTempView("base_table")

    // read outsiders
    readInnocence(spark)

    // TODO: 清楚按照时间戳截取逻辑

    // 根据从mysql抽取的数据将每个ideaid的更新时间戳之前的用户记录剔除
    val sqlRequestNew1 =
      s"""
         |SELECT
         |  a.userid,
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
         |  (case when c.flag is not null then 1
         |        when b.update_date is null or b.update_hour is null then 1
         |        when b.update_date < date then 1
         |        when b.update_date = date and b.update_hour <= hour then 1
         |        else 0 end) as flag
         |FROM
         |  base_table as a
         |LEFT JOIN
         |  test.ocpc_idea_update_time as b
         |ON
         |  a.ideaid=b.ideaid
         |LEFT JOIN
         |   test.test_ocpc_innocence_idea_list as c
         |on
         |   a.ideaid=c.ideaid
       """.stripMargin

    println(sqlRequestNew1)

    val rawData = spark.sql(sqlRequestNew1)

    println("##### records of flag = 0 ##############")
    rawData.filter("flag=0").show(10)

    val base = rawData.filter("flag=1").select("userid", "ideaid", "adclass", "cost", "ctr_cnt", "cvr_cnt", "total_cnt")


    // 按ideaid求和
    val userData = base
      .groupBy("userid", "ideaid", "adclass")
      .agg(sum("cost").alias("cost"), sum("ctr_cnt").alias("user_ctr_cnt"), sum("cvr_cnt").alias("user_cvr_cnt"))
      .select("ideaid", "userid", "adclass", "cost", "user_ctr_cnt", "user_cvr_cnt")


    userData.write.mode("overwrite").saveAsTable("test.test_ocpc_data_userdata")


    // 按adclass求和
    val adclassData = userData
      .groupBy("adclass")
      .agg(sum("cost").alias("adclass_cost"), sum("user_ctr_cnt").alias("adclass_ctr_cnt"), sum("user_cvr_cnt").alias("adclass_cvr_cnt"))
      .select("adclass", "adclass_cost", "adclass_ctr_cnt", "adclass_cvr_cnt")

    adclassData.write.mode("overwrite").saveAsTable("test.test_ocpc_data_adclassdata")


    // 关联adclass和ideaid
    val useridAdclassData = spark.sql(
      s"""
         |SELECT
         |    a.ideaid,
         |    a.userid,
         |    a.adclass,
         |    a.cost,
         |    a.user_ctr_cnt as ctr_cnt,
         |    a.user_cvr_cnt as cvr_cnt,
         |    b.adclass_cost,
         |    b.adclass_ctr_cnt,
         |    b.adclass_cvr_cnt
         |FROM
         |    test.test_ocpc_data_userdata a
         |INNER JOIN
         |    test.test_ocpc_data_adclassdata b
         |ON
         |    a.adclass=b.adclass
       """.stripMargin)

    useridAdclassData.createOrReplaceTempView("useridTable")

    // 生成中间表
    val sqlRequest2 =
      s"""
         |SELECT
         |    ideaid,
         |    userid,
         |    adclass,
         |    cost,
         |    ctr_cnt,
         |    cvr_cnt,
         |    adclass_cost,
         |    adclass_ctr_cnt,
         |    adclass_cvr_cnt,
         |    '$end_date' as date,
         |    '$hour' as hour
         |FROM
         |    useridTable
       """.stripMargin


    val userFinalData = spark.sql(sqlRequest2)
//    userFinalData.write.mode("overwrite").insertInto("dl_cpc.ocpc_pb_result_table")



    // 根据中间表加入k值
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
         |        when b.k_value > 1.0 then 1.0
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
         |and
         |   a.adclass=b.adclass
       """.stripMargin

    println(sqlRequest3)

    val userFinalData2 = spark.sql(sqlRequest3).filter("cvr_cnt>=20")

    userFinalData2.show(10)

    userFinalData2.write.mode("overwrite").saveAsTable("test.test_new_pb_ocpc_bak")

//    userFinalData2
//      .withColumn("date", lit(end_date))
//      .withColumn("hour", lit(hour))
//      .write.mode("overwrite")
//      .insertInto("dl_cpc.ocpc_pb_result_table_v1_new")


    // TODO 增加hpcvr的数据
    calculateHPCVR(end_date, hour, spark)

    val sqlRequest4 =
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
         |  a.k_value,
         |  b.hpcvr
         |FROM
         |  test.test_new_pb_ocpc_bak as a
         |INNER JOIN
         |  test.test_ocpc_hpcvr_total as b
         |ON
         |  a.ideaid=b.ideaid
         |AND
         |  a.adclass=b.adclass
       """.stripMargin

    println(sqlRequest4)

    val finalData = spark.sql(sqlRequest4)

    finalData.write.mode("overwrite").saveAsTable("test.test_new_pb_ocpc_with_pcvr_bak")

    // 保存pb文件
    savePbPack(finalData)
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
      val hpcvr = record.getDouble(10)

      val tmpCost = adclassCost.toLong
      if (tmpCost<0) {
        println("#######################################")
        println("negative cost")
        println(record)
      } else {
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
          kvalue = k,
          hpcvr = hpcvr
        )
        list += currentItem
      }
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

  def readInnocence(spark: SparkSession): Unit ={
    import spark.implicits._

    val filename = "/user/cpc/wangjun/ocpc_ideaid.txt"
    val data = spark.sparkContext.textFile(filename)

    val dataRDD = data.map(x => (x.split(",")(0).toInt, x.split(",")(1).toInt))
    //    dataRDD.foreach(println)

    dataRDD.toDF("ideaid", "flag").createOrReplaceTempView("innocence_list")

    val sqlRequest =
      s"""
         |SELECT
         |  ideaid,
         |  flag
         |FROM
         |  innocence_list
         |GROUP BY ideaid, flag
       """.stripMargin

    val resultDF = spark.sql(sqlRequest)

    resultDF.write.mode("overwrite").saveAsTable("test.test_ocpc_innocence_idea_list")
  }


  def calculateHPCVR(endDate: String, hour: String, spark: SparkSession): Unit ={
    // calculate time period for historical data
    val threshold = 20
    val sdf = new SimpleDateFormat("yyyy-MM-dd")
    val date = sdf.parse(endDate)
    val calendar = Calendar.getInstance
    calendar.setTime(date)
    calendar.add(Calendar.DATE, -7)
    val dt = calendar.getTime
    val startDate = sdf.format(dt)
    val selectCondition1 = s"`date`='$startDate' and hour > '$hour'"
    val selectCondition2 = s"`date`>'$startDate' and `date`<'$endDate'"
    val selectCondition3 = s"`date`='$endDate' and hour <= '$hour'"

    // read data and set redis configuration
    val sqlRequest =
      s"""
         |SELECT
         |  ideaid,
         |  adclass,
         |  SUM(total_cvr) * 1.0 / SUM(cnt) as hpcvr
         |FROM
         |  dl_cpc.ocpc_pcvr_history
         |WHERE ($selectCondition1) OR
         |      ($selectCondition2) OR
         |      ($selectCondition3)
         |GROUP BY ideaid, adclass
       """.stripMargin
    println(sqlRequest)

    val rawTable = spark.sql(sqlRequest)

    rawTable.write.mode("overwrite").saveAsTable("test.test_ocpc_hpcvr_total")

  }


}


package com.cpc.spark.ocpc

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.functions.{col, concat_ws, sum, when}

object OcpcHistoryData {
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
         |  adclass,
         |  SUM(cost) as cost,
         |  SUM(ctr_cnt) as ctr_cnt,
         |  SUM(cvr_cnt) as cvr_cnt,
         |  SUM(total_cnt) as total_cnt
         |FROM
         |  dl_cpc.ocpc_uid_userid_track
         |WHERE ($selectCondition1) OR
         |($selectCondition2) OR
         |($selectCondition3)
         |GROUP BY userid, uid, adclass
       """.stripMargin
    println(sqlRequest)

    val base = spark.sql(sqlRequest)

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

    //     save data into pb file
    saveDataHive(useridAdclassData, threshold)
  }

  def saveDataHive(dataset: Dataset[Row], threshold: Int): Unit = {
    val df = dataset
      .withColumn("ctr", when(col("user_cvr_cnt")<20, col("user_ctr_cnt")).otherwise(col("adclass_ctr_cnt")))
      .withColumn("cvr", when(col("user_cvr_cnt")<20, col("user_cvr_cnt")).otherwise(col("adclass_cvr_cnt")))
      .select("userid", "cost", "ctr", "cvr")

    df.write.mode("overwrite").saveAsTable("test.historical_union_log_data")

  }

}
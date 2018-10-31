

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

import com.cpc.spark.common.Utils.getTimeRangeSql
import org.apache.spark.sql.functions._



object OcpcTestObject {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()

    // calculate time period for historical data
    val date = args(0).toString
    val hour = args(1).toString

    val sqlRequest =
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
         |    adclass_cvr_cnt
         |FROM
         |    dl_cpc.ocpc_pb_result_table
         |WHERE
         |    `date`='$date'
         |AND
         |    `hour`='$hour'
       """.stripMargin

    println(sqlRequest)
    val rawData = spark.sql(sqlRequest)

    filterDataByType(rawData, date, hour, spark)
  }



  def checkAdType(endDate: String, hour: String, spark: SparkSession): DataFrame ={
    // 计算时间区间
    val threshold = 20
    val sdf = new SimpleDateFormat("yyyy-MM-dd")
    val date = sdf.parse(endDate)
    val calendar = Calendar.getInstance
    calendar.setTime(date)
    calendar.add(Calendar.DATE, -7)
    val dt = calendar.getTime
    val startDate = sdf.format(dt)
    val selectCondition = getTimeRangeSql(startDate, hour, endDate, hour)

    // 汇总近七天数据并找到每个ideaid，adclass的最新数据的类型
    val sqlRequest1 =
      s"""
         |SELECT
         |    ideaid,
         |    adclass,
         |    (case when siteid>0 then 1 else 0 end) as type_flag,
         |    row_number() over(partition by ideaid, adclass ORDER BY timestamp DESC) as seq
         |FROM
         |    dl_cpc.ocpc_track_ad_type_hourly
         |WHERE
         |    $selectCondition
       """.stripMargin

    println(sqlRequest1)
    val rawData = spark.sql(sqlRequest1)

    val typeData = rawData.filter("seq=1").select("ideaid", "adclass", "type_flag")

    // 存储数据
    typeData.write.mode("overwrite").saveAsTable("test.ocpc_ideaid_type")
    typeData
  }

  def filterDataByType(rawData: DataFrame, date:String, hour: String, spark:SparkSession): Unit ={
    val typeData = checkAdType(date, hour, spark)

    val joinData = rawData
      .join(typeData, Seq("ideaid", "adclass"), "left_outer")
      .select("ideaid", "userid", "adclass", "cost", "ctr_cnt", "cvr_cnt", "adclass_cost", "adclass_ctr_cnt", "adclass_cvr_cnt", "type_flag")
      .withColumn("new_type_flag", when(col("type_flag").isNull, 0).otherwise(col("type_flag")))

    joinData.createOrReplaceTempView("join_table")

    joinData.write.mode("overwrite").saveAsTable("test.ocpc_adclass_join_table")

    val sqlRequest1 =
      s"""
         |SELECT
         |    adclass,
         |    new_type_flag,
         |    SUM(cost) as total_cost,
         |    SUM(ctr_cnt) as total_ctr,
         |    SUM(cvr_cnt) as total_cvr
         |FROM
         |    join_table
         |GROUP BY adclass, new_type_flag
       """.stripMargin

    println(sqlRequest1)
    val groupbyData = spark.sql(sqlRequest1)
//    groupbyData.createOrReplaceTempView("groupby_table")
    groupbyData.write.mode("overwrite").saveAsTable("test.ocpc_type_groupby_data")

    val joinData2 = joinData
      .join(groupbyData, Seq("adclass", "new_type_flag"), "left_outer")
      .select("ideaid", "userid", "adclass", "cost", "ctr_cnt", "cvr_cnt", "adclass_cost", "adclass_ctr_cnt", "adclass_cvr_cnt", "new_type_flag", "total_cost", "total_ctr", "total_cvr")
      .withColumn("new_cost", when(col("cvr_cnt")<20 and col("new_type_flag")===1, col("total_cost")).otherwise(col("cost")))
      .withColumn("new_ctr_cnt", when(col("cvr_cnt")<20 and col("new_type_flag")===1, col("total_ctr")).otherwise(col("ctr_cnt")))
      .withColumn("new_cvr_cnt", when(col("cvr_cnt")<20 and col("new_type_flag")===1, col("total_cvr")).otherwise(col("cvr_cnt")))


    joinData2.write.mode("overwrite").saveAsTable("test.ocpc_final_join_table")


  }


}


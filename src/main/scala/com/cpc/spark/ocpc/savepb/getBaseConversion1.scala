package com.cpc.spark.ocpc.savepb

import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.common.Utils.getTimeRangeSql
import com.cpc.spark.ocpc.utils.OcpcUtils._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._


object getBaseConversion1 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()

    // 计算日期周期
    val date = args(0).toString
    val hour = args(1).toString

    val resultDF = baseDataV1(date, hour, spark)
    // TODO: 换成dl_cpc的表
    resultDF.write.mode("overwrite").saveAsTable("test.ocpc_conversion1_base_hourly")
  }

  def baseDataV1(end_date: String, hour: String, spark: SparkSession) :DataFrame = {
    // 计算日期周期
    val sdf = new SimpleDateFormat("yyyy-MM-dd")
    val date = sdf.parse(end_date)
    val calendar = Calendar.getInstance
    calendar.setTime(date)
    calendar.add(Calendar.DATE, -7)
    val dt = calendar.getTime
    val start_date = sdf.format(dt)
    val selectCondition = getTimeRangeSql(start_date, hour, end_date, hour)


    // 累积计算最近一周数据
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
         |WHERE $selectCondition
         |GROUP BY userid, ideaid, adclass, date, hour
       """.stripMargin
    println(sqlRequest)

    val rawBase = spark.sql(sqlRequest)

    val base = rawBase
      .select("userid", "ideaid", "adclass", "cost", "ctr_cnt", "cvr_cnt", "total_cnt")

    // 按ideaid求和
    val userData = base
      .groupBy("userid", "ideaid", "adclass")
      .agg(
        sum("cost").alias("cost"),
        sum("ctr_cnt").alias("ctr_cnt"),
        sum("cvr_cnt").alias("cvr_cnt"))
      .select("ideaid", "userid", "adclass", "cost", "ctr_cnt", "cvr_cnt")


    // 按adclass求和
    val adclassData = userData
      .groupBy("adclass")
      .agg(
        sum("cost").alias("adclass_cost"),
        sum("ctr_cnt").alias("adclass_ctr_cnt"),
        sum("cvr_cnt").alias("adclass_cvr_cnt"))
      .select("adclass", "adclass_cost", "adclass_ctr_cnt", "adclass_cvr_cnt")

    // 关联ideaid和adclass维度的累计表
    val userFinalData = userData
      .join(adclassData, Seq("adclass"))
      .select("ideaid", "userid", "adclass", "cost", "ctr_cnt", "cvr_cnt", "adclass_cost", "adclass_ctr_cnt", "adclass_cvr_cnt")
      .withColumn("date", lit(end_date))
      .withColumn("hour", lit(hour))

    userFinalData


  }


}


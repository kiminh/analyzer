package com.cpc.spark.ocpc.utils

import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.ocpc.OcpcSampleToRedis.checkAdType
import com.cpc.spark.ocpc.OcpcUtils.getTimeRangeSql2
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, when}

object OcpcUtils {
  def filterDataByType(rawData: DataFrame, date:String, hour: String, spark:SparkSession): DataFrame ={
    /**
      * 如果表单类广告数量过少，使用行业类别数据来替代
      */
    // TODO 使广告行业的ctr与cvr的替换更加平滑
    // TODO 还需不需要这个阶段？
    val typeData = spark
      .table("test.ocpc_idea_update_time")
      .withColumn("type_flag", when(col("conversion_goal")===3, 1).otherwise(0)).select("ideaid", "type_flag")

    val joinData = rawData
      .join(typeData, Seq("ideaid"), "left_outer")
      .select("ideaid", "userid", "adclass", "cost", "ctr_cnt", "cvr_cnt", "adclass_cost", "adclass_ctr_cnt", "adclass_cvr_cnt", "type_flag")
      .withColumn("new_type_flag", when(col("type_flag").isNull, 0).otherwise(col("type_flag")))

    joinData.createOrReplaceTempView("join_table")

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

    val joinData2 = joinData
      .join(groupbyData, Seq("adclass", "new_type_flag"), "left_outer")
      .select("ideaid", "userid", "adclass", "cost", "ctr_cnt", "cvr_cnt", "adclass_cost", "adclass_ctr_cnt", "adclass_cvr_cnt", "new_type_flag", "total_cost", "total_ctr", "total_cvr")
      .withColumn("new_cost", when(col("cvr_cnt")<20 && col("new_type_flag")===1, col("total_cost")).otherwise(col("cost")))
      .withColumn("new_ctr_cnt", when(col("cvr_cnt")<20 && col("new_type_flag")===1, col("total_ctr")).otherwise(col("ctr_cnt")))
      .withColumn("new_cvr_cnt", when(col("cvr_cnt")<20 && col("new_type_flag")===1, col("total_cvr")).otherwise(col("cvr_cnt")))


    joinData2.write.mode("overwrite").saveAsTable("test.ocpc_final_join_table")
    joinData2
  }

  def getCvr3Data(date: String, hour: String, hourCnt: Int, spark: SparkSession) :DataFrame ={
    /**
      * 按照给定的时间区间获取从OcpcActivationData程序的结果表获取历史数据
      */

    // 取历史数据
    val dateConverter = new SimpleDateFormat("yyyy-MM-dd HH")
    val newDate = date + " " + hour
    val today = dateConverter.parse(newDate)
    val calendar = Calendar.getInstance
    calendar.setTime(today)
    calendar.add(Calendar.HOUR, -hourCnt)
    val yesterday = calendar.getTime
    val tmpDate = dateConverter.format(yesterday)
    val tmpDateValue = tmpDate.split(" ")
    val date1 = tmpDateValue(0)
    val hour1 = tmpDateValue(1)
    val selectCondition = getTimeRangeSql2(date1, hour1, date, hour)

    // read data and set redis configuration
    val sqlRequest =
      s"""
         |SELECT
         |  ideaid,
         |  adclass,
         |  cost,
         |  ctr_cnt,
         |  cvr_cnt,
         |  hour
         |FROM
         |  dl_cpc.ocpc_ideaid_adclass_label3_track_v1
         |WHERE $selectCondition
       """.stripMargin
    println(sqlRequest)
    val resultDF = spark.sql(sqlRequest)
    resultDF
  }



}

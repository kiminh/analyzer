package com.cpc.spark.OcpcProtoType.suggest_cpa_qtt

import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.ocpcV3.ocpc.OcpcUtils._
import com.cpc.spark.ocpcV3.utils
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import com.cpc.spark.OcpcProtoType.suggest_cpa_v1.OcpcCalculateAUC._
import org.apache.log4j.{Level, Logger}

object OcpcCalculateAUCv2 {
  def main(args: Array[String]): Unit = {
    // 计算日期周期
    Logger.getRootLogger.setLevel(Level.WARN)
    val date = args(0).toString
    val hour = args(1).toString
    val conversionGoal = args(2).toInt
    val version = args(3).toString
    val hourInt = args(4).toInt
    val spark = SparkSession
      .builder()
      .appName(s"ocpc identifier auc: $date, $hour, $conversionGoal")
      .enableHiveSupport().getOrCreate()

    // 抽取数据
    val data = getData("qtt", conversionGoal, hourInt, version, date, hour, spark)
    val tableName = "dl_cpc.ocpc_auc_raw_data"
    data
      .repartition(10).write.mode("overwrite").insertInto(tableName)

    // 获取identifier与industry之间的关联表
    val unitidIndustry = getIndustry(tableName, conversionGoal, version, date, hour, spark)

    // 计算auc
    val aucData = getAuc(tableName, conversionGoal, version, date, hour, spark)

    val result = aucData
      .join(unitidIndustry, Seq("identifier"), "left_outer")
      .select("identifier", "auc", "industry")

    val conversionGoalInt = conversionGoal.toInt
    val resultDF = result
      .withColumn("conversion_goal", lit(conversionGoalInt))
      .withColumn("date", lit(date))
      .withColumn("hour", lit(hour))
      .withColumn("version", lit(version))

    val finalTableName = "test.ocpc_unitid_auc_daily_" + conversionGoal
    resultDF
      .repartition(10).write.mode("overwrite").insertInto("dl_cpc.ocpc_unitid_auc_hourly")
    //        .write.mode("overwrite").saveAsTable(finalTableName)
  }

}
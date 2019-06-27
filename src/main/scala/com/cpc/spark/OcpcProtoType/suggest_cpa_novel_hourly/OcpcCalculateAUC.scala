package com.cpc.spark.OcpcProtoType.suggest_cpa_novel_hourly

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import com.cpc.spark.OcpcProtoType.suggest_cpa_v2.OcpcCalculateAUC._

object OcpcCalculateAUC {
  def main(args: Array[String]): Unit = {
    // 计算日期周期
    Logger.getRootLogger.setLevel(Level.WARN)
    val date = args(0).toString
    val hour = args(1).toString
    val conversionGoal = args(2).toInt
    val version = args(3).toString
    val media = args(4).toString
    val hourInt = args(5).toInt
    val spark = SparkSession
      .builder()
      .appName(s"ocpc identifier auc: $date, $hour, $conversionGoal")
      .enableHiveSupport().getOrCreate()

    // 抽取数据
    val data = getData(media, conversionGoal, hourInt, version, date, hour, spark)
//    val tableName = "test.ocpc_auc_raw_conversiongoal_" + conversionGoal
//    data
//      .repartition(10).write.mode("overwrite").saveAsTable(tableName)
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

    val finalTableName = "test.ocpc_unitid_auc_hourly_" + conversionGoal
    resultDF
      .repartition(10).write.mode("overwrite").insertInto("dl_cpc.ocpc_unitid_auc_hourly")
//        .write.mode("overwrite").saveAsTable(finalTableName)
  }


}
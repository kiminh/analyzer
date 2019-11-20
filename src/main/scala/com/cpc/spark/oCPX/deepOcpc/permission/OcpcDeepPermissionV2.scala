package com.cpc.spark.oCPX.deepOcpc.permission

import com.cpc.spark.oCPX.deepOcpc.permission.OcpcDeepCalculateAUC._
import com.cpc.spark.oCPX.deepOcpc.permission.OcpcDeepCalculateCV._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._


object OcpcDeepPermissionV2 {
  def main(args: Array[String]): Unit = {
    // 计算日期周期
    Logger.getRootLogger.setLevel(Level.WARN)
    val date = args(0).toString
    val hour = args(1).toString
    val version = args(2).toString
    val hourInt = args(3).toInt
    val spark = SparkSession
      .builder()
      .appName(s"ocpc identifier auc: $date, $hour")
      .enableHiveSupport().getOrCreate()

    println(s"parameters: date=$date, hour=$hour, version=$version, hourInt=$hourInt")

    // 计算auc
    val auc = OcpcDeepCalculateAUCmain(date, hour, hourInt, spark)

    // 计算cv
    val cv = OcpcDeepCalculateCVmain(date, hour, hourInt, spark)

    // 数据关联
    val data = cv
      .join(auc, Seq("identifier", "media", "deep_conversion_goal"), "outer")
      .na.fill(0, Seq("cv"))
      .na.fill(-1.0, Seq("auc"))
      .withColumn("flag", udfDetermineFlag()(col("cv"), col("auc")))
      .select("identifier", "media", "deep_conversion_goal", "cv", "auc", "flag", "cost")
      .withColumn("cpa", col("cost") / col("cv"))
      .cache()
    data.show(10)

    data
      .withColumn("date", lit(date))
      .withColumn("version", lit(version))
      .repartition(1)
      .write.mode("overwrite").insertInto("test.ocpc_deep_white_unit_daily")
//      .write.mode("overwrite").insertInto("dl_cpc.ocpc_deep_white_unit_daily")

    data
      .withColumn("version", lit(version))
      .repartition(1)
      .write.mode("overwrite").insertInto("test.ocpc_deep_white_unit_version")
//      .write.mode("overwrite").insertInto("dl_cpc.ocpc_deep_white_unit_version")

    val prevData = getPrevData(version, spark)
    val resultDF = updateLight(prevData, data, spark)

    resultDF
      .withColumn("version", lit(version))
      .repartition(1)
      .write.mode("overwrite").insertInto("test.ocpc_deep_status_light_version")
//      .write.mode("overwrite").insertInto("dl_cpc.ocpc_deep_status_light_version")
  }

  def udfDetermineFlag() = udf((cv: Int, auc: Double) => {
    var result = 0
    if (cv > 10 && auc > 0.55) {
      result = 1
    }
    result
  })

  def updateLight(prevData: DataFrame, data: DataFrame, spark: SparkSession) = {
    val currentData = data
      .groupBy("identifier")
      .agg(
        max(col("flag")).alias("current_flag")
      )
      .select("identifier", "current_flag")

    val result = currentData
      .join(prevData, Seq("identifier"), "outer")
      .select("identifier", "prev_flag", "current_flag")
      .na.fill(0, Seq("prev_flag", "current_flag"))
      .withColumn("flag", udfCheckFlag()(col("prev_flag"), col("current_flag")))

    result
  }

  def udfCheckFlag() = udf((prevFlag: Int, currentFlag: Int) => {
    val result = math.max(prevFlag, currentFlag)
    result
  })

  def getPrevData(version: String, spark: SparkSession) = {
    val sqlRequest =
      s"""
         |SELECT
         |  identifier,
         |  flag
         |FROM
         |  dl_cpc.ocpc_deep_status_light_version
         |WHERE
         |  version = '$version'
         |""".stripMargin
    println(sqlRequest)
    val data = spark
        .sql(sqlRequest)
        .groupBy("identifier")
        .agg(
          max(col("flag")).alias("prev_flag")
        )
        .select("identifier", "prev_flag")

    data
  }

}
package com.cpc.spark.oCPX.deepOcpc.permission

import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.OcpcProtoType.OcpcTools.getTimeRangeSqlDate
import com.cpc.spark.oCPX.deepOcpc.permission.OcpcDeepCalculateAUC._
import com.cpc.spark.oCPX.deepOcpc.permission.OcpcDeepCalculateCV._
import com.cpc.spark.oCPX.deepOcpc.permission.OcpcDeepPermission.udfDetermineFlag
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
    val spark = SparkSession
      .builder()
      .appName(s"ocpc identifier auc: $date, $hour")
      .enableHiveSupport().getOrCreate()

    println(s"parameters: date=$date, hour=$hour, version=$version")

    /*
    次留单元的准入数据
     */
    // 取历史数据
    val dateConverter = new SimpleDateFormat("yyyy-MM-dd HH")
    val newDate = date + " " + hour
    val today = dateConverter.parse(newDate)
    val calendar = Calendar.getInstance
    calendar.setTime(today)
    calendar.add(Calendar.DATE, -1)
    val yesterday = calendar.getTime
    val tmpDate = dateConverter.format(yesterday)
    val tmpDateValue = tmpDate.split(" ")
    val date1 = tmpDateValue(0)
    val hour1 = tmpDateValue(1)
    println("retention data #################################")
    val retentionData = getPermissionData(date1, hour1, 72, 2, spark)

    /*
    付费单元的准入数据
     */
    println("pay data ######################################")
    val payData = getPermissionData(date, hour, 72, 3, spark)

    /*
    1.union数据
    2.判断是否准入
    3.保存数据
     */
    val data = retentionData
      .union(payData)
      .withColumn("flag", udfDetermineFlag()(col("cv"), col("auc")))
      .withColumn("cpa", col("deep_cpareal"))
      .select("identifier", "media", "deep_conversion_goal", "cv", "auc", "flag", "cost", "cpa", "deep_cpagiven", "click")

    data
      .withColumn("date", lit(date))
      .withColumn("version", lit(version))
      .repartition(1)
      .write.mode("overwrite").insertInto("test.ocpc_deep_white_unit_daily")
    //      .write.mode("overwrite").insertInto("dl_cpc.ocpc_deep_white_unit_daily")

    /*
    读取历史准入数据
     */
    val prevData = getPrevData(version, spark)

    /*
    更新准入数据
     */
    val result = updateData(prevData, data, spark)

    result
        .repartition(1)
        .write.mode("overwrite").saveAsTable("test.ocpc_deep_white_unit_version20191120")

    /*
    保存数据
     */
    result
      .select("identifier", "media", "deep_conversion_goal", "cv", "auc", "flag", "cost", "cpa", "deep_cpagiven", "click")
      .withColumn("version", lit(version))
      .repartition(1)
      .write.mode("overwrite").insertInto("test.ocpc_deep_white_unit_version")
    //      .write.mode("overwrite").insertInto("dl_cpc.ocpc_deep_white_unit_version")


    result
      .select("identifier", "media", "deep_conversion_goal", "cv", "auc", "flag", "cost", "cpa", "deep_cpagiven", "click")
      .withColumn("date", lit(date))
      .withColumn("version", lit(version))
      .repartition(1)
      .write.mode("overwrite").insertInto("test.ocpc_deep_white_unit_backup_daily")
    //      .write.mode("overwrite").insertInto("dl_cpc.ocpc_deep_white_unit_backup_daily")
  }

  def updateData(prevData: DataFrame, data: DataFrame, spark: SparkSession) = {
    /*
    过滤出不在prevData，同时在data中推荐的新单元
     */
    val prevDataFlag = prevData
      .select("identifier", "media", "deep_conversion_goal")
      .withColumn("prev_flag", lit(1))
    val currentData = data
      .join(prevDataFlag, Seq("identifier", "media", "deep_conversion_goal"), "left_outer")
      .na.fill(0, Seq("prev_flag"))
      .filter(s"prev_flag = 0")
      .select("identifier", "media", "deep_conversion_goal", "cv", "auc", "flag", "cost", "cpa", "deep_cpagiven", "click")
      .withColumn("is_new", lit(1))

    val result = prevData.union(currentData)

    result
  }

  def getPrevData(version: String, spark: SparkSession) = {
    val sqlRequest =
      s"""
         |SELECT
         |  *
         |FROM
         |  test.ocpc_deep_white_unit_version
         |WHERE
         |  version = '$version'
         |AND
         |  flag = 1
         |""".stripMargin
    println(sqlRequest)
    val data = spark
      .sql(sqlRequest)
      .select("identifier", "media", "deep_conversion_goal", "cv", "auc", "flag", "cost", "cpa", "deep_cpagiven", "click")
      .withColumn("is_new", lit(0))
      .cache()

    data.show(10)

    data
  }

  def udfDetermineFlag() = udf((cv: Int, auc: Double) => {
    var result = 0
    if (cv > 10 && auc > 0.55) {
      result = 1
    }
    result
  })

  def getPermissionData(date: String, hour: String, hourInt: Int, deepConversionGoal: Int, spark: SparkSession) = {
    // 计算auc
    val auc = OcpcDeepCalculateAUCmain(date, hour, hourInt, deepConversionGoal, spark)

    // 计算cv
    val cv = OcpcDeepCalculateCVmain(date, hour, hourInt, deepConversionGoal, spark)

    // 数据关联
    val data = cv
      .join(auc, Seq("identifier", "media", "deep_conversion_goal"), "outer")
      .na.fill(0, Seq("click", "cv", "cost", "deep_cpagiven", "deep_cpareal"))
      .na.fill(-1.0, Seq("auc"))
      .select("identifier", "media", "deep_conversion_goal", "click", "cv", "auc", "cost", "deep_cpagiven", "deep_cpareal")

    data
  }



}
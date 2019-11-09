package com.cpc.spark.oCPX.deepOcpc.permission

import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.OcpcProtoType.OcpcTools._
import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import com.cpc.spark.oCPX.deepOcpc.permission.OcpcDeepCalculateAUC._
import com.cpc.spark.oCPX.deepOcpc.permission.OcpcDeepCalculateCV._


object OcpcDeepPermission {
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

    // 计算auc
    val auc = OcpcDeepCalculateAUCmain(date, hour, hourInt, spark)

    // 计算cv
    val cv = OcpcDeepCalculateCVmain(date, hour, hourInt, spark)

    // 数据关联
    val data = cv
      .join(auc, Seq("identifier", "media", "deep_conversion_goal"), "inner")
      .withColumn("flag", udfDetermineFlag()(col("cv"), col("auc")))

    data
      .write.mode("overwrite").saveAsTable("test.check_ocpc_data_result20191108")
  }

  def udfDetermineFlag() = udf((cv: Int, auc: Double) => {
    var result = 0
    if (cv > 10 && auc > 0.55) {
      result = 1
    }
    result
  })

}
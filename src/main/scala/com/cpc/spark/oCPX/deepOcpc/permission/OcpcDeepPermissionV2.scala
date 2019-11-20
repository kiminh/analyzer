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

    /*
    次留单元的准入数据
     */
    val permissionData = getPermissionData(date, hour, hourInt, 2, spark)

    /*
    付费单元的准入数据
     */


    /*
    1.union数据
    2.判断是否准入
    3.保存数据
     */

    /*
    读取历史准入数据
     */

    /*
    更新准入数据
     */

  }

  def getPermissionData(date: String, hour: String, hourInt: Int, deepConversionGoal: Int, spark: SparkSession) = {
    // 计算auc
    val auc = OcpcDeepCalculateAUCmain(date, hour, hourInt, deepConversionGoal, spark)

    // 计算cv
    val cv = OcpcDeepCalculateCVmain(date, hour, hourInt, deepConversionGoal, spark)
  }



}
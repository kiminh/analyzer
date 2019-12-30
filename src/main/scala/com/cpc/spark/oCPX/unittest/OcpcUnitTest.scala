package com.cpc.spark.oCPX.unittest



import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.oCPX.deepOcpc.permission.OcpcDeepPermissionV2.{getPermissionData, getUnitInfo}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}


object OcpcUnitTest {
  /*
  新增部分媒体id采用暗投
   */
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()
    Logger.getRootLogger.setLevel(Level.WARN)

    val date = args(0).toString
    val hour = args(1).toString
//    val version = "ocpctest"
//    val expTag = "weightv1"
//    val hourInt = 89

    println("parameters:")
    println(s"date=$date, hour=$hour")

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

    // get unit info
    val unitInfo = getUnitInfo(spark)

    /*
    1.union数据
    2.判断是否准入
    3.保存数据
     */
    val data = retentionData
      .union(payData)
      .join(unitInfo, Seq("identifier", "deep_conversion_goal"), "left_outer")

    data
      .write.mode("overwrite").saveAsTable("test.check_ocpc_data201901230b")



  }

}



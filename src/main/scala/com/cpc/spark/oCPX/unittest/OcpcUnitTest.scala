package com.cpc.spark.oCPX.unittest


import com.cpc.spark.oCPX.oCPC.pay.v2.OcpcChargeCost.{getDeepData, getShallowData}
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

    println("parameters:")
    println(s"date=$date, hour=$hour")

    val shallowOcpcData = getDeepData(date, 7, spark)

    shallowOcpcData
      .write.mode("overwrite").saveAsTable("test.check_ocpc_data20200113b")



  }

}



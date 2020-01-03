package com.cpc.spark.oCPX.unittest


import com.cpc.spark.oCPX.oCPC.report.OcpcHourlyReportV2.{calculateData, getBaseData}
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


    // 拉取点击、消费、转化等基础数据
    val rawData = getBaseData(date, hour, spark)

    // stage3
    val stage3DataRaw = rawData.filter(s"deep_ocpc_step = 2")
    val stage3Data = calculateData(stage3DataRaw, spark)

    stage3Data
      .write.mode("overwrite").saveAsTable("test.check_ocpc_data20200103a")



  }

}



package com.cpc.spark.oCPX.unittest


import com.cpc.spark.oCPX.deepOcpc.DeepOcpcTools.{getDeepData, getDeepDataDelay}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession


object OcpcUnitTest {
  /*
  新增部分媒体id采用暗投
   */
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()
    Logger.getRootLogger.setLevel(Level.WARN)

    val date = args(0).toString
    val hour = args(1).toString
    val hourInt = 24

    println("parameters:")
    println(s"date=$date, hour=$hour")


    val data1 = getDeepData(23, date, hour, spark)
    val data2 = getDeepDataDelay(23, date, hour, spark)

    data1
      .write.mode("overwrite").saveAsTable("test.check_deep_ocpc_data20200205a")

    data2
      .write.mode("overwrite").saveAsTable("test.check_deep_ocpc_data20200205b")



  }

}



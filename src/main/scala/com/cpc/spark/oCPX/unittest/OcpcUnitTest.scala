package com.cpc.spark.oCPX.unittest


import com.cpc.spark.oCPX.oCPC.assessment.OcpcCalibration_assessment.getPcoc
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


    val pcocData = getPcoc(date, hour, 24, "qtt", spark)

//    data
//      .write.mode("overwrite").saveAsTable("test.check_cv_recall20200120c")

    pcocData
      .write.mode("overwrite").saveAsTable("test.check_ocpc_exp_data20200120a")



  }

}



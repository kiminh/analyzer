package com.cpc.spark.oCPX.unittest

import com.cpc.spark.oCPX.cv_recall.deep_cv.OcpcDeepCVrecall_assessment.{calculateCV, cvRecallAssessment}
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
    val hourInt = 24

    println("parameters:")
    println(s"date=$date, hour=$hour")



    val data = cvRecallAssessment(date, 1, spark)


    data
      .write.mode("overwrite").saveAsTable("test.check_ocpc_exp_data20200203h")



  }

}



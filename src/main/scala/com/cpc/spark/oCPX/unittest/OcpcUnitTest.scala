package com.cpc.spark.oCPX.unittest


import com.cpc.spark.oCPX.cv_recall.shallow_cv.OcpcShallowCVrecall_assessmentV2.{calculateCV, cvRecallPredictV1, cvRecallPredictV2, predictCvValue}
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
    val hourInt = 6

    println("parameters:")
    println(s"date=$date, hour=$hour")


    val cvData = calculateCV(date, hourInt, spark)
    val recallValue1 = cvRecallPredictV1(date, spark)
    val recallValue2 = cvRecallPredictV2(date, spark)
    val predCvData = predictCvValue(cvData, 1, hourInt, recallValue1, recallValue2, spark)

    recallValue1
      .write.mode("overwrite").saveAsTable("test.check_shallow_ocpc_data20200206a")

    recallValue2
      .write.mode("overwrite").saveAsTable("test.check_shallow_ocpc_data20200206b")


    predCvData
      .write.mode("overwrite").saveAsTable("test.check_shallow_ocpc_data20200206c")



  }

}



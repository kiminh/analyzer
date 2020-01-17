package com.cpc.spark.oCPX.unittest

import com.cpc.spark.oCPX.cv_recall.shallow_cv.OcpcShallowCVrecall_predict.{calculateCV, calculateRecallValue}
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


    val cvData = calculateCV(date, 6, spark)

    var data = calculateRecallValue(cvData, 1, 6, spark)

    for (startHour <- 2 to 24) {
      val singleData = calculateRecallValue(cvData, startHour, 6, spark)
      data = data.union(singleData)
    }

    data
      .write.mode("overwrite").saveAsTable("test.check_ocpc_data20200117d")



  }

}



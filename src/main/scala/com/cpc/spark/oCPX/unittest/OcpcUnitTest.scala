package com.cpc.spark.oCPX.unittest


import com.cpc.spark.oCPX.cv_recall.shallow_cv.OcpcShallowCVrecall_assessment.calculateCV
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
    val version = "ocpctest"

    println("parameters:")
    println(s"date=$date, hour=$hour")


    val cvData = calculateCV(date, 6, spark)

    cvData
      .write.mode("overwrite").saveAsTable("test.check_shallow_ocpc_data20200206a")



  }

}



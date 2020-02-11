package com.cpc.spark.oCPX.unittest


import com.cpc.spark.oCPX.cv_recall.shallow_cv.OcpcShallowCVrecall_assessmentV3.cvRecallPredict
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, concat_ws, lit}


object OcpcUnitTest {
  /*
  新增部分媒体id采用暗投
   */
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()
    Logger.getRootLogger.setLevel(Level.WARN)

    val date = args(0).toString
    val hour = args(1).toString
    val version = "ocpcv1"
    val expTag = "adtype15"

    println("parameters:")
    println(s"date=$date, hour=$hour")


    val recallValue = cvRecallPredict(date, spark)
    recallValue
      .write.mode("overwrite").saveAsTable("test.check_shallow_ocpc_data20200211a")



  }

}



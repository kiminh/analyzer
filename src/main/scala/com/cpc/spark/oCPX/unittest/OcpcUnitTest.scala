package com.cpc.spark.oCPX.unittest


import com.cpc.spark.oCPX.OcpcTools.{getBaseDataRealtime, getConversionGoalNew}
import com.cpc.spark.oCPX.cv_recall.shallow_cv.OcpcShallowCV_delay.getUserDelay
import com.cpc.spark.oCPX.oCPC.calibration_by_tag.OcpcGetPb_weightv5.{OcpcCalibrationBase, OcpcRealtimeCalibrationBase, udfCalculateHourDiff}
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




    val baseDataRaw = getBaseDataRealtime(hourInt, date, hour, spark)

    val delayDataRaw = getUserDelay(date, spark)
    val delayData = delayDataRaw
      .withColumn("delay_hour", when(col("hour_diff") > 6, 6).otherwise(col("hour_diff")))
      .filter(s"cost > 100")
      .select("userid", "conversion_goal", "delay_hour")

    val unitUserInfoRaw = getConversionGoalNew(spark)
    val unitUserInfo = unitUserInfoRaw.select("unitid", "userid").distinct().cache()
    unitUserInfo.show(10)

    val baseData = baseDataRaw
      .join(unitUserInfo, Seq("unitid"), "inner")
      .join(delayData, Seq("userid", "conversion_goal"), "left_outer")
      .na.fill(0.0, Seq("delay_hour"))
      .withColumn("hour_diff", udfCalculateHourDiff(date, hour)(col("date"), col("hour"), lit(1)))




    baseData
      .write.mode("overwrite").saveAsTable("test.check_ocpc_exp_data20200121a")



  }

}



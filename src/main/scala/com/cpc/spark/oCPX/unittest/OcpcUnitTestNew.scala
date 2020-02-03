package com.cpc.spark.oCPX.unittest

import com.cpc.spark.oCPX.deepOcpc.calibration_v9.OcpcGetPb_retention.{OcpcCalibrationBase, calculateCvrPart1, calculateDeepCvr, getDataByHourDiff, getRecallValue}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, lit, sum, when}


object OcpcUnitTestNew {
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


    val dataRaw = OcpcCalibrationBase(date, hour, 96, spark)

    val minCV = 20
    // calculate deep_cvr
    val deepCvrRaw = calculateDeepCvr(date, 3, spark)

    val dataRaw1 = getDataByHourDiff(dataRaw, 24, 48, spark)

    val recallValue = getRecallValue(spark).cache()
    recallValue.show(10)

    val deepCvr = deepCvrRaw
      .groupBy("unitid")
      .agg(
        sum(col("cv1")).alias("cv1"),
        sum(col("cv2")).alias("cv2")
      )
      .filter(s"cv2 >= 10")
      .withColumn("deep_cvr", col("cv2") * 1.0 / col("cv1"))
      .selectExpr("unitid", "cast(deep_cvr as double) as deep_cvr")


    val data1 = dataRaw1
      .withColumn("flag", when(col("cv2") >= minCV, 1).otherwise(0))
      .withColumn("hour_diff", lit(24))
      .join(recallValue, Seq("conversion_goal", "hour_diff"), "left_outer")
      .na.fill(1.0, Seq("recall_value"))
      .withColumn("cv2_recall1", col("cv2") * col("recall_value"))
      .join(deepCvr, Seq("unitid"), "left_outer")
      .withColumn("cv2_recall2", col("cv1") * col("deep_cvr"))
      .withColumn("cv2_recall", when(col("deep_cvr").isNotNull, (col("cv2_recall1") + col("cv2_recall2")) * 1.0 / 2).otherwise(col("cv2")))


    data1
      .write.mode("overwrite").saveAsTable("test.check_ocpc_cali_exp_data20200203c")



  }

}



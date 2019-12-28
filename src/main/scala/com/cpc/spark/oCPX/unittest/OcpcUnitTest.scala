package com.cpc.spark.oCPX.unittest


import com.cpc.spark.oCPX.deepOcpc.calibration_v7.OcpcGetPb_retention.{OcpcCalibrationBase, calculateCvrPart1, calculateCvrPart2, calculateDeepCvr, getDataByHourDiff}
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
//    val version = "ocpctest"
//    val expTag = "weightv1"
//    val hourInt = 89

    println("parameters:")
    println(s"date=$date, hour=$hour")

//    // 拉取点击、消费、转化等基础数据
//    val rawData = getBaseData(date, hour, spark)
//
//    // stage3
//    val stage3DataRaw = rawData.filter(s"deep_ocpc_step = 2")
//    val stage3Data = calculateData(stage3DataRaw, spark)
//
//    // stage2
//    val stage2DataRaw = rawData.filter(s"deep_ocpc_step != 2 and ocpc_step = 2")
//    val stage2Data = calculateData(stage2DataRaw, spark)
//
//    // stage1
//    val stage1DataRaw = rawData.filter(s"ocpc_step = 1")
//    val stage1Data = calculateData(stage1DataRaw, spark)
//
//
//    stage3Data
//      .write.mode("overwrite").saveAsTable("test.check_ocpc_data201901227a")
//
//    stage2Data
//      .write.mode("overwrite").saveAsTable("test.check_ocpc_data201901227b")
//
//    stage1Data
//      .write.mode("overwrite").saveAsTable("test.check_ocpc_data201901227c")


    val dataRaw = OcpcCalibrationBase(date, hour, 96, spark)

    val deepCvr = calculateDeepCvr(date, 3, spark)

    // calculate cv2_t1
    val data1 = calculateCvrPart1(dataRaw, deepCvr, 10, spark)

    // calculate cv2_t2 ~ cv2_t4
    val data2 = calculateCvrPart2(dataRaw, 20, spark)

    // data join
    val data = data1.union(data2)

    data.createOrReplaceTempView("data")

    val sqlRequest =
      s"""
         |SELECT
         |  unitid,
         |  conversion_goal,
         |  media,
         |  sum(click) as click,
         |  sum(cv2) as cv2,
         |  sum(pre_cvr2 * click) * 1.0 / sum(click) as pre_cvr2,
         |  sum(cv2_recall) as cv2_recall
         |FROM
         |  result_table
         |GROUP BY unitid, conversion_goal, media
         |""".stripMargin
    println(sqlRequest)
    val result = spark.sql(sqlRequest)
      .withColumn("post_cvr2", col("cv2_recall") * 1.0 / col("click"))
      .withColumn("cvr_factor", col("post_cvr2") * 1.0 / col("pre_cvr2"))

    result
      .write.mode("overwrite").saveAsTable("test.check_ocpc_data201901227c")



  }

}



package com.cpc.spark.oCPX.unittest


import com.cpc.spark.oCPX.oCPC.calibration_by_tag.v1.OcpcGetPb_v1_part2.{OcpcCVRfactor, OcpcCalibrationBase, OcpcJFBfactor, OcpcRealtimeCalibrationBase, assemblyData}
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

    println("parameters:")
    println(s"date=$date, hour=$hour")


    val date1 = "2020-02-2"
    val hour1 = args(3).toString
    val version = args(4).toString
    val expTag = args(5).toString

    // 主校准回溯时间长度
    val hourInt1 = args(6).toInt
    // 备用校准回溯时间长度
    val hourInt2 = args(7).toInt
    // 兜底校准时长
    val hourInt3 = args(8).toInt

    // 计费比系数模块
    val dataRaw = OcpcCalibrationBase(date1, hour1, hourInt3, spark).cache()
    dataRaw.show(10)
    val jfbDataRaw = OcpcJFBfactor(date1, hour1, expTag, dataRaw, hourInt1, hourInt2, hourInt3, spark)
    val jfbData = jfbDataRaw
      .withColumn("jfb_factor", lit(1.0) / col("jfb"))
      .select("unitid", "conversion_goal", "exp_tag", "jfb_factor")
      .cache()
    jfbData.show(10)

    // 校准系数模块
    val realtimeDataRaw = OcpcRealtimeCalibrationBase(date, hour, 100, spark).cache()
    realtimeDataRaw.show(10)
    val pcocDataRaw = OcpcCVRfactor(realtimeDataRaw, date, expTag, spark)
    val pcocData = pcocDataRaw
      .withColumn("cvr_factor", lit(1.0) / col("pcoc"))
      .select("unitid", "conversion_goal", "exp_tag", "cvr_factor")
      .cache()
    pcocData.show(10)

    val data = assemblyData(jfbData, pcocData, spark).cache()
    data.show(10)

    dataRaw.unpersist()

    // 明投单元
    val resultDF = data
      .withColumn("cpagiven", lit(1.0))
      .withColumn("is_hidden", lit(0))
      .withColumn("date", lit(date1))
      .withColumn("hour", lit(hour1))
      .withColumn("version", lit(version))
      .withColumn("exp_tag", concat_ws("-", col("exp_tag"), col("conversion_goal")))
      .select("unitid", "conversion_goal", "jfb_factor", "post_cvr", "smooth_factor", "cvr_factor", "high_bid_factor", "low_bid_factor", "cpagiven", "date", "hour", "exp_tag", "is_hidden", "version")

    realtimeDataRaw
      .write.mode("overwrite").saveAsTable("test.check_shallow_ocpc_data20200206c")



  }

}



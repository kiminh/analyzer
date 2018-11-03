package com.cpc.spark.ocpc

import com.cpc.spark.ocpc.OcpcPcvrCalibration.calibrationV1
import com.cpc.spark.udfs.Udfs_wj._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, sum, when}
import com.cpc.spark.ocpc.OcpcUtils._

object OcpcPactCalibration {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()

    // calculate time period for historical data
    val date = args(0).toString
    val hour = args(1).toString

    val result = calibrationV1(date, hour, spark)
    result.write.mode("overwrite").saveAsTable("test.ocpc_new_calibration_value_cvr3")
  }

  def calibrationV1(date: String, hour: String, spark: SparkSession) :DataFrame = {
    /**
      * 对api回传数据的预测模型进行校准
      */
    // 前一段时间的ctr和cvr数据
    val historyData1 = getActData(date, hour, 24 * 7, spark)
    val ctrcvrData = historyData1
      .groupBy("ideaid", "adclass")
      .agg(
        sum(col("ctr_cnt")).alias("ctr_cnt"),
        sum(col("cvr_cnt")).alias("cvr_cnt"))
      .select("ideaid", "adclass", "ctr_cnt", "cvr_cnt")


    // 前一段时间的pcvr数据
    val historyData2 = getPcvrData(date, hour, 24 * 7, spark)
    val pcvrData = historyData2
      .groupBy("ideaid", "adclass")
      .agg(
        sum(col("total_cvr")).alias("total_cvr"),
        sum(col("cnt")).alias("cnt"))
      .select("ideaid", "adclass", "total_cvr", "cnt")


    // 关联数据
    val rawData = ctrcvrData
      .join(pcvrData, Seq("ideaid", "adclass"), "left_outer")
      .select("ideaid", "adclass", "ctr_cnt", "cvr_cnt", "total_cvr", "cnt")
      .withColumn("hcvr", col("cvr_cnt")/col("ctr_cnt"))
      .withColumn("hpcvr", col("total_cvr")/col("cnt"))
      .withColumn("cali_value", col("hcvr")/col("hpcvr"))

    // TODO 删除临时表
    rawData.write.mode("overwrite").saveAsTable("test.ocpc_new_calibration_value1_cvr3")

     val resultDF = rawData.select("ideaid", "adclass", "hcvr", "hpcvr", "cali_value")

    // TODO 删除临时表
    resultDF.write.mode("overwrite").saveAsTable("test.ocpc_new_calibration_value2_cvr3")

    resultDF
  }
}
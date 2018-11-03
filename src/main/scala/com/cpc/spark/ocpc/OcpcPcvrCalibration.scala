package com.cpc.spark.ocpc

import org.apache.spark.sql.{DataFrame, SparkSession}
import com.cpc.spark.ocpc.OcpcUtils._
import org.apache.spark.sql.functions._
import com.cpc.spark.udfs.Udfs_wj._

object OcpcPcvrCalibration {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()

    // calculate time period for historical data
    val date = args(0).toString
    val hour = args(1).toString

    val historyData = getCompleteData(date, hour, 24 * 7, spark)
    val result = calibrationV1(historyData, spark)
    result.write.mode("overwrite").saveAsTable("test.ocpc_new_calibration_value")
  }

  def calibrationV1(historyData: DataFrame,spark: SparkSession) :DataFrame = {
    val resultDF = historyData
      .withColumn("timespan", udfHourToTimespan()(col("hour")))
      .select("ideaid", "adclass", "timespan", "ctr_cnt", "cvr_cnt")
      .groupBy("ideaid", "adclass", "timespan")
      .agg(
        sum(col("ctr_cnt")).alias("ctr_cnt"),
        sum(col("cvr_cnt")).alias("cvr_cnt"))
      .withColumn("weight", udfTimespanToWeight()(col("timespan")))
      .withColumn("weighted_ctr", col("ctr_cnt") * col("weight"))
      .withColumn("weighted_cvr", col("cvr_cnt") * col("weight"))

    resultDF

  }

}

package com.cpc.spark.ocpc

import java.text.SimpleDateFormat
import java.util.Calendar

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

    val result = calibrationV2(date, hour, spark)
    result.write.mode("overwrite").saveAsTable("test.ocpc_new_calibration_value_cvr3")
  }

  def calibrationV0(date: String, hour: String, spark: SparkSession) :DataFrame = {
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
    val historyData2 = getPactData(date, hour, 24 * 7, spark)
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

  def calibrationV2(date: String, hour: String, spark: SparkSession) :DataFrame = {
    /**
      * 对pcvr进行校准：按时间段分别计算进行校准
      * t1: 0 ~ 5
      * t2: 6 ~ 19
      * t3: 20 ~ 23
      * 对应时段内，权重为0.6，其他时段权重各为0.2
      */

    // 取历史数据
    val dateConverter = new SimpleDateFormat("yyyy-MM-dd HH")
    val newDate = date + " " + hour
    val today = dateConverter.parse(newDate)
    val calendar = Calendar.getInstance
    calendar.setTime(today)
    calendar.add(Calendar.HOUR, 3)
    val yesterday = calendar.getTime
    val tmpDate = dateConverter.format(yesterday)
    val tmpDateValue = tmpDate.split(" ")
    val date1 = tmpDateValue(0)
    val hour1 = tmpDateValue(1)


    // 前一段时间的ctr和cvr数据
    val historyData1 = getActData(date, hour, 24 * 7, spark)
    val baseData = historyData1
      .select("ideaid", "adclass", "hour")
      .withColumn("timespan", udfHourToTimespan()(col("hour")))
      .select("ideaid", "adclass", "timespan")
      .distinct()

    val ctrcvrData = historyData1
      .withColumn("timespan", udfHourToTimespan()(col("hour")))
      .select("ideaid", "adclass", "timespan", "ctr_cnt", "cvr_cnt")
      .groupBy("ideaid", "adclass", "timespan")
      .agg(
        sum(col("ctr_cnt")).alias("ctr_cnt"),
        sum(col("cvr_cnt")).alias("cvr_cnt"))
      .select("ideaid", "adclass", "timespan", "ctr_cnt", "cvr_cnt")

    // 前一段时间的pcvr数据
    val historyData2 = getPactData(date, hour, 24 * 7, spark)
    val pcvrData = historyData2
      .withColumn("timespan", udfHourToTimespan()(col("hour")))
      .select("ideaid", "adclass", "timespan", "total_cvr", "cnt")
      .groupBy("ideaid", "adclass", "timespan")
      .agg(
        sum(col("total_cvr")).alias("total_cvr"),
        sum(col("cnt")).alias("cnt"))
      .select("ideaid", "adclass", "timespan", "total_cvr", "cnt")

    // 关联数据
    val rawData = baseData
      .join(ctrcvrData, Seq("ideaid", "adclass", "timespan"), "left_outer")
      .select("ideaid", "adclass", "timespan", "ctr_cnt", "cvr_cnt")
      .join(pcvrData, Seq("ideaid", "adclass", "timespan"), "left_outer")
      .select("ideaid", "adclass", "timespan", "ctr_cnt", "cvr_cnt", "total_cvr", "cnt")
      .withColumn("weight", udfTimespanToWeightV2(hour1)(col("timespan")))
      .withColumn("hcvr", col("cvr_cnt") * 1.0 / col("ctr_cnt"))
      .withColumn("hpcvr", col("total_cvr") * 1.0 / col("cnt"))
      .withColumn("cali_hcvr", col("weight") * col("hcvr"))
      .withColumn("cali_hpcvr", col("weight") * col("hpcvr"))


    // TODO 删除临时表
    rawData.write.mode("overwrite").saveAsTable("test.ocpc_new_calibration_value1_cvr3")

    val resultDF = rawData
      .groupBy("ideaid", "adclass")
      .agg(
        sum(col("cali_hcvr")).alias("cali_hcvr"),
        sum(col("cali_hpcvr")).alias("cali_hpcvr"))
      .withColumn("base_cali_value", col("cali_hcvr")/col("cali_hpcvr"))
      .withColumn("cali_value", when(col("base_cali_value")===0, 1.0).otherwise(col("base_cali_value")))
      .select("ideaid", "adclass", "cali_value")
    // TODO 删除临时表
    resultDF.write.mode("overwrite").saveAsTable("test.ocpc_new_calibration_value2_cvr3")

    resultDF
  }


}
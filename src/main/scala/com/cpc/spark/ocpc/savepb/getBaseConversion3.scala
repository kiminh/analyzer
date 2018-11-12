package com.cpc.spark.ocpc.savepb

import com.cpc.spark.ocpc.utils.OcpcUtils._
import org.apache.spark.sql.{DataFrame, SparkSession}

object getBaseConversion3 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()

    // 计算日期周期
    val date = args(0).toString
    val hour = args(1).toString

    val resultDF = getDataV1(date, hour, spark)
    // TODO: 换成dl_cpc的表
    resultDF.write.mode("overwrite").saveAsTable("test.ocpc_conversion3_base_hourly")
  }

  def getDataV1(date: String, hour: String, spark: SparkSession) :DataFrame = {
    // TODO 表名更换一下
    val rawData = spark.table("test.ocpc_conversion1_base_hourly").where(s"`date`='$date' and `hour`='$hour'")
    val result = filterDataByType(rawData, date, hour, spark)

    result
  }
}

package com.cpc.spark.ocpc.savepb

import org.apache.spark.sql.SparkSession
import com.cpc.spark.ocpc.utils.OcpcUtils._

object getCaliAPI {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()

    // 计算日期周期
    val date = args(0).toString
    val hour = args(1).toString

    val resultDF = spark.table("test.ocpc_new_calibration_value_cvr3")
    // TODO: 换成dl_cpc的表
    resultDF.write.mode("overwrite").saveAsTable("test.ocpc_cvr3_cali_hourly")

  }


}

package com.cpc.spark.ocpc.savepb

import com.cpc.spark.ocpc.utils.OcpcUtils.calculateHPCVR
import org.apache.spark.sql.{DataFrame, SparkSession}

object getCaliNoAPI {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()

    // 计算日期周期
    val date = args(0).toString
    val hour = args(1).toString

    val hpcvr = calculateHPCVR(date, hour, spark)
    val newCali = spark.table("test.ocpc_new_calibration_value")

    // TODO 删除临时表
    hpcvr.write.mode("overwrite").saveAsTable("test.ocpc_hpcvr_base_hourly")
    newCali.write.mode("overwrite").saveAsTable("test.ocpc_cvr2_cali_hourly")
  }

}

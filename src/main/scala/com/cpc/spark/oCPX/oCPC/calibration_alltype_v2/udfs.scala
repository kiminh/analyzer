package com.cpc.spark.oCPX.oCPC.calibration_alltype_v2

import com.cpc.spark.oCPX.OcpcTools._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}


object udfs {
  def udfGenerateId() = udf((unitid: Int, adslotType: Int) => {
    var result = unitid.toString + "&" + adslotType.toString
    result
  })

}
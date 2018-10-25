package com.cpc.spark.udfs

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

import scala.collection.mutable

object Udfs_wj{
  def udfMode1OcpcLogExtractCPA1() = udf((valueLog: String) => {
    val logs = valueLog.split(",")
    val kValue = logs(7).split(":")(1)
    kValue
  })

  def udfModelOcpcLogExtractCPA2() = udf((valueLog: String) => {
    val logs = valueLog.split(",")
    logs(5)
  })

  def udfCalculatePercent(totalCount: Long) = udf((valueCount: Long) => {
    val percent = valueCount * 1.0 / totalCount
    percent
  })

  def udfStringToMap() = udf((valueLog: String) =>{
    var result = mutable.LinkedHashMap[String, String]()
    val logs = valueLog.split(",")
    for (log <- logs) {
      val splits = log.split(":")
      val key = splits(0)
      val value = splits(1)
      result += (key -> value)
    }
    result
  })
}

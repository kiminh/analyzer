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
    val kValue = logs(5).split(":")(1)
    kValue
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

  def udfCalculateWeightByHour(hour: String) = udf((valueHour: String) => {
    val currentHour = hour.toInt + 1
    val tableHour = valueHour.toInt
    var diff = 0
    if (tableHour < currentHour) {
      diff = currentHour - tableHour
    } else {
      diff = 24 - (tableHour - currentHour)
    }
    val numerator = 1 / math.sqrt(diff.toDouble)
    var denominator = 0.0
    for (i <- 1 to 24) {
      denominator += 1 / math.sqrt(i.toDouble)
    }
    val result = numerator.toDouble / denominator
    result
  })

  def udfHourToTimespan() = udf((valHour: String) => {
    val intHour = valHour.toInt
    var result = ""
    if (intHour >= 0 && intHour < 6) {
      result = "t1"
    } else if (intHour >= 6 & intHour <= 19) {
      result = "t2"
    } else {
      result = "t3"
    }
    result
  })

  def udfTimespanToWeight() = udf((valTimespan: String) => {
    var result = 0.1
    if (valTimespan == "t1") {
      result = 0.4
    } else if (valTimespan == "t2") {
      result = 0.4
    } else {
      result = 0.2
    }
    result
  })

  def udfTimespanToWeightV2(hour: String) = udf((valTimespan: String) => {
    val trueHour = hour.toInt
    var hourTag = ""
    if (trueHour <= 5) {
      hourTag = "t1"
    } else if (trueHour > 6 && trueHour <= 19) {
      hourTag = "t2"
    } else {
      hourTag = "t3"
    }
    var result = 0.2
    if (hourTag == valTimespan) {
      result = 0.6
    } else {
      result = 0.2
    }
    result
  })
}

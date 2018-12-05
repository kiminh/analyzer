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

  def udfStringToMap() = udf((valueLog: String) => {
    var result = mutable.LinkedHashMap[String, String]()
    if (valueLog != null && valueLog != "") {
      val logs = valueLog.split(",")
      for (log <- logs) {
        val splits = log.split(":")
        val key = splits(0)
        val value = splits(1)
        result += (key -> value)
      }
      result
    } else {
      null
    }
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

  def udfSetRatioCase() = udf((valueRatio: Double) =>{
    /**
      * 根据新的K基准值和cpa_ratio来在分段函数中重新定义k值
      *
      * case1: ratio < 0, t1
      * case2: ratio < 0.4, t2
      * case3: 0.4 <= ratio < 0.6, t3
      * case4: 0.6 <= ratio < 0.8, t4
      * case5: 0.8 <= ratio < 0.9, t5
      * case6: 0.9 <= ratio <= 1.1, t6
      * case7: 1.1 < ratio <= 1.2, t7
      * case8: 1.2 < ratio <= 1.4, t8
      * case9: 1.4 < ratio <= 1.6, t9
      * case10: ratio > 1.6, t10
      *
      */
    var ratioCase = 0
    if (valueRatio < 0) {
      ratioCase = 1
    } else if (valueRatio >=0 && valueRatio < 0.4) {
      ratioCase = 2
    } else if (valueRatio >= 0.4 && valueRatio < 0.6) {
      ratioCase = 3
    } else if (valueRatio >= 0.6 && valueRatio < 0.8) {
      ratioCase = 4
    } else if (valueRatio >= 0.8 && valueRatio < 0.9 ) {
      ratioCase = 5
    } else if (valueRatio >= 0.9 && valueRatio <= 1.1) {
      ratioCase = 6
    } else if (valueRatio > 1.1 && valueRatio <= 1.2) {
      ratioCase = 7
    } else if (valueRatio > 1.2 && valueRatio <= 1.4) {
      ratioCase = 8
    } else if (valueRatio > 1.4 && valueRatio <= 1.6) {
      ratioCase = 9
    } else if (valueRatio > 1.6){
      ratioCase = 10
    } else {
      ratioCase = 11
    }
    ratioCase
  })


  def udfUpdateK() = udf((valueTag: Int, valueK: Double) => {
    /**
      * 根据新的K基准值和cpa_ratio来在分段函数中重新定义k值
      * t1: k * 1.2 or k
      * t2: k / 1.6
      * t3: k / 1.4
      * t4: k / 1.2
      * t5: k / 1.1
      * t6: k
      * t7: k * 1.05
      * t8: k * 1.1
      * t9: k * 1.2
      * t10: k * 1.3
      *
      * 上下限依然是0.2 到1.2
      */
    val result = valueTag match {
      case 1 if valueK >= 1.2 => valueK
      case 1 if valueK < 1.2 => valueK * 1.1
      case 2 => valueK / 2.5
      case 3 => valueK / 2.0
      case 4 => valueK / 1.8
      case 5 => valueK / 1.5
      case 6 => valueK
      case 7 => valueK * 1.1
      case 8 => valueK * 1.2
      case 9 => valueK * 1.4
      case 10 => valueK * 1.6
      case _ => valueK
    }
    result
  })

  def udfSqrt() = udf((value: Double) => {
    math.sqrt(value)
  })
}

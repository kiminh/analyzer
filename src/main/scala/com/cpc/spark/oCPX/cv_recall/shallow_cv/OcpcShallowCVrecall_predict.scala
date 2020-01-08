package com.cpc.spark.oCPX.cv_recall.shallow_cv

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object OcpcShallowCVrecall_predict {
  def main(args: Array[String]): Unit = {
    // 计算日期周期
    val date = args(0).toString
    println("parameters:")
    println(s"date=$date")

    // spark app name
    val spark = SparkSession.builder().appName(s"OcpcShallowCVrecall: $date").enableHiveSupport().getOrCreate()



  }


}
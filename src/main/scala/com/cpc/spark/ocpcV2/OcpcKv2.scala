package com.cpc.spark.ocpcV2

import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.ocpc.utils.OcpcUtils._
import org.apache.commons.math3.fitting.{PolynomialCurveFitter, WeightedObservedPoints}
import org.apache.spark.sql.{Dataset, Row, SparkSession}

import scala.collection.mutable
import org.apache.spark.sql.functions._

import com.cpc.spark.udfs.Udfs_wj._

object OcpcKv2 {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("ocpc v2").enableHiveSupport().getOrCreate()

    val date = args(0).toString
    val hour = args(1).toString

    
  }
}

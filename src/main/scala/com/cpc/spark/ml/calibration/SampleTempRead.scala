package com.cpc.spark.ml.calibration

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}
import org.apache.spark.sql.functions._
//import com.cpc.spark.ml.calibration.MultiDimensionCalibOnQtt.computeCalibration

/**
  * author: wangyao
  * date: 5/14/19
  */
object SampleTempRead {
  def main(args: Array[String]): Unit = {
    Logger.getRootLogger.setLevel(Level.WARN)

    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()
    import spark.implicits._


    val df = spark.read.format("com.databricks.spark.csv")
      .option("header", "false")
      .option("inferSchema", "false") //是否自动推到内容的类型
      .option("delimiter", "\001") //分隔符，默认为 ,
      .load("/home/cpc/wy/calibration_sample/cali_prediction-*.csv")
    df.show(10)
    val resultDF = df.select($"_c0".alias("id"), $"_c1".alias("title"))

    resultDF.write.mode("overwrite").saveAsTable("dl_cpc.wy_tensorflow_cali_result")
  }
}


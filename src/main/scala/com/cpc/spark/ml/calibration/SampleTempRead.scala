package com.cpc.spark.ml.calibration

import com.cpc.spark.common.Murmur3Hash.stringHash64
import com.cpc.spark.ml.calibration.MultiDimensionCalibOnQttCvrV3.LogToPb
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, udf}
//import com.cpc.spark.ml.calibration.MultiDimensionCalibOnQtt.computeCalibration

/**
  * author: wangyao
  * date: 5/14/19
  */
object SampleTempRead{
  def main(args: Array[String]): Unit = {
    Logger.getRootLogger.setLevel(Level.WARN)

    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()
    import spark.implicits._

    val df = spark.read.format("com.databricks.spark.csv")
      .option("header", "true")
      .option("inferSchema", "false") //是否自动推到内容的类型
      .option("delimiter","\001")  //分隔符，默认为 ,
      .load("hdfs://emr-cluster/user/cpc/wy/tensorflow_cali_result_wzjf_r5.csv")
    df.show(10)

    df.write.mode("overwrite").saveAsTable("dl_cpc.wy_tensorflow_cali_result")
  }

  def hash64(seed:Int)= udf {
    x:String =>  stringHash64(x,seed)}

}

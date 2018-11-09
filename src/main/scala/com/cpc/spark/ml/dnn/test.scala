package com.cpc.spark.ml.dnn

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  *
  * created time : 2018/11/8 14:13
  *
  * @author zhj
  * @version 1.0
  *
  */
class test(spark: SparkSession, trdate: String = "", trpath: String = "",
           tedate: String = "", tepath: String = "")
  extends DNNSample(spark, trdate, trpath, tedate, tepath) {

  override def getTrainSample(spark: SparkSession, date: String): DataFrame = super.getTrainSample(spark, date)

  override def getTestSamle(spark: SparkSession, date: String, percent: Double): DataFrame = super.getTestSamle(spark, date, percent)

  override def getTestSamle4Gauc(spark: SparkSession, date: String, percent: Double): DataFrame = super.getTestSamle4Gauc(spark, date, percent)
}

object test {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .enableHiveSupport()
      .getOrCreate()

    val Array(trdate, trpath, tedate, tepath) = args

    val test = new test(spark, trdate, trpath, tedate, tepath)
    test.saveTrain()
    test.saveTest()
  }
}

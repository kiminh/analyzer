package com.cpc.spark.user_signal.dsp

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

/**
  * author: huazhenhao
  * date: 11/29/18
  */
object ExtractDspInfo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("cpc-dsp-extract-info").enableHiveSupport().getOrCreate()
    val sql =
      """
        |SELECT
        |   uid,
        |   isshow,
        |   isclick,
        |   ext['adid_str'].string_value as adid,
        |   adsrc,
        |   ext['ad_title'].string_value as title,
        |   ext['ad_desc'].string_value as desc,
        |   ext['ad_img_urls'].string_value as image_url,
        |   ext['ad_click_url'].string_value as click_url
        |FROM dl_cpc.cpc_union_log
        |WHERE date='{0}' and ext['adid_str'].string_value != '' and adsrc > 1
        | and media_appsid in ("80000001", "80000002")
        | and ext['ad_title'].string_value != ''
        | group by ext['adid_str'].string_value, adsrc
      """.stripMargin
    val rawData = spark.sql(sql)
//    val adData = rawData.groupBy("adid", "adsrc")
  }

}

class GeometricMean extends UserDefinedAggregateFunction {
  // This is the input fields for your aggregate function.
  override def inputSchema: org.apache.spark.sql.types.StructType =
    StructType(StructField("value", StringType) :: Nil)

  // This is the internal fields you keep for computing your aggregate.
  override def bufferSchema: StructType = StructType(
    StructField("result", StringType) :: Nil)

  // This is the output type of your aggregatation function.
  override def dataType: DataType = StringType

  override def deterministic: Boolean = true

  // This is the initial value for your buffer schema.
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = ""
  }

  // This is how to update your buffer schema given an input.
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    val str = input.getAs[String](0)
    if (buffer.getAs[String](0).length < str.length) {
      buffer(0) = input.getAs[String](0)
    }
    buffer(0) = buffer.getAs[Long](0) + 1
    buffer(1) = buffer.getAs[Double](1) * input.getAs[Double](0)
  }

  // This is how to merge two objects with the bufferSchema type.
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getAs[Long](0) + buffer2.getAs[Long](0)
    buffer1(1) = buffer1.getAs[Double](1) * buffer2.getAs[Double](1)
  }

  // This is where you output the final value, given the final value of your bufferSchema.
  override def evaluate(buffer: Row): Any = {
    buffer.getAs[String](0)
  }
}

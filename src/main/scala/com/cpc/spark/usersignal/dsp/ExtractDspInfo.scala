package com.cpc.spark.usersignal.dsp

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.expr
/**
  * author: huazhenhao
  * date: 11/29/18
  */
object ExtractDspInfo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("cpc-dsp-extract-info").enableHiveSupport().getOrCreate()
    val date = args(0)
    val sql =
      s"""
        |SELECT
        |   uid,
        |   isshow,
        |   isclick,
        |   ext['adid_str'].string_value as adid,
        |   cast(adsrc as int) as adsrc,
        |   ext['ad_title'].string_value as title,
        |   ext['ad_desc'].string_value as desc,
        |   ext['ad_img_urls'].string_value as image_url,
        |   ext['ad_click_url'].string_value as click_url
        |FROM dl_cpc.cpc_union_log
        |WHERE date='$date' and ext['adid_str'].string_value != '' and adsrc > 1
        |   and media_appsid in ("80000001", "80000002")
        |   and ext['ad_title'].string_value != ''
      """.stripMargin
    val rawData = spark.sql(sql)
    spark.udf.register("slm", new StringLengthMax)

    val adData = rawData.groupBy("adid", "adsrc").agg(
      expr("slm(title) as title"), expr("slm(desc) as desc"),
      expr("slm(image_url) as image_url"), expr("slm(click_url) as click_url"))

    println(s"total dsp ad: ${adData.count()}")
    println(s"dsp ad with title: ${adData.filter(row => {
      val title = row.getAs[String]("title")
      title != null && !title.equals("")
    }).count()}")

    adData.coalesce(20).write.mode("overwrite")
      .parquet(s"/user/cpc/user_signal/dsp_raw/$date")
  }

}

class StringLengthMax extends UserDefinedAggregateFunction {
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
      buffer(0) = str
    }
  }

  // This is how to merge two objects with the bufferSchema type.
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    update(buffer1, buffer2)
  }

  // This is where you output the final value, given the final value of your bufferSchema.
  override def evaluate(buffer: Row): Any = {
    buffer.getAs[String](0)
  }
}

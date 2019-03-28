package com.cpc.spark.OcpcProtoType.model_qtt

import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.ocpc.OcpcUtils._
import com.cpc.spark.udfs.Udfs_wj._
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._


object OcpcPCOCapi {
  def main(args: Array[String]): Unit = {
    /*
    基于pcoc和jfb计算k值，此处为了避免api回传数据延迟问题，不再使用searchid进行关联
    1. 抽取click基础数据：isclick, exp_cvr, price, bid
    2. 从ml_cvr_feature_v2表中抽取iscvr的转化数据
    3. 计算pcoc与jfb
    4. 存储到k值模型表中
     */
    val spark = SparkSession.builder().appName("qtt: OcpcPCOCv1").enableHiveSupport().getOrCreate()

    // bash: 2019-01-02 12 24 1 qtt_demo qtt
    val date = args(0).toString
    val hour = args(1).toString
    val hourInt = args(2).toInt
    val version = args(3).toString
    val media = args(4).toString

    println("parameters:")
    println(s"date=$date, hour=$hour, hourInt=$hourInt, version=$version, media=$media")

    // 抽取click的基础数据
    val clickData = getClickData(media, hourInt, date, hour, spark)

    // 从ml_cvr_feature_v2表中抽取iscvr的转化数据
    val cvData = getCvData(media, hourInt, date, hour, spark)

    // 计算pcoc与jfb
    val result = calculateK(clickData, cvData, spark)

    val resultDF = result
      .select("identifier", "kvalue")
      .withColumn("conversion_goal", lit(2))
      .withColumn("date", lit(date))
      .withColumn("hour", lit(hour))
      .withColumn("version", lit(version))
      .withColumn("method", lit("api_pcoc"))

    resultDF.write.mode("overwrite").saveAsTable("test.ocpc_k_api_pcoc_hourly")
//    resultDF
//      .repartition(10).write.mode("overwrite").insertInto("dl_cpc.ocpc_k_model_hourly")


  }

  def calculateK(clickData: DataFrame, cvData: DataFrame, spark: SparkSession) = {
    val data = clickData
        .join(cvData, Seq("unitid"), "outer")
        .withColumn("jfb", col("total_price") * 1.0 / col("total_bid"))
        .select("unitid", "total_price", "click", "total_bid", "total_pcvr", "jfb", "cv")
        .withColumn("post_cvr", col("cv") * 1.0 / col("click"))
        .withColumn("pre_cvr", col("total_pcvr") * 1.0 / col("click"))
        .withColumn("pcoc", col("pre_cvr") / col("post_cvr"))

    data.show(10)
    data.write.mode("overwrite").saveAsTable("test.check_api_unitid_pcoc201909328")
    data.createOrReplaceTempView("base_data")
    val sqlRequest =
      s"""
         |SELECT
         |  cast(unitid as string) identifier,
         |  1.0 / (pcoc * jfb) as kvalue
         |FROM
         |  base_data
       """.stripMargin
    println(sqlRequest)
    val resultDF = spark.sql(sqlRequest)

    resultDF
  }

  def getCvData(media: String, hourInt: Int, date: String, hour: String, spark: SparkSession) = {
    // 媒体选择
    val conf = ConfigFactory.load("ocpc")
    val conf_key = "medias." + media + ".media_selection"
    val mediaSelection = conf.getString(conf_key)

    // 取历史数据
    val dateConverter = new SimpleDateFormat("yyyy-MM-dd HH")
    val newDate = date + " " + hour
    val today = dateConverter.parse(newDate)
    val calendar = Calendar.getInstance
    calendar.setTime(today)
    calendar.add(Calendar.HOUR, -hourInt)
    val yesterday = calendar.getTime
    val tmpDate = dateConverter.format(yesterday)
    val tmpDateValue = tmpDate.split(" ")
    val date1 = tmpDateValue(0)
    val hour1 = tmpDateValue(1)
    val selectCondition = getTimeRangeSql2(date1, hour1, date, hour)

    // 抽取转化数
    val sqlRequest =
      s"""
         |SELECT
         |  searchid,
         |  unitid,
         |  label
         |FROM
         |  dl_cpc.ml_cvr_feature_v2
         |WHERE
         |  $selectCondition
         |AND
         |  $mediaSelection
         |AND
         |  label = 1
       """.stripMargin
    println(sqlRequest)
    val data = spark.sql(sqlRequest)

    val resultDF = data
      .groupBy("unitid")
      .agg(sum(col("label")).alias("cv"))
      .select("unitid", "cv")

    resultDF.show(10)
    resultDF
  }

  def getClickData(media: String, hourInt: Int, date: String, hour: String, spark: SparkSession) = {
    /*
    抽取click基础数据：isclick, exp_cvr, price, bid
     */
    // 媒体选择
    val conf = ConfigFactory.load("ocpc")
    val conf_key = "medias." + media + ".media_selection"
    val mediaSelection = conf.getString(conf_key)

    // 取历史数据
    val dateConverter = new SimpleDateFormat("yyyy-MM-dd HH")
    val newDate = date + " " + hour
    val today = dateConverter.parse(newDate)
    val calendar = Calendar.getInstance
    calendar.setTime(today)
    calendar.add(Calendar.HOUR, -hourInt)
    val yesterday = calendar.getTime
    val tmpDate = dateConverter.format(yesterday)
    val tmpDateValue = tmpDate.split(" ")
    val date1 = tmpDateValue(0)
    val hour1 = tmpDateValue(1)
    val selectCondition = getTimeRangeSql2(date1, hour1, date, hour)

    val sqlRequest =
      s"""
         |SELECT
         |  searchid,
         |  unitid,
         |  isshow,
         |  isclick,
         |  bid as original_bid,
         |  price,
         |  exp_cvr,
         |  ocpc_log
         |FROM
         |  dl_cpc.ocpc_base_unionlog
         |WHERE
         |  $selectCondition
         |AND
         |  $mediaSelection
       """.stripMargin
    println(sqlRequest)
    val base = spark
      .sql(sqlRequest)
      .withColumn("ocpc_log_dict", udfStringToMap()(col("ocpc_log")))

    base.createOrReplaceTempView("base_table")
    val sqlRequestBase =
      s"""
         |select
         |    searchid,
         |    unitid,
         |    price,
         |    original_bid,
         |    cast(exp_cvr as double) as exp_cvr,
         |    isclick,
         |    isshow,
         |    ocpc_log,
         |    ocpc_log_dict,
         |    (case when length(ocpc_log)>0 then cast(ocpc_log_dict['dynamicbid'] as int) else original_bid end) as bid
         |from base_table
       """.stripMargin
    println(sqlRequestBase)
    val rawData = spark.sql(sqlRequestBase)
    rawData.createOrReplaceTempView("raw_table")

    // 展现数、点击数、花费
    val sqlRequest1 =
      s"""
         |SELECT
         |  unitid,
         |  SUM(case when isclick=1 then price else 0 end) as total_price,
         |  SUM(isclick) as click,
         |  SUM(case when isclick=1 then bid else 0 end) as total_bid,
         |  SUM(case when isclick=1 then exp_cvr else 0 end) as total_pcvr
         |FROM
         |  raw_table
         |GROUP BY unitid
       """.stripMargin
    println(sqlRequest1)
    val data = spark.sql(sqlRequest1)

    val resultDF = data
      .select("unitid", "total_price", "click", "total_bid", "total_pcvr")
    resultDF.show(10)

    resultDF
  }
}
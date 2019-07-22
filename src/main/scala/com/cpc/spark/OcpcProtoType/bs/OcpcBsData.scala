package com.cpc.spark.OcpcProtoType.bs

import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.OcpcProtoType.OcpcTools._
import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}


object OcpcBsData {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()
    Logger.getRootLogger.setLevel(Level.WARN)

    val date = args(0).toString
    val hour = args(1).toString
    val version = args(2).toString
    val expTag = args(3).toString
    val hourInt = args(4).toInt
    val minCV = args(5).toInt
    println("parameters:")
    println(s"date=$date, hour=$hour, hourInt:$hourInt")


    val baseData = getBaseData(hourInt, date, hour, spark)
    baseData.show(10)

    // 计算结果
    val result = calculateData(baseData, expTag, spark)
//
//    val finalVersion = version + hourInt.toString
//    val resultDF = result
//      .select("identifier", "conversion_goal", "click", "cv", "pre_cvr", "total_price", "total_bid", "hour_cnt")
//      .filter(s"cv > 0")
//
//    resultDF
  }

  def calculateData(baseData: DataFrame, expTag: String, spark: SparkSession) = {
    baseData.createOrReplaceTempView("base_data")
    val sqlRequest1 =
      s"""
         |SELECT
         |  unitid,
         |  sum(case when isclick=1 then iscvr else 0 end) as cv,
         |  sum(case when isclick=1 then iscvr else 0 end) * 1.0 / sum(isclick) as cvr,
         |  sum(isclick) * 1.0 / sum(isshow) as ctr
         |FROM
         |  base_data
         |GROUP BY unitid
       """.stripMargin
    println(sqlRequest1)
    val data1 = spark
      .sql(sqlRequest1)
      .withColumn("exp_tag", lit(expTag))
      .withColumn("key", concat_ws("&", col("exp_tag"), col("unitid")))
      .select("key", "cv", "cvr", "ctr")
      .cache()

    val sqlRequest2 =
      s"""
         |SELECT
         |  adslot_type,
         |  adtype,
         |  conversion_goal,
         |  sum(case when isclick=1 then iscvr else 0 end) as cv,
         |  sum(case when isclick=1 then iscvr else 0 end) * 1.0 / sum(isclick) as cvr,
         |  sum(isclick) * 1.0 / sum(isshow) as ctr
         |FROM
         |  base_data
         |GROUP BY adslot_type, adtype, conversion_goal
       """.stripMargin
    println(sqlRequest2)
    val data2 = spark
      .sql(sqlRequest2)
      .withColumn("exp_tag", lit(expTag))
      .withColumn("key", concat_ws("&", col("exp_tag"), col("adslot_type"), col("adtype"), col("conversion_goal")))
      .select("key", "cv", "cvr", "ctr")
      .cache()

    data1.show(10)
    data2.show(10)

    val data = data1.union(data2)

    data



  }

  def getBaseData(hourInt: Int, date: String, hour: String, spark: SparkSession) = {
    // 抽取媒体id
    val conf = ConfigFactory.load("ocpc")
    val conf_key = "medias.total.media_selection"
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
    val selectCondition = getTimeRangeSqlDate(date1, hour1, date, hour)

    val sqlRequest =
      s"""
         |SELECT
         |  searchid,
         |  unitid,
         |  conversion_goal,
         |  adslot_type,
         |  adtype,
         |  isshow,
         |  isclick,
         |  (case
         |      when media_appsid in ('80000001', '80000002') then 'qtt'
         |      when media_appsid in ('80002819') then 'hottopic'
         |      else 'novel'
         |  end) as media,
         |  cast(exp_cvr as double) as exp_cvr,
         |  cast(exp_ctr as double) as exp_ctr
         |FROM
         |  dl_cpc.ocpc_base_unionlog
         |WHERE
         |  $selectCondition
         |AND
         |  $mediaSelection
         |AND
         |  isshow = 1
         |AND
         |  is_ocpc = 1
       """.stripMargin
    println(sqlRequest)
    val clickData = spark
      .sql(sqlRequest)
      .withColumn("cvr_goal", udfConcatStringInt("cvr")(col("conversion_goal")))

    // 抽取cv数据
    val sqlRequest2 =
      s"""
         |SELECT
         |  searchid,
         |  label as iscvr,
         |  cvr_goal
         |FROM
         |  dl_cpc.ocpc_label_cvr_hourly
         |WHERE
         |  $selectCondition
       """.stripMargin
    println(sqlRequest2)
    val cvData = spark.sql(sqlRequest2)


    // 数据关联
    val resultDF = clickData
      .join(cvData, Seq("searchid", "cvr_goal"), "left_outer")
      .na.fill(0, Seq("iscvr"))

    resultDF
  }

}
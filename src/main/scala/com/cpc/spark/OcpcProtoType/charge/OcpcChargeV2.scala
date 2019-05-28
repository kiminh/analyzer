package com.cpc.spark.OcpcProtoType.charge

import java.sql.{Connection, DriverManager}
import java.text.SimpleDateFormat
import java.util.{Calendar, Properties}

import com.cpc.spark.ocpc.OcpcUtils.getTimeRangeSql2
import com.cpc.spark.udfs.Udfs_wj.udfStringToMap
import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._

object OcpcChargeV2 {
  def main(args: Array[String]): Unit = {
    /*
    根据最近七天有投放oCPC广告的广告单元各自的消费时间段的消费数据统计是否超成本和赔付数据
    允许重复赔付
     */
    Logger.getRootLogger.setLevel(Level.WARN)
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()

    // 计算日期周期
    val date = args(0).toString
    val version = args(1).toString
    val media = args(2).toString
    val dayCnt = args(3).toInt

    val clickData = getClickData(date, media, dayCnt, spark)
    val cvData = getCvData(date, dayCnt, spark)

    val data = clickData
      .join(cvData, Seq("searchid"), "left_outer")

    val payData = calculatePay(data, date, dayCnt, spark)
    payData.show(10)
  }

  def calculatePay(baseData: DataFrame, date: String, dayCnt: Int, spark: SparkSession) = {
    // 取历史数据
    val dateConverter = new SimpleDateFormat("yyyy-MM-dd")
    val today = dateConverter.parse(date)
    val calendar = Calendar.getInstance
    calendar.setTime(today)
    calendar.add(Calendar.DATE, -dayCnt)
    val yesterday = calendar.getTime
    val date1 = dateConverter.format(yesterday)
    val selectCondition = s"`date` >= '$date1'"

    baseData.createOrReplaceTempView("base_data")
    val sqlRequest1 =
      s"""
         |SELECT
         |  unitid,
         |  sum(case when isclick=1 then price else 0 end) as cost,
         |  sum(iscvr) as cv,
         |  sum(case when isclick=1 then cpagiven else 0 end) * 1.0 / sum(isclick) as cpagiven
         |FROM
         |  base_data
         |GROUP BY unitid
       """.stripMargin
    println(sqlRequest1)
    val rawData = spark.sql(sqlRequest1)
    rawData.createOrReplaceTempView("raw_data")

    val sqlRequest2 =
      s"""
         |SELECT
         |  unitid,
         |  cost - 1.2 * cv * cpagiven as pay
         |  cost,
         |  cost * 1.0 / cv as cpareal,
         |  cpagiven
         |FROM
         |  raw_data
       """.stripMargin
    println(sqlRequest2)
    val result = spark
      .sql(sqlRequest2)
      .withColumn("pay", when(col("pay") <= 0.0, 0.0).otherwise(col("pay")))
      .withColumn("start_date", lit(date1))

    result

  }

  def getCvData(date: String, dayCnt: Int, spark: SparkSession) = {
    // 取历史数据
    val dateConverter = new SimpleDateFormat("yyyy-MM-dd")
    val today = dateConverter.parse(date)
    val calendar = Calendar.getInstance
    calendar.setTime(today)
    calendar.add(Calendar.DATE, -dayCnt)
    val yesterday = calendar.getTime
    val date1 = dateConverter.format(yesterday)
    val selectCondition = s"`date` >= '$date1'"

    val sqlRequest =
      s"""
         |SELECT
         |  searchid,
         |  label as iscvr
         |FROM
         |  dl_cpc.ocpc_label_cvr_hourly
         |WHERE
         |  $selectCondition
         |AND
         |  cvr_goal = 'cvr3'
       """.stripMargin
    println(sqlRequest)
    val data = spark.sql(sqlRequest)

    data
  }

  def getClickData(date: String, media: String, dayCnt: Int, spark: SparkSession) = {
    // 取历史数据
    val dateConverter = new SimpleDateFormat("yyyy-MM-dd")
    val today = dateConverter.parse(date)
    val calendar = Calendar.getInstance
    calendar.setTime(today)
    calendar.add(Calendar.DATE, -dayCnt)
    val yesterday = calendar.getTime
    val date1 = dateConverter.format(yesterday)
    val selectCondition = s"`date` between '$date1' and '$date'"

    // 媒体选择
    val conf = ConfigFactory.load("ocpc")
    val conf_key1 = "medias." + media + ".media_selection"
    val mediaSelection = conf.getString(conf_key1)

    val sqlRequest =
      s"""
         |SELECT
         |  searchid,
         |  timestamp,
         |  unitid,
         |  userid,
         |  cast(ocpc_log_dict['conversiongoal'] as int) as conversion_goal,
         |  cast(ocpc_log_dict['cpagiven'] as double) as cpagiven,
         |  cast(ocpc_log_dict['IsHiddenOcpc'] as int) as is_hidden,
         |  isclick,
         |  price,
         |  date,
         |  hour
         |FROM
         |  dl_cpc.ocpc_filter_unionlog
         |WHERE
         |  $selectCondition
         |AND
         |  $mediaSelection
         |AND
         |  is_ocpc = 1
         |AND
         |  isclick=1
         |AND
         |  (cast(adclass as string) like "134%" or cast(adclass as string) like "107%")
       """.stripMargin
    println(sqlRequest)
    val rawData = spark
      .sql(sqlRequest)
      .filter(s"is_hidden = 0 and conversion_goal = 3")

    rawData.createOrReplaceTempView("raw_data")

    val sqlRequest2 =
      s"""
         |SELECT
         |  *,
         |  row_number() over(partition by unitid order by timestamp) as seq
         |FROM
         |  raw_data
       """.stripMargin
    println(sqlRequest2)
    val firstData = spark
      .sql(sqlRequest2)
      .filter(s"seq = 1")
      .filter(s"`date` = '$date1'")
      .withColumn("start_date", col("date"))
      .select("unitid", "start_date")

    val result = rawData
      .join(firstData, Seq("unitid"), "inner")


    result.printSchema()
    result.show(10)
    result

  }
}

package com.cpc.spark.oCPX.oCPC.pay

import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.oCPX.OcpcTools.{udfConcatStringInt, udfDetermineIndustry, udfDetermineMedia}
import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object OcpcChargeUpdate {
  def main(args: Array[String]): Unit = {
    /*
    按照七天周期计算赔付数据
    1. 计算当天所有单元的点击、消费、转化、平均cpagiven、平均cpareal、赔付金额
    2. 获取这批单元在赔付周期中的起始时间
    3. 如果当前为周期第一天，则重新落表，否则，叠加上前一天的历史数据
    4. 数据落表，需包括周期编号，是否周期第一天
     */
    Logger.getRootLogger.setLevel(Level.WARN)
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()

    // 计算日期周期
    val date = args(0).toString
    val version = args(1).toString

    // 计算当天数据
    val baseData = spark
      .table("dl_cpc.ocpc_pay_single_date_daily_v2")
      .where(s"date = '$date' and version = '$version'")
      .withColumn("current_click", col("click"))
      .withColumn("current_cv", col("cv"))
      .withColumn("current_cost", col("cost"))
      .withColumn("current_cpagiven", col("cpagiven"))
      .withColumn("current_ocpc_charge_time", col("ocpc_charge_time"))
      .select("unitid", "current_click", "current_cv", "current_cost", "current_cpagiven", "current_ocpc_charge_time")

    // 判断这批单元的赔付周期
    val scheduleData = spark
      .table("dl_cpc.ocpc_pay_cnt_daily_v2")
      .where(s"date = '$date' and version = '$version'")


    // 如果当前为周期第一天，则重新落表，否则，叠加上前一天的历史数
    // 获取前一天数据
    val prevData = getPrevData(date, version, spark)
    // 根据date是否等于pay_date来判断是否重新落表
    val finalPayData = updatePay(prevData, scheduleData, baseData, date, spark)
//    finalPayData
//      .repartition(1)
//      .write.mode("overwrite").saveAsTable("test.check_ocpc_pay_data20191014b")

    // 数据落表
    val resultDF = finalPayData
      .withColumn("ocpc_charge_time", udfSetOcpcChargeTime(date + " 00:00:00")(col("pay_cnt"), col("ocpc_charge_time")))
      .withColumn("pay", udfCalculatePay()(col("cv"), col("cost"), col("cpagiven")))
      .select("unitid", "click", "cv", "cost", "cpagiven", "pay", "ocpc_charge_time", "pay_cnt", "pay_date", "restart_flag")
      .withColumn("date", lit(date))
      .withColumn("version", lit(version))

    resultDF
      .repartition(1)
//      .write.mode("overwrite").insertInto("test.ocpc_pay_data_daily_v2")
      .write.mode("overwrite").insertInto("dl_cpc.ocpc_pay_data_daily_v2")

  }

  def udfCalculatePay() = udf((cv: Long, cost: Double, cpagiven: Double) => {
    var result = cost - 1.2 * cv.toDouble * cpagiven
    if (result <= 0) {
      result = 0
    }
    result
  })

  def updatePay(prevDataRaw: DataFrame, scheduleData: DataFrame, baseDataRaw: DataFrame, date: String, spark: SparkSession) = {
    val flagData = scheduleData
      .withColumn("restart_flag", when(col("pay_date") === lit(date), 1).otherwise(0))

//    flagData
//      .repartition(1)
//      .write.mode("overwrite").saveAsTable("test.check_ocpc_pay_data20191014a")



    val rawData = flagData
      .filter("flag = 1")
      .select("unitid", "pay_date", "pay_cnt", "restart_flag")
      .distinct()
      .join(prevDataRaw, Seq("unitid"), "left_outer")
      .na.fill(0.0, Seq("prev_click", "prev_cv", "prev_cost", "prev_cpagiven"))
      .na.fill(date + " 00:00:00", Seq("prev_ocpc_charge_time"))
      .select("unitid", "pay_cnt", "pay_date", "prev_click", "prev_cv", "prev_cost", "prev_cpagiven", "prev_ocpc_charge_time", "restart_flag")
      .join(baseDataRaw, Seq("unitid"), "left_outer")
      .na.fill(0.0, Seq("current_click", "current_cv", "current_cost", "current_cpagiven"))
      .na.fill(date + " 00:00:00", Seq("current_ocpc_charge_time"))
      .select("unitid", "pay_cnt", "pay_date", "prev_click", "prev_cv", "prev_cost", "prev_cpagiven", "prev_ocpc_charge_time", "restart_flag", "current_click", "current_cv", "current_cost", "current_cpagiven", "current_ocpc_charge_time")

    rawData.createOrReplaceTempView("raw_data")

    val sqlRequest =
      s"""
         |SELECT
         |  *,
         |  (case when restart_flag = 1 then current_click
         |        else prev_click + current_click end) as click,
         |  (case when restart_flag = 1 then current_cv
         |        else prev_cv + current_cv end) as cv,
         |  (case when restart_flag = 1 then current_cost
         |        else prev_cost + current_cost end) as cost,
         |  (case when restart_flag = 1 then current_cpagiven
         |        else (prev_cpagiven * prev_click + current_cpagiven * current_click) * 1.0 / (prev_click + current_click) end) as cpagiven,
         |  (case when restart_flag = 1 then current_ocpc_charge_time
         |        else prev_ocpc_charge_time end) as ocpc_charge_time
         |FROM
         |  raw_data
         |""".stripMargin
    println(sqlRequest)
    val data = spark
      .sql(sqlRequest)
      .na.fill(0, Seq("cv", "click", "cost", "cpagiven"))

    data
  }

  def getPrevData(date: String, version: String, spark: SparkSession) = {
    // 取历史数据
    val dateConverter = new SimpleDateFormat("yyyy-MM-dd")
    val today = dateConverter.parse(date)
    val calendar = Calendar.getInstance
    calendar.setTime(today)
    calendar.add(Calendar.DATE, -1)
    val yesterday = calendar.getTime
    val date1 = dateConverter.format(yesterday)

    val sqlRequest =
      s"""
         |SELECT
         |  unitid,
         |  click as prev_click,
         |  cv as prev_cv,
         |  cost as prev_cost,
         |  cpagiven as prev_cpagiven,
         |  ocpc_charge_time as prev_ocpc_charge_time
         |FROM
         |  test.ocpc_pay_data_daily_v2
         |WHERE
         |  `date` = '$date1'
         |AND
         |  version = '$version'
         |""".stripMargin
    println(sqlRequest)
    val result = spark.sql(sqlRequest)

    result
  }

  def udfSetOcpcChargeTime(ocpcChargeDate: String) = udf((prevPayCnt: Int, ocpcChargeTime: String) => {
    val result = prevPayCnt match {
      case 1 => ocpcChargeTime
      case _ => ocpcChargeDate
    }
    result
  })



}

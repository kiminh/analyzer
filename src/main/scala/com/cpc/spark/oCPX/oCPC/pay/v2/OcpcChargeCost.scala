package com.cpc.spark.oCPX.oCPC.pay.v2

import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.oCPX.OcpcTools.udfConcatStringInt
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object OcpcChargeCost {
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
    val dayCnt = args(1).toInt

    // 计算七天的分天展点消以及浅层转化
    val shallowOcpcData = getShallowData(date, dayCnt, spark)

    // 计算七天的分天展点消以及深层转化
    val deepOcpcData = getDeepData(date, dayCnt, spark)

    // 数据关联
    val data = assemblyData(shallowOcpcData, deepOcpcData, spark)

  }

  def assemblyData(dataRaw1: DataFrame, dataRaw2: DataFrame, session: SparkSession) = {
    val data1 = dataRaw1
      .withColumn("click1", col("click"))
      .withColumn("cv1", col("cv"))
      .withColumn("cost1", col("cost"))
      .withColumn("cpagiven1", col("cpagiven"))
      .select("unitid", "date", "flag", "click1", "cv1", "cost1", "cpagiven1")

    val data2 = dataRaw2
      .withColumn("click2", col("click"))
      .withColumn("cv2", col("cv"))
      .withColumn("cost2", col("cost"))
      .withColumn("cpagiven2", col("cpagiven"))
      .select("unitid", "date", "cpa_check_priority", "flag", "click2", "cv2", "cost2", "cpagiven2")
      .filter(s"flag = 1")

    val data = data1
      .join(data2, Seq("unitid", "date", "flag"), "left_outer")
      .na.fill(0, Seq("cpa_check_priority", "click2", "cv2", "cost2", "cpagiven2"))

    data
  }

  def getDeepData(date: String, dayCnt: Int, spark: SparkSession) = {
    // 取历史数据
    val dateConverter = new SimpleDateFormat("yyyy-MM-dd")
    val today = dateConverter.parse(date)
    val calendar = Calendar.getInstance
    calendar.setTime(today)
    calendar.add(Calendar.DATE, -dayCnt)
    val yesterday = calendar.getTime
    val date1 = dateConverter.format(yesterday)
    val selectCondition = s"`date` between '$date1' and '$date'"

    val sqlRequest1 =
      s"""
         |SELECT
         |  searchid,
         |  unitid,
         |  deep_conversion_goal,
         |  isshow,
         |  isclick,
         |  price,
         |  cast(deep_cpa as double) as cpagiven,
         |  deep_ocpc_step,
         |  cpa_check_priority,
         |  is_deep_ocpc,
         |  (case when is_deep_ocpc = 1 and cpa_check_priority in (2, 3) and deep_ocpc_step = 2 then 1
         |        else 0
         |   end) as flag,
         |   date
         |FROM
         |  dl_cpc.ocpc_filter_unionlog
         |WHERE
         |  $selectCondition
         |AND
         |  cpa_check_priority in (2, 3)
         |AND
         |  is_deep_ocpc = 1
         |""".stripMargin
    println(sqlRequest1)
    val clickData = spark
      .sql(sqlRequest1)

    // 抽取cv数据
    val sqlRequest2 =
      s"""
         |SELECT
         |  searchid,
         |  label as iscvr,
         |  deep_conversion_goal
         |FROM
         |  dl_cpc.ocpc_label_deep_cvr_hourly
         |WHERE
         |  `date` >= '$date'
       """.stripMargin
    println(sqlRequest2)
    val cvData = spark.sql(sqlRequest2).distinct()

    val baseData = clickData
      .join(cvData, Seq("searchid", "deep_conversion_goal"), "left_outer")
      .na.fill(0, Seq("iscvr"))

    baseData.createOrReplaceTempView("base_data")
    val sqlRequest3 =
      s"""
         |SELECT
         |  unitid,
         |  date,
         |  cpa_check_priority,
         |  flag,
         |  sum(isclick) as click,
         |  sum(iscvr) as cv,
         |  sum(case when isclick=1 then price else 0 end) * 1.0 as cost,
         |  sum(case when isclick=1 then cpagiven else 0 end) * 1.0 / sum(isclick) as cpagiven
         |FROM
         |  base_data
         |GROUP BY unitid, date, cpa_check_priority, flag
         |""".stripMargin
    println(sqlRequest3)
    val data = spark.sql(sqlRequest3)

    data
  }

  def getShallowData(date: String, dayCnt: Int, spark: SparkSession) = {
    // 取历史数据
    val dateConverter = new SimpleDateFormat("yyyy-MM-dd")
    val today = dateConverter.parse(date)
    val calendar = Calendar.getInstance
    calendar.setTime(today)
    calendar.add(Calendar.DATE, -dayCnt)
    val yesterday = calendar.getTime
    val date1 = dateConverter.format(yesterday)
    val selectCondition = s"`date` between '$date1' and '$date'"

    val sqlRequest1 =
      s"""
         |SELECT
         |  searchid,
         |  unitid,
         |  conversion_goal,
         |  isshow,
         |  isclick,
         |  price,
         |  cast(ocpc_log_dict['cpagiven'] as double) as cpagiven,
         |  deep_ocpc_step,
         |  cpa_check_priority,
         |  is_deep_ocpc,
         |  (case when is_deep_ocpc = 1 and cpa_check_priority in (2, 3) and deep_ocpc_step = 2 then 1
         |        else 0
         |   end) as flag,
         |   date
         |FROM
         |  dl_cpc.ocpc_filter_unionlog
         |WHERE
         |  $selectCondition
         |""".stripMargin
    println(sqlRequest1)
    val clickData = spark
      .sql(sqlRequest1)
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
         |  `date` >= '$date'
       """.stripMargin
    println(sqlRequest2)
    val cvData = spark.sql(sqlRequest2).distinct()

    val baseData = clickData
      .join(cvData, Seq("searchid", "cvr_goal"), "left_outer")
      .na.fill(0, Seq("iscvr"))


    baseData.createOrReplaceTempView("base_data")
    val sqlRequest3 =
      s"""
         |SELECT
         |  unitid,
         |  date,
         |  flag,
         |  sum(isclick) as click,
         |  sum(iscvr) as cv,
         |  sum(case when isclick=1 then price else 0 end) * 1.0 as cost,
         |  sum(case when isclick=1 then cpagiven else 0 end) * 1.0 / sum(isclick) as cpagiven
         |FROM
         |  base_data
         |GROUP BY unitid, date, flag
         |""".stripMargin
    println(sqlRequest3)
    val data = spark.sql(sqlRequest3)

    data
  }


}

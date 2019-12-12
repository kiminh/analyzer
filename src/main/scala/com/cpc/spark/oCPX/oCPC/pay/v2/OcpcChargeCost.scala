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

    // 抽取周期数据表
    val scheduleData = getSchedule(date, spark)

    // 统计消费与赔付
    val payDataRaw = calculatePayRaw(data, scheduleData, date, spark)

    // 按照深度ocpc赔付的逻辑进行数据调整
    val payData = calculateFinalPay(payDataRaw, spark)

  }

  def calculateFinalPay(dataRaw: DataFrame, spark: SparkSession) = {
    /*
    按照深度ocpc的赔付逻辑调整数据
    1. 对flag=1的部分，分别汇总结算每个单元浅层和深层的赔付情况，如果cpa_check_priority = 2， 则保留深度赔付数据，如果cpa_check_priority = 3， 则保留赔付金额较大的数据，需要记录保留的是浅层还是深层
    2. 对flag=0的部分，分别汇总结算每个单元的浅层赔付情况，需要记录保留的是浅层
    3. 数据union
     */
    dataRaw.createOrReplaceTempView("raw_data")
    val sqlRequest1 =
      s"""
         |SELECT
         |  unitid,
         |  cpa_check_priority,
         |  flag,
         |  last_ocpc_charge_time,
         |  last_deep_ocpc_charge_time,
         |  click1,
         |  cv1,
         |  cpagiven1,
         |  cost1,
         |  cost1 / cv1 as cpareal1,
         |  (case when cv1 = 0 then cost1
         |        when cv1 > 0 and cost1 > 1.2 * cv1 * cpagiven1 then cost1 - 1.2 * cv1 * cpagiven1
         |        else 0
         |  end) as pay1,
         |  click2,
         |  cv2,
         |  cpagiven2,
         |  cost2,
         |  cost2 / cv2 as cpareal2,
         |  (case when cv2 = 0 then cost2
         |        when cv2 > 0 and cost2 > 1.2 * cv2 * cpagiven2 then cost2 - 1.2 * cv2 * cpagiven2
         |        else 0
         |  end) as pay2
         |FROM
         |  raw_data
         |""".stripMargin
    println(sqlRequest1)
    val baseData = spark.sql(sqlRequest1)

    // 对flag=1的部分，分别汇总结算每个单元浅层和深层的赔付情况，如果cpa_check_priority = 2， 则保留深度赔付数据，如果cpa_check_priority = 3， 则保留赔付金额较大的数据，需要记录保留的是浅层还是深层
    val data1 = baseData
      .filter(s"flag = 1")
      .withColumn("pay_type", udfDeterminePayType()(col("cpa_check_priority"), col("pay1"), col("pay2")))
      .withColumn("click", when(col("pay_type") === 1, col("click2")).otherwise(col("click1")))
      .withColumn("cv", when(col("pay_type") === 1, col("cv2")).otherwise(col("cv1")))
      .withColumn("cpagiven", when(col("pay_type") === 1, col("cpagiven2")).otherwise(col("cpagiven1")))
      .withColumn("cpareal", when(col("pay_type") === 1, col("cpareal2")).otherwise(col("cpareal1")))
      .withColumn("cost", when(col("pay_type") === 1, col("cost2")).otherwise(col("cost1")))
      .withColumn("pay", when(col("pay_type") === 1, col("pay2")).otherwise(col("pay1")))

    // 对flag=0的部分，分别汇总结算每个单元的浅层赔付情况，需要记录保留的是浅层
    val data2 = baseData
      .filter(s"flag = 0")
      .withColumn("click", col("click1"))
      .withColumn("cv", col("cv1"))
      .withColumn("cpagiven", col("cpagiven1"))
      .withColumn("cpareal", col("cpareal1"))
      .withColumn("cost", col("cost1"))
      .withColumn("pay", col("pay1"))


  }

  def udfDeterminePayType() = udf((cpaCheckPriority: Int, pay1: Double, pay2: Double) => {
    val result = {
      if (cpaCheckPriority == 2) {
        1
      } else {
        if (pay1 >= pay2) {
          0
        } else {
          1
        }
      }
    }
    result
  })

  def calculatePayRaw(dataRaw: DataFrame, scheduleDataRaw: DataFrame, date: String, spark: SparkSession) = {
    val costData = dataRaw
      .withColumn("date_dist", udfCalculateDateDist(date)(col("date")))
      .select("unitid", "date", "flag", "click1", "cv1", "cost1", "cpagiven1", "cpa_check_priority", "click2", "cv2", "cost2", "cpagiven2", "date_dist")
      .na.fill(0, Seq("cv1", "cv2"))

    val schedulData = scheduleDataRaw
      .select("unitid", "calc_dates", "last_ocpc_charge_time", "last_deep_ocpc_charge_time")

    val data = costData
      .join(schedulData, Seq("unitid"), "inner")
      .select("unitid", "date", "flag", "click1", "cv1", "cost1", "cpagiven1", "cpa_check_priority", "click2", "cv2", "cost2", "cpagiven2", "date_dist", "calc_dates", "last_ocpc_charge_time", "last_deep_ocpc_charge_time")
      .withColumn("is_in_schedule", when(col("date_dist") <= col("calc_dates"), 1).otherwise(0))

    data.createOrReplaceTempView("data")

    val sqlRequest =
      s"""
         |SELECT
         |  unitid,
         |  cpa_check_priority,
         |  flag,
         |  sum(click1) as click1,
         |  sum(cv1) as cv1,
         |  sum(cpagiven1 * click1) * 1.0 / sum(click1) as cpagiven1,
         |  sum(cost1) as cost1,
         |  sum(click2) as click2,
         |  sum(cv2) as cv2,
         |  sum(cpagiven2 * click2) * 1.0 / sum(click2) as cpagiven2,
         |  sum(cost2) as cost2
         |FROM
         |  data
         |WHERE
         |  is_in_schedule = 1
         |GROUP BY unitid, cpa_check_priority, flag
         |""".stripMargin
    println(sqlRequest)
    val result = spark
      .sql(sqlRequest)
      .join(schedulData, Seq("unitid"), "inner")
      .select("unitid", "cpa_check_priority", "flag", "click1", "cv1", "cost1", "cpagiven1", "click2", "cv2", "cost2", "cpagiven2", "last_ocpc_charge_time", "last_deep_ocpc_charge_time")

    result
  }

  def udfCalculateDateDist(date: String) = udf((currentDate: String) => {
    // 取历史数据
    val dateConverter = new SimpleDateFormat("yyyy-MM-dd")

    val today = dateConverter.parse(date)
    val ocpcChargeDate = dateConverter.parse(currentDate.split(" ")(0))
    val result = (today.getTime() - ocpcChargeDate.getTime()) / (1000 * 60 * 60 * 24) + 1
    result
  })

  def getSchedule(date: String, spark: SparkSession) = {
    val sqlRequest =
      s"""
         |SELECT
         |  *
         |FROM
         |  test.ocpc_check_exp_data20191211b
         |WHERE
         |  pay_flag = 1
         |""".stripMargin
    println(sqlRequest)
    val data = spark.sql(sqlRequest)
    data
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

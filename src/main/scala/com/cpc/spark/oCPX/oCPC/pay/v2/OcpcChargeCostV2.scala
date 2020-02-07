package com.cpc.spark.oCPX.oCPC.pay.v2

import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.oCPX.OcpcTools.udfConcatStringInt
import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object OcpcChargeCostV2 {
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
    val dayCnt = args(2).toInt
    var envSuffix = ""
    if (args.size == 4) {
      envSuffix = args(3).toString
    }

    // 计算七天的分天展点消以及浅层转化
    val shallowOcpcData = getShallowData(date, dayCnt, spark)

    // 计算七天的分天展点消以及深层转化
    val deepOcpcData = getDeepData(date, dayCnt, spark)

    // 数据关联
    val data = assemblyData(shallowOcpcData, deepOcpcData, spark)

    // 抽取周期数据表
    val scheduleData = getSchedule(date, version, spark)

    // 统计消费与赔付
    val payDataRaw = calculatePayRaw(data, scheduleData, date, spark)

    // 按照深度ocpc赔付的逻辑进行数据调整
    val payData = calculateFinalPay(payDataRaw, spark)

//    payData
//      .write.mode("overwrite").saveAsTable("test.ocpc_check_exp_data20191216b")

    val finalVersion = version + envSuffix
    println(s"finalVersion = $finalVersion")

    val resultDF = payData
      .select("unitid", "deep_ocpc_step", "cpa_check_priority", "click", "cv", "cost", "cpagiven", "cpareal", "pay", "ocpc_charge_time", "deep_ocpc_charge_time", "pay_cnt", "is_pay_flag", "is_deep_pay_flag", "pay_type")
      .withColumn("date", lit(date))
      .withColumn("version", lit(finalVersion))

    resultDF
      .repartition(1)
      .write.mode("overwrite").saveAsTable("test.ocpc_compensate_result_daily20200207c")
//      .write.mode("overwrite").insertInto("test.ocpc_compensate_result_daily")
//      .write.mode("overwrite").insertInto("dl_cpc.ocpc_compensate_result_daily")

  }

  def calculateFinalPay(dataRaw: DataFrame, spark: SparkSession) = {
    /*
    按照deep_ocpc_step来区分消费部分：deep_ocpc_step = 1的为浅层消费，等于2为深层消费

    对于浅层消费: 正常计算数据

    对于深层消费：
    1. 如果cpa_check_priority为1，使用浅层消费，为2使用深层消费，为3使用赔付金额更大的消费类型

    对于ocpc_charge_time和deep_ocpc_charge_time的使用:
    1. 如果is_pay_flag为0，ocpc_charge_time为空，否则使用ocpc_charge_time
    2. 如果is_deep_pay_flag为0，则deep_ocpc_charge_time为空，否则使用deep_ocpc_charge_time
     */
    dataRaw.createOrReplaceTempView("raw_data")
    val sqlRequest1 =
      s"""
         |SELECT
         |  unitid,
         |  deep_ocpc_step,
         |  cpa_check_priority,
         |  ocpc_charge_time,
         |  deep_ocpc_charge_time,
         |  date_diff,
         |  pay_cnt,
         |  is_pay_flag,
         |  is_deep_pay_flag,
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
         |  end) as pay2,
         |  (case when cpa_check_priority = 0 and is_deep_pay_flag = 1 then 1 else 0 end) as is_filter
         |FROM
         |  raw_data
         |""".stripMargin
    println(sqlRequest1)
    val baseData = spark.sql(sqlRequest1)

    // 对于浅层消费: 正常计算数据, deep_ocpc_step = 1
    val data1 = baseData
      .filter(s"deep_ocpc_step != 2")
      .withColumn("click", col("click1"))
      .withColumn("cv", col("cv1"))
      .withColumn("cpagiven", col("cpagiven1"))
      .withColumn("cpareal", col("cpareal1"))
      .withColumn("cost", col("cost1"))
      .withColumn("pay", col("pay1"))
      .withColumn("pay_type", lit(0))

    // 对于深层消费：
    // 如果cpa_check_priority为1，使用浅层消费，为2使用深层消费，为3使用赔付金额更大的消费类型
    val data2 = baseData
      .filter(s"deep_ocpc_step = 2")
      .withColumn("pay_type", udfDeterminePayType()(col("cpa_check_priority"), col("pay1"), col("pay2")))
      .withColumn("click", when(col("pay_type") === 1, col("click2")).otherwise(col("click1")))
      .withColumn("cv", when(col("pay_type") === 1, col("cv2")).otherwise(col("cv1")))
      .withColumn("cpagiven", when(col("pay_type") === 1, col("cpagiven2")).otherwise(col("cpagiven1")))
      .withColumn("cpareal", when(col("pay_type") === 1, col("cpareal2")).otherwise(col("cpareal1")))
      .withColumn("cost", when(col("pay_type") === 1, col("cost2")).otherwise(col("cost1")))
      .withColumn("pay", when(col("pay_type") === 1, col("pay2")).otherwise(col("pay1")))

    val result1 = data1
      .filter(s"is_filter = 0")
      .select("unitid", "deep_ocpc_step", "cpa_check_priority", "click", "cv", "cost", "cpagiven", "cpareal", "pay", "ocpc_charge_time", "deep_ocpc_charge_time", "pay_cnt", "is_pay_flag", "is_deep_pay_flag", "pay_type")

    val result2 = data2
      .select("unitid", "deep_ocpc_step", "cpa_check_priority", "click", "cv", "cost", "cpagiven", "cpareal", "pay", "ocpc_charge_time", "deep_ocpc_charge_time", "pay_cnt", "is_pay_flag", "is_deep_pay_flag", "pay_type")

    val result = result1.union(result2)

    result
  }


  def udfDeterminePayType() = udf((cpaCheckPriority: Int, pay1: Double, pay2: Double) => {
    val result = cpaCheckPriority match {
      case 1 => 0
      case 2 => 1
      case 3 => {
        if (pay1 >= pay2) {
          0
        } else {
          1
        }
      }
      case _ => 0
    }
    result
  })

  def calculatePayRaw(dataRaw: DataFrame, scheduleDataRaw: DataFrame, date: String, spark: SparkSession) = {
    val costData = dataRaw
      .withColumn("date_dist", udfCalculateDateDist(date)(col("date")))
      .select("unitid", "date", "deep_ocpc_step", "cpa_check_priority", "click1", "cv1", "cost1", "cpagiven1", "click2", "cv2", "cost2", "cpagiven2", "date_dist")
      .na.fill(0, Seq("cv1", "cv2"))

    val schedulData = scheduleDataRaw
      .select("unitid", "calc_dates", "date_diff", "pay_cnt", "ocpc_charge_time", "deep_ocpc_charge_time", "is_pay_flag", "is_deep_pay_flag")

    val data = costData
      .join(schedulData, Seq("unitid"), "inner")
      .select("unitid", "date", "deep_ocpc_step", "cpa_check_priority", "click1", "cv1", "cost1", "cpagiven1", "cpa_check_priority", "click2", "cv2", "cost2", "cpagiven2", "date_dist", "calc_dates", "ocpc_charge_time", "deep_ocpc_charge_time")
      .withColumn("is_in_schedule", when(col("date_dist") <= col("calc_dates"), 1).otherwise(0))

    data.createOrReplaceTempView("data")

    val sqlRequest =
      s"""
         |SELECT
         |  unitid,
         |  deep_ocpc_step,
         |  cpa_check_priority,
         |  sum(click1) as click1,
         |  sum(cv1) as cv1,
         |  sum(cpagiven1 * cv1) * 1.0 / sum(cv1) as cpagiven1,
         |  sum(cost1) as cost1,
         |  sum(click2) as click2,
         |  sum(cv2) as cv2,
         |  sum(cpagiven2 * cv2) * 1.0 / sum(cv2) as cpagiven2,
         |  sum(cost2) as cost2
         |FROM
         |  data
         |WHERE
         |  is_in_schedule = 1
         |GROUP BY unitid, deep_ocpc_step, cpa_check_priority
         |""".stripMargin
    println(sqlRequest)
    val result = spark
      .sql(sqlRequest)
      .join(schedulData, Seq("unitid"), "inner")
      .select("unitid", "deep_ocpc_step", "cpa_check_priority", "click1", "cv1", "cost1", "cpagiven1", "click2", "cv2", "cost2", "cpagiven2", "ocpc_charge_time", "deep_ocpc_charge_time", "date_diff", "pay_cnt", "is_pay_flag", "is_deep_pay_flag")

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

  def getSchedule(date: String, version: String, spark: SparkSession) = {
    val sqlRequest =
      s"""
         |SELECT
         |  *
         |FROM
         |  dl_cpc.ocpc_compensate_schedule_daily
         |WHERE
         |  `date` = '$date'
         |AND
         |  version = '$version'
         |""".stripMargin
    println(sqlRequest)
    val data = spark.sql(sqlRequest).filter(s"is_pay_flag = 1 OR is_deep_pay_flag = 1")
    data
  }

  def assemblyData(dataRaw1: DataFrame, dataRaw2: DataFrame, session: SparkSession) = {
    val data1 = dataRaw1
      .withColumn("click1", col("click"))
      .withColumn("cv1", col("cv"))
      .withColumn("cost1", col("cost"))
      .withColumn("cpagiven1", col("cpagiven"))
      .select("unitid", "date", "deep_ocpc_step", "cpa_check_priority", "click1", "cv1", "cost1", "cpagiven1")

    val data2 = dataRaw2
      .withColumn("click2", col("click"))
      .withColumn("cv2", col("cv"))
      .withColumn("cost2", col("cost"))
      .withColumn("cpagiven2", col("cpagiven"))
      .select("unitid", "date", "deep_ocpc_step", "cpa_check_priority", "click2", "cv2", "cost2", "cpagiven2")

    val data = data1
      .join(data2, Seq("unitid", "date", "deep_ocpc_step", "cpa_check_priority"), "left_outer")
      .na.fill(0, Seq("click2", "cv2", "cost2", "cpagiven2"))

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
         |  (case when date >= '2019-12-09' and deep_ocpc_step=2 then 2 else 1 end) as deep_ocpc_step,
         |  is_deep_ocpc,
         |  date
         |FROM
         |  dl_cpc.ocpc_filter_unionlog
         |WHERE
         |  $selectCondition
         |AND is_deep_ocpc = 1
         |AND isshow = 1
         |AND conversion_goal > 0
         |AND is_antou_deep_ocpc != 1
         |""".stripMargin
    println(sqlRequest1)
    val clickDataRaw = spark
      .sql(sqlRequest1)
      .na.fill(1, Seq("deep_ocpc_step"))
      .filter(s"deep_ocpc_step = 2")

    val cpaCheckPriority = getCPAcheckPriority(spark)
    val clickData = clickDataRaw
      .join(cpaCheckPriority, Seq("unitid"), "inner")

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
         |  `date` >= '$date1'
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
         |  deep_ocpc_step,
         |  cpa_check_priority,
         |  sum(isclick) as click,
         |  sum(iscvr) as cv,
         |  sum(case when isclick=1 then price else 0 end) * 1.0 as cost,
         |  sum(case when iscvr=1 then cpagiven else 0 end) * 1.0 / sum(iscvr) as cpagiven
         |FROM
         |  base_data
         |GROUP BY unitid, date, deep_ocpc_step, cpa_check_priority
         |""".stripMargin
    println(sqlRequest3)
    val data = spark
        .sql(sqlRequest3)
        .filter(s"deep_ocpc_step = 2")

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
         |  (case when date >= '2019-12-09' and deep_ocpc_step=2 then 2 else 1 end) as deep_ocpc_step,
         |  is_deep_ocpc,
         |  date
         |FROM
         |  dl_cpc.ocpc_filter_unionlog
         |WHERE
         |  $selectCondition
         |AND isshow = 1
         |AND conversion_goal > 0
         |""".stripMargin
    println(sqlRequest1)
    val clickDataRaw = spark
      .sql(sqlRequest1)
      .withColumn("cvr_goal", udfConcatStringInt("cvr")(col("conversion_goal")))
      .na.fill(1, Seq("deep_ocpc_step"))

    val cpaCheckPriority = getCPAcheckPriority(spark)
    val clickData = clickDataRaw
      .join(cpaCheckPriority, Seq("unitid"), "left_outer")
      .na.fill(0, Seq("cpa_check_priority"))

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
         |  `date` >= '$date1'
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
         |  deep_ocpc_step,
         |  cpa_check_priority,
         |  sum(isclick) as click,
         |  sum(iscvr) as cv,
         |  sum(case when isclick=1 then price else 0 end) * 1.0 as cost,
         |  sum(case when iscvr=1 then cpagiven else 0 end) * 1.0 / sum(iscvr) as cpagiven
         |FROM
         |  base_data
         |GROUP BY unitid, date, deep_ocpc_step, cpa_check_priority
         |""".stripMargin
    println(sqlRequest3)
    val data = spark.sql(sqlRequest3)

    data
  }

  def getCPAcheckPriority(spark: SparkSession) = {
    val conf = ConfigFactory.load("ocpc")

    val url = conf.getString("adv_read_mysql.new_deploy.url")
    val user = conf.getString("adv_read_mysql.new_deploy.user")
    val passwd = conf.getString("adv_read_mysql.new_deploy.password")
    val driver = conf.getString("adv_read_mysql.new_deploy.driver")
    val table =
      s"""
         |(SELECT unit_id as unitid, cpa_check_priority
         |FROM unit_ocpc
         |WHERE is_deep_ocpc = 1) as tmp
         |""".stripMargin

    val data = spark.read.format("jdbc")
      .option("url", url)
      .option("driver", driver)
      .option("user", user)
      .option("password", passwd)
      .option("dbtable", table)
      .load()
      .cache()

    val resultDF = data
      .selectExpr("cast(unitid as int) unitid",  "cast(cpa_check_priority as int) as cpa_check_priority")
      .distinct()
      .cache()

    resultDF.show(10)
    resultDF
  }


}

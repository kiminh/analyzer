package com.cpc.spark.oCPX.oCPC.pay.v2

import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.oCPX.OcpcTools.udfConcatStringInt
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
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
    val version = args(1).toString
    val dayCnt = args(2).toInt

    // 计算当天数据
    val baseData = getBaseData(date, dayCnt, spark)
    val resultDF = baseData
      .withColumn("date", lit(date))
      .withColumn("version", lit(version))

    resultDF
      .repartition(10)
//      .write.mode("overwrite").insertInto("test.ocpc_pay_single_date_daily_v2")
      .write.mode("overwrite").insertInto("dl_cpc.ocpc_pay_single_date_daily_v2")


  }

  def getBaseData(date: String, dayCnt: Int, spark: SparkSession) = {
    // 取历史数据
    val dateConverter = new SimpleDateFormat("yyyy-MM-dd")
    val today = dateConverter.parse(date)
    val calendar = Calendar.getInstance
    calendar.setTime(today)
    calendar.add(Calendar.DATE, -dayCnt)
    val yesterday = calendar.getTime
    val date1 = dateConverter.format(yesterday)

    // 获取基础数据
    val sqlRequest1 =
      s"""
         |SELECT
         |  searchid,
         |  timestamp,
         |  unitid,
         |  userid,
         |  conversion_goal,
         |  isclick,
         |  price,
         |  bid_ocpc as cpagiven,
         |  0 as is_hidden
         |FROM
         |  dl_cpc.cpc_basedata_union_events
         |WHERE
         |  `day` = '$date'
         |AND
         |  is_ocpc = 1
         |AND
         |  isclick = 1
         |AND
         |  ocpc_status = 2
       """.stripMargin
    println(sqlRequest1)
    val clickData = spark
      .sql(sqlRequest1)
      .withColumn("cvr_goal", udfConcatStringInt("cvr")(col("conversion_goal")))
      .filter(s"is_hidden = 0")

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

    // 数据关联
    val result = clickData
      .join(cvData, Seq("searchid", "cvr_goal"), "left_outer")
      .na.fill(0, Seq("iscvr"))
    result.createOrReplaceTempView("base_table")

    // 计算cost和cpa
    val sqlRequest3 =
      s"""
         |SELECT
         |  unitid,
         |  sum(isclick) as click,
         |  sum(iscvr) as cv,
         |  sum(case when isclick=1 then price else 0 end) * 1.0 as cost,
         |  sum(case when isclick=1 then cpagiven else 0 end) * 1.0 / sum(isclick) as cpagiven
         |FROM
         |  base_table
         |GROUP BY unitid
         |""".stripMargin
    println(sqlRequest3)
    val costData = spark.sql(sqlRequest3)

    // 计算ocpc_charge_time
    val sqlRequest4 =
      s"""
         |SELECT
         |  searchid,
         |  unitid,
         |  timestamp,
         |  from_unixtime(timestamp,'YYYY-MM-dd HH:mm:ss') as ocpc_charge_time,
         |  row_number() over(partition by unitid order by timestamp) as seq
         |FROM
         |  base_table
       """.stripMargin
    println(sqlRequest4)
    val ocpcChargeData = spark
      .sql(sqlRequest4)
      .filter(s"seq = 1")
      .select("unitid", "ocpc_charge_time")
      .distinct()

    val resultDF = costData
        .join(ocpcChargeData, Seq("unitid"), "left_outer")
        .select("unitid", "click", "cv", "cost", "cpagiven", "ocpc_charge_time")

    resultDF
  }



}

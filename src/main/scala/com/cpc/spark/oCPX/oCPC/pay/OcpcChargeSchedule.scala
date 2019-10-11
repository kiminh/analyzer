package com.cpc.spark.oCPX.oCPC.pay

import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.oCPX.OcpcTools.{udfConcatStringInt, udfDetermineIndustry, udfDetermineMedia}
import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object OcpcChargeSchedule {
  def main(args: Array[String]): Unit = {
    /*
    ocpc周期控制模块
    结果表包括以下字段：unitid, pay_cnt, pay_date, flag

    pay_cnt: 已经完成赔付的次数
    pay_date: 当前赔付周期的起始日期
    flag: 当前周期是否需要计算赔付(基于pay_cnt判断)

    pay_cnt的更新：
    根据pay_date，date，dayCnt判断该赔付周期是否结束，如果当前周期结束，pay_cnt++

    pay_date的更新：
    更新逻辑同上
     */
    Logger.getRootLogger.setLevel(Level.WARN)
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()

    // 计算日期周期
    val date = args(0).toString
    val version = args(1).toString
    val dayCnt = args(2).toInt

    // 兼容逻辑：兼容老版本的逻辑
    // 抽取基本数据
    val scheduleData = getPaySchedule(date, spark)
    scheduleData
      .repartition(10)
      .write.mode("overwrite").saveAsTable("test.ocpc_pay_data20191010a")

    // 更新pay_cnt，pay_date
    val updateScheduleData = updatePaySchedule(date, dayCnt, scheduleData, spark)
    updateScheduleData
      .repartition(10)
      .write.mode("overwrite").saveAsTable("test.ocpc_pay_data20191010b")



  }


  def updatePaySchedule(date: String, dayCnt: Int, baseData: DataFrame, spark: SparkSession) = {
    baseData.createOrReplaceTempView("base_data")
    val sqlRequest =
      s"""
         |SELECT
         |  unitid,
         |  pay_cnt,
         |  pay_date,
         |  cast(date_add(pay_date, $dayCnt) as string) as end_date
         |FROM
         |  base_data
         |""".stripMargin
    println(sqlRequest)
    val rawData = spark.sql(sqlRequest)
    rawData.createOrReplaceTempView("raw_data")
    rawData
      .repartition(10)
      .write.mode("overwrite").saveAsTable("test.ocpc_pay_data20191010c")


    val sqlRequest2 =
      s"""
         |SELECT
         |  unitid,
         |  pay_cnt,
         |  pay_date,
         |  end_date,
         |  (case when end_date < '$date' then 1 else 0 end) as update_flag
         |FROM
         |  raw_data
         |""".stripMargin
    println(sqlRequest2)

    val data = spark
      .sql(sqlRequest2)
      .withColumn("flag", when(col("pay_cnt") < 4, 1).otherwise(0))
      .withColumn("pay_cnt", when(col("update_flag") === 1, col("pay_cnt") + 1).otherwise(col("pay_cnt")))
      .withColumn("pay_date", when(col("update_flag") === 1, col("current_date")).otherwise("pay_date"))

    data
  }

  def getPaySchedule(date: String, spark: SparkSession) = {
    // 获取老版的单元周期数据
    val prevData = spark
      .table("dl_cpc.ocpc_pay_cnt_daily")
      .where(s"`date` = '2019-10-10'")

    // 抽取媒体id，获取当天的数据
    val conf = ConfigFactory.load("ocpc")
    val conf_key = "medias.total.media_selection"
    val mediaSelection = conf.getString(conf_key)

    val sqlRequest =
      s"""
         |SELECT
         |  searchid,
         |  unitid,
         |  cast(ocpc_log_dict['IsHiddenOcpc'] as int) as is_hidden
         |FROM
         |  dl_cpc.ocpc_filter_unionlog
         |WHERE
         |  `date` = '$date'
         |AND
         |  $mediaSelection
         |AND
         |  is_ocpc = 1
         |AND
         |  isclick = 1
       """.stripMargin
    println(sqlRequest)
    val newData = spark
      .sql(sqlRequest)
      .filter(s"is_hidden = 0")
      .select("unitid")
      .distinct()
//
//    val newData = rawData
//        .withColumn("new_flag", lit(1))
//
//    // 数据union
//    // 取历史数据
//    val dateConverter = new SimpleDateFormat("yyyy-MM-dd")
//    val today = dateConverter.parse(date)
//    val calendar = Calendar.getInstance
//    calendar.setTime(today)
//    calendar.add(Calendar.DATE, +dayCnt)
//    val tomorrow = calendar.getTime
//    val nextPayDate = dateConverter.format(tomorrow)
//
    val data = prevData
      .filter(s"pay_date <= '$date'")
      .join(newData, Seq("unitid"), "outer")
      .select("unitid", "pay_cnt", "pay_date")
      .na.fill(0, Seq("pay_cnt"))
      .na.fill(date, Seq("pay_date"))
//      .withColumn("pay_cnt", when(col("pay_cnt") < 5, col("pay_cnt") + 1).otherwise(col("pay_cnt")))
//      .withColumn("flag", when(col("pay_cnt") < 5, 1).otherwise(0))

    data.printSchema()
    data
  }


}

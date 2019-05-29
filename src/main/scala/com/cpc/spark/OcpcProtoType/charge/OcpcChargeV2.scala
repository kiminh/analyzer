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

    val unitidList = getUnitList(date, media, version, dayCnt, spark)

    val clickData = getClickData(date, media, dayCnt, spark)
    val cvData = getCvData(date, dayCnt, spark)

    val data = clickData
      .join(cvData, Seq("searchid"), "left_outer")
      .join(unitidList.filter(s"flag == 1"), Seq("unitid"), "inner")

    val payData = calculatePay(data, date, dayCnt, spark)

    val resultDF1 = payData
      .selectExpr("unitid", "adslot_type", "cast(pay as bigint) pay", "cost", "cpareal", "cpagiven", "cv", "start_date")
      .withColumn("date", lit(date))
      .withColumn("version", lit(version))

    resultDF1.show(10)

    resultDF1
//      .repartition(5).write.mode("overwrite").saveAsTable("test.ocpc_pay_data_daily")
      .repartition(5).write.mode("overwrite").insertInto("dl_cpc.ocpc_pay_data_daily")

    val resultDF2 = unitidList
      .selectExpr("unitid", "pay_cnt", "pay_date")
      .withColumn("date", lit(date))
      .withColumn("version", lit(version))

    resultDF2.show(10)

    resultDF2
//      .repartition(5).write.mode("overwrite").saveAsTable("test.ocpc_pay_cnt_daily")
      .repartition(5).write.mode("overwrite").insertInto("dl_cpc.ocpc_pay_cnt_daily")

  }

  def getUnitList(date: String, media: String, version: String, dayCnt: Int, spark: SparkSession) = {
    /*
    1. 抽取赔付周期开始第一天有消费的单元
    2. 抽取赔付周期表中当天开始赔付的单元
    3. 两个部分的单元数据进行outer join，并更新赔付周期字段和赔付周期次数字段
     */

    // 抽取赔付周期开始第一天有消费的单元
    // 取历史数据
    val dateConverter = new SimpleDateFormat("yyyy-MM-dd")
    val today = dateConverter.parse(date)

    val calendar1 = Calendar.getInstance
    calendar1.setTime(today)
    calendar1.add(Calendar.DATE, -dayCnt)
    val prevDay = calendar1.getTime
    val date1 = dateConverter.format(prevDay)
    val selectCondition = s"`date` = '$date1'"

    val calendar2 = Calendar.getInstance()
    calendar2.setTime(today)
    calendar2.add(Calendar.DATE, +1)
    val tomorrow = calendar2.getTime
    val date2 = dateConverter.format(tomorrow)

    // 媒体选择
    val conf = ConfigFactory.load("ocpc")
    val conf_key1 = "medias." + media + ".media_selection"
    val mediaSelection = conf.getString(conf_key1)

    val sqlRequest1 =
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
    println(sqlRequest1)
    val rawData = spark
      .sql(sqlRequest1)
      .filter(s"is_hidden = 0 and conversion_goal = 3")

    val costUnits = rawData
      .select("unitid")
      .distinct()

    // 抽取赔付周期表中当天开始赔付的单元
    // 取历史数据
    val calendar3 = Calendar.getInstance
    calendar3.setTime(today)
    calendar3.add(Calendar.DATE, -1)
    val yesterday = calendar3.getTime
    val date3 = dateConverter.format(yesterday)

    println(s"today is '$date'")
    println(s"prev_day is '$date1'")
    println(s"yesterday is '$date3'")
    println(s"tomorrow is '$date2'")

    val sqlRequest2 =
      s"""
         |SELECT
         |  unitid,
         |  pay_cnt prev_pay_cnt,
         |  pay_date prev_pay_date,
         |  (case when pay_date = '$date1' then 1 else 0 end) as flag
         |FROM
         |  dl_cpc.ocpc_pay_cnt_daily
         |WHERE
         |  `date` = '$date3'
         |AND
         |  version = '$version'
       """.stripMargin
    println(sqlRequest2)
    val payUnits = spark.sql(sqlRequest2)

    // 数据关联并更新pay_cnt与pay_date:
    // 如果pay_cnt为空，则初始化为0，pay_date初始化为本赔付周期开始日期
    // 全部更新：pay_cnt加1，pay_date更新为下一个起始赔付周期
    val data = costUnits
      .join(payUnits, Seq("unitid"), "outer")
      .select("unitid", "prev_pay_cnt", "prev_pay_date", "flag")
      .na.fill(0, Seq("prev_pay_cnt"))
      .na.fill(date1, Seq("prev_pay_date"))
      .na.fill(1, Seq("flag"))
      .withColumn("pay_date", udfCalculatePayDate(date2)(col("prev_pay_cnt"), col("prev_pay_date"), col("flag")))
      .withColumn("pay_cnt", udfCalculateCnt()(col("prev_pay_cnt"), col("flag")))

    data.show(10)

    val result = data
      .select("unitid", "pay_cnt", "pay_date", "flag")

    result

  }

  def udfCalculateCnt() = udf((prevPayCnt: Int, flag: Int) => {
    var result = prevPayCnt
    if (flag == 1) {
      result += 1
    }
    result
  })

  def udfCalculatePayDate(date: String) = udf((prevPayCnt: Int, prevPayDate: String, flag: Int) => {
    var result = prevPayDate
    if (prevPayCnt < 4  && flag ==  1) {
      result = date
    }
    result
  })

//  def udfCalculateCnt() = udf((prevCnt: Int, flag: Int) => {
//    var currentCnt = prevCnt
//    if (flag == 0) {
//      currentCnt += 1
//    }
//    currentCnt
//  })

//  def getPayCnt(date: String, version: String, spark: SparkSession) = {
//    // 取历史数据
//    val dateConverter = new SimpleDateFormat("yyyy-MM-dd")
//    val today = dateConverter.parse(date)
//    val calendar = Calendar.getInstance
//    calendar.setTime(today)
//    calendar.add(Calendar.DATE, -1)
//    val yesterday = calendar.getTime
//    val date1 = dateConverter.format(yesterday)
//    val selectCondition = s"`date` = '$date1'"
//
//    val sqlRequest =
//      s"""
//         |SELECT
//         |  unitid,
//         |  pay_cnt as prev_cnt
//         |FROM
//         |  dl_cpc.ocpc_pay_cnt_daily
//         |WHERE
//         |  $selectCondition
//         |AND
//         |  version = '$version'
//       """.stripMargin
//    println(sqlRequest)
//    val data = spark.sql(sqlRequest)
//
//    data
//  }

//  def getPrevData(date: String, dayCnt: Int, version: String, spark: SparkSession) = {
//    // 取历史数据
//    val dateConverter = new SimpleDateFormat("yyyy-MM-dd")
//    val today = dateConverter.parse(date)
//    val calendar = Calendar.getInstance
//    calendar.setTime(today)
//    calendar.add(Calendar.DATE, -dayCnt)
//    val yesterday = calendar.getTime
//    val date1 = dateConverter.format(yesterday)
//    val selectCondition = s"`date` >= '$date1'"
//
//    val sqlRequest =
//      s"""
//         |SELECT
//         |  unitid,
//         |  1 as flag
//         |FROM
//         |  dl_cpc.ocpc_pay_data_daily
//         |WHERE
//         |  $selectCondition
//         |AND
//         |  version = '$version'
//       """.stripMargin
//    println(sqlRequest)
//    val data = spark.sql(sqlRequest).distinct()
//
//    data
//  }

  def calculatePay(baseData: DataFrame, date: String, dayCnt: Int, spark: SparkSession) = {
    // 取历史数据
    val dateConverter = new SimpleDateFormat("yyyy-MM-dd")
    val today = dateConverter.parse(date)
    val calendar = Calendar.getInstance
    calendar.setTime(today)
    calendar.add(Calendar.DATE, -dayCnt)
    val yesterday = calendar.getTime
    val date1 = dateConverter.format(yesterday)

    baseData.createOrReplaceTempView("base_data")
    val sqlRequest1 =
      s"""
         |SELECT
         |  unitid,
         |  adslot_type,
         |  sum(case when isclick=1 then price else 0 end) as cost,
         |  sum(iscvr) as cv,
         |  sum(case when isclick=1 then cpagiven else 0 end) * 1.0 / sum(isclick) as cpagiven
         |FROM
         |  base_data
         |GROUP BY unitid, adslot_type
       """.stripMargin
    println(sqlRequest1)
    val rawData = spark.sql(sqlRequest1).na.fill(0, Seq("cv"))
    rawData.createOrReplaceTempView("raw_data")

    val sqlRequest2 =
      s"""
         |SELECT
         |  unitid,
         |  adslot_type,
         |  cost - 1.2 * cv * cpagiven as pay,
         |  cost,
         |  cv,
         |  cost * 1.0 / cv as cpareal,
         |  cpagiven
         |FROM
         |  raw_data
       """.stripMargin
    println(sqlRequest2)
    val result = spark
      .sql(sqlRequest2)
      .withColumn("pay", when(col("pay") <= 0.0, 0.0).otherwise(col("pay")))
      .withColumn("pay", when(col("cv") === 0, col("cost")).otherwise(col("pay")))
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
         |  adslot_type,
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
    val result = spark
      .sql(sqlRequest)
      .filter(s"is_hidden = 0 and conversion_goal = 3")


    result.printSchema()
    result.show(10)
    result

  }
}

package com.cpc.spark.OcpcProtoType.charge

import java.text.SimpleDateFormat
import java.util.Calendar

import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object OcpcChargeTotalV2 {
  def main(args: Array[String]): Unit = {
    /*
    v2版本赔付数据
    1. 仅包括在趣头条投放的oCPC广告
    2. api-app和二类电商广告
    3. 允许重复赔付，赔付周期为7天
     */
    Logger.getRootLogger.setLevel(Level.WARN)
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()

    // 计算日期周期
    val date = args(0).toString
    val version = args(1).toString
    val media = args(2).toString
    val dayCnt = args(3).toInt

    val unitidList = getUnitList(date, media, version, dayCnt, spark).cache()
    unitidList.show(10)


    val clickData = getClickData(date, media, dayCnt, spark)
    val cv2Data = getCvData(date, 2, dayCnt, spark)
    val cv3Data = getCvData(date, 3, dayCnt, spark)
    val cpcData = getCPCdata(date, media, dayCnt, spark)

    val data = clickData
      .join(cv2Data, Seq("searchid"), "left_outer")
      .join(cv3Data, Seq("searchid"), "left_outer")
      .na.fill(0, Seq("cvr2", "cvr3"))
      .join(unitidList.filter(s"flag == 1"), Seq("unitid"), "inner")
      .withColumn("iscvr", udfSelectCv()(col("conversion_goal"), col("cvr2"), col("cvr3")))

    val payData = calculatePay(data, cpcData, date, dayCnt, spark).cache()
    payData.show(10)

    val resultDF1 = payData
      .selectExpr("unitid", "adslot_type", "cast(pay as bigint) pay", "cost", "cpareal", "cpagiven", "cv", "start_date", "cpc_flag")
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

  def udfSelectCv() = udf((conversionGoal: Int, iscvr2: Int, iscvr3: Int) => {
    var iscvr = conversionGoal match {
      case 2 => iscvr2
      case 3 => iscvr3
      case _ => 0
    }
    iscvr
  })

  def getCPCdata(date: String, media: String, dayCnt: Int, spark: SparkSession) = {
    // 取历史数据
    val dateConverter = new SimpleDateFormat("yyyy-MM-dd")
    val today = dateConverter.parse(date)
    val calendar = Calendar.getInstance
    calendar.setTime(today)
    calendar.add(Calendar.DATE, -dayCnt)
    calendar.add(Calendar.DATE, +1)
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
         |  unitid
         |FROM
         |  dl_cpc.ocpc_base_unionlog
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
         |AND
         |  length(ocpc_log) = 0
       """.stripMargin
    println(sqlRequest)
    val result = spark
        .sql(sqlRequest)
        .select("unitid")
        .withColumn("cpc_flag", lit(1))
        .distinct()
        .cache()


    result.printSchema()
    result.show(10)
    result
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
         |  (case
         |        when (cast(adclass as string) like '134%' or cast(adclass as string) like '107%') then "elds"
         |        when (adslot_type<>7 and cast(adclass as string) like '100%') then "feedapp"
         |        when (adslot_type=7 and cast(adclass as string) like '100%') then "yysc"
         |        when adclass in (110110100, 125100100) then "wzcp"
         |        else "others"
         |  end) as industry,
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
       """.stripMargin
    println(sqlRequest1)
    val rawData = spark
      .sql(sqlRequest1)
      .filter(s"is_hidden = 0")
      .filter(s"(industry = 'feedapp' and conversion_goal = 2) or (industry = 'elds' and conversion_goal = 3)")

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

  def calculatePay(baseData: DataFrame, cpcData: DataFrame, date: String, dayCnt: Int, spark: SparkSession) = {
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
      .join(cpcData, Seq("unitid"), "left_outer")
      .na.fill(0, Seq("cpc_flag"))

    result

  }

  def getCvData(date: String, conversionGoal: Int, dayCnt: Int, spark: SparkSession) = {
    // 取历史数据
    val dateConverter = new SimpleDateFormat("yyyy-MM-dd")
    val today = dateConverter.parse(date)
    val calendar = Calendar.getInstance
    calendar.setTime(today)
    calendar.add(Calendar.DATE, -dayCnt)
    val yesterday = calendar.getTime
    val date1 = dateConverter.format(yesterday)
    val selectCondition = s"`date` >= '$date1'"
    val cvrType = "cvr" + conversionGoal.toString

    val sqlRequest =
      s"""
         |SELECT
         |  distinct searchid,
         |  1 as iscvr
         |FROM
         |  dl_cpc.ocpc_label_cvr_hourly
         |WHERE
         |  $selectCondition
         |AND
         |  cvr_goal = '$cvrType'
       """.stripMargin
    println(sqlRequest)
    val data = spark
        .sql(sqlRequest)
        .withColumn(cvrType, col("iscvr"))
        .select("searchid", cvrType)

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
         |  (case
         |        when (cast(adclass as string) like '134%' or cast(adclass as string) like '107%') then "elds"
         |        when (adslot_type<>7 and cast(adclass as string) like '100%') then "feedapp"
         |        when (adslot_type=7 and cast(adclass as string) like '100%') then "yysc"
         |        when adclass in (110110100, 125100100) then "wzcp"
         |        else "others"
         |    end) as industry,
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
       """.stripMargin
    println(sqlRequest)
    val result = spark
      .sql(sqlRequest)
      .filter(s"is_hidden = 0")
      .filter(s"(industry = 'feedapp' and conversion_goal = 2) or (industry = 'elds' and conversion_goal = 3)")


    result.printSchema()
    result.show(10)
    result

  }
}

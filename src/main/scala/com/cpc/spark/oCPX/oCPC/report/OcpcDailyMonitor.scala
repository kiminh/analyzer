package com.cpc.spark.oCPX.oCPC.report

import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.oCPX.OcpcTools._
import com.cpc.spark.tools.testOperateMySQL
import com.cpc.spark.udfs.Udfs_wj.udfStringToMap
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}


object OcpcDailyMonitor {
  def main(args: Array[String]): Unit = {
    /*
    新版报表程序
    1. 从ocpc_unionlog拉取ocpc广告记录
    2. 采用数据关联方式获取转化数据
    3. 统计分ideaid级别相关数据
    4. 统计分conversion_goal级别相关数据
    5. 存储到hdfs
    6. 存储到mysql
     */
    val spark = SparkSession
      .builder()
      .appName("OcpcHourlyReport")
      .enableHiveSupport()
      .getOrCreate()

    val date = args(0).toString
    val hour = args(1).toString
    println("parameters:")
    println(s"date=$date, hour=$hour")

    val baseData1 = getBaseStatData(date, 1, spark)
    val baseData2 = getBaseStatData(date, 2, spark)

    val unitData = calculateUnit(baseData1, spark)
    val payUnits = unitData
      .filter(s"cost >= 1000")
      .filter(s"pay > 0")

    val result = checkPayReason(payUnits, baseData2, date, hour, spark)
    result
      .withColumn("date", lit(date))
      .repartition(5)
      .write.mode("overwrite").saveAsTable("test.ocpc_daily_over_cost_units_report20190802")

  }

  def checkPayReason(payData: DataFrame, baseData: DataFrame, date: String, hour: String, spark: SparkSession) = {
    /*
    对超成本单元进行数据分析:
    1. 平均投放时间过短
    2. 相比前一天，原始pcoc有突然上升
    3. 相比前一天，校准pcoc有突然上升
    4. 最近两天有CPC阶段消费
     */
    // 平均投放时间过短
    val data1 = payData
      .withColumn("flag1", udfCheckReason1()(col("hour_cnt")))
      .select("unitid", "userid", "adclass", "media", "industry", "conversion_goal", "is_hidden", "flag1")

    // 相比前一天，原始pcoc有突然上升
    val pcoc1 = payData
      .withColumn("pcoc1", col("pre_cvr") / col("post_cvr"))
    val pcoc0 = baseData
      .withColumn("pcoc0", col("pre_cvr") / col("post_cvr"))
      .select("unitid", "userid", "adclass", "media", "industry", "conversion_goal", "is_hidden", "pcoc0")
    val data2 = pcoc1
      .join(pcoc0, Seq("unitid", "userid", "adclass", "media", "industry", "conversion_goal", "is_hidden"), "inner")
      .withColumn("flag2", udfCheckReasonPCOC()(col("pcoc1"), col("pcoc0")))
      .select("unitid", "userid", "adclass", "media", "industry", "conversion_goal", "is_hidden", "flag2")

    // 相比前一天，校准pcoc有突然上升
    val calipcoc1 = payData
      .withColumn("pcoc1", col("cali_precvr") / col("post_cvr"))
    val calipcoc0 = baseData
      .withColumn("pcoc0", col("cali_precvr") / col("post_cvr"))
      .select("unitid", "userid", "adclass", "media", "industry", "conversion_goal", "is_hidden", "pcoc0")
    val data3 = calipcoc1
      .join(calipcoc0, Seq("unitid", "userid", "adclass", "media", "industry", "conversion_goal", "is_hidden"), "inner")
      .withColumn("flag3", udfCheckReasonPCOC()(col("pcoc1"), col("pcoc0")))
      .select("unitid", "userid", "adclass", "media", "industry", "conversion_goal", "is_hidden", "flag3")

    // 最近两天有CPC阶段消费
    // 取历史数据
    val dateConverter = new SimpleDateFormat("yyyy-MM-dd")
    val today = dateConverter.parse(date)
    val calendar = Calendar.getInstance
    calendar.setTime(today)
    calendar.add(Calendar.DATE, -1)
    val yesterday = calendar.getTime
    val date1 = dateConverter.format(yesterday)
    val selectCondition = s"`date` between '$date1' and '$date'"

    val conf = ConfigFactory.load("ocpc")
    val conf_key = "medias.total.media_selection"
    val mediaSelection = conf.getString(conf_key)

    val sqlRequest =
      s"""
         |SELECT
         |    searchid,
         |    ideaid,
         |    unitid,
         |    userid,
         |    adslot_type,
         |    adclass,
         |    conversion_goal,
         |    isclick,
         |    isshow,
         |    price,
         |    bid_discounted_by_ad_slot as bid,
         |    exp_cvr,
         |    exp_ctr,
         |    media_appsid,
         |    ocpc_log,
         |    hour as hr
         |FROM
         |    dl_cpc.ocpc_base_unionlog
         |WHERE
         |    $selectCondition
         |AND
         |    $mediaSelection
         |AND
         |    ocpc_step = 1
         |AND
         |    isclick = 1
       """.stripMargin
    println(sqlRequest)
    val cpcData = spark
      .sql(sqlRequest)
      .withColumn("media", udfDetermineMedia()(col("media_appsid")))
      .withColumn("industry", udfDetermineIndustry()(col("adslot_type"), col("adclass")))
      .groupBy("unitid", "userid", "adclass", "media", "industry", "conversion_goal")
      .agg(
        sum("isclick").alias("cpc_click")
      )
      .filter(s"cpc_click > 0")
    val data4 = payData
      .join(cpcData, Seq("unitid", "userid", "adclass", "media", "industry", "conversion_goal"), "inner")
      .withColumn("flag4", udfCheckCPCreason()(col("click"), col("cpc_click")))
      .select("unitid", "userid", "adclass", "media", "industry", "conversion_goal", "flag4")

    // 数据汇总
    val result = payData
      .join(data1, Seq("unitid", "userid", "adclass", "media", "industry", "conversion_goal", "is_hidden"), "left_outer")
      .join(data2, Seq("unitid", "userid", "adclass", "media", "industry", "conversion_goal", "is_hidden"), "left_outer")
      .join(data3, Seq("unitid", "userid", "adclass", "media", "industry", "conversion_goal", "is_hidden"), "left_outer")
      .join(data4, Seq("unitid", "userid", "adclass", "media", "industry", "conversion_goal"), "left_outer")
      .na.fill(0, Seq("flag1", "flag2", "flag3", "flag4"))

    result

  }

  def udfCheckCPCreason() = udf((click: Int, cpcClick: Int) => {
    var result = 0
    val percent = cpcClick.toDouble / click.toDouble
    if (percent >= 0.1) {
      result = 1
    } else {
      result = 0
    }
    result
  })

  def udfCheckReasonPCOC() = udf((pcoc1: Double, pcoc0: Double) => {
    var diffPercent = (pcoc1 - pcoc0) / pcoc0
    var result = 0
    if (diffPercent > 0.2) {
      result = 1
    } else {
      result = 0
    }
    result
  })

  def udfCheckReason1() = udf((hourCnt: Int) => {
    var result = 0
    if (hourCnt < 6) {
      result = 1
    } else {
      result = 0
    }
    result
  })

  def getBaseStatData(date: String, dayInt: Int, spark: SparkSession) = {
    // 取历史数据
    val dateConverter = new SimpleDateFormat("yyyy-MM-dd")
    val today = dateConverter.parse(date)
    val calendar = Calendar.getInstance
    calendar.setTime(today)
    calendar.add(Calendar.DATE, -dayInt)
    val yesterday = calendar.getTime
    val date1 = dateConverter.format(yesterday)

    val sqlRequest =
      s"""
         |SELECT
         |  *
         |FROM
         |  dl_cpc.ocpc_report_base_hourly
         |WHERE
         |  `date` = '$date1'
       """.stripMargin
    println(sqlRequest)
    val data = spark.sql(sqlRequest)
    data
  }



  def calculateUnit(baseData: DataFrame, spark: SparkSession) = {
    baseData.createOrReplaceTempView("base_data")
    val sqlRequest =
      s"""
         |SELECT
         |    unitid,
         |    userid,
         |    adclass,
         |    media,
         |    industry,
         |    conversion_goal,
         |    is_hidden,
         |    sum(show) as show,
         |    sum(click) as click,
         |    sum(cv) as cv,
         |    sum(total_prectr) * 1.0 / sum(show) as pre_ctr,
         |    sum(click) * 1.0 / sum(show) as post_ctr,
         |    sum(total_precvr) * 1.0 / sum(click) as pre_cvr,
         |    sum(total_calipcvr) * 1.0 / sum(click) as cali_precvr,
         |    sum(cv) * 1.0 / sum(click) as post_cvr,
         |    sum(total_price) * 0.01 as cost,
         |    (case
         |      when sum(total_price) <= 1.2 * sum(cv) * sum(total_cpagiven) / sum(click) then 0
         |      else sum(total_price) - 1.2 * sum(cv) * sum(total_cpagiven) / sum(click)
         |    end) * 0.01 as pay,
         |    sum(total_price) * 1.0 / sum(click) as acp,
         |    sum(total_bid) * 1.0 / sum(click) as acb,
         |    sum(total_cpagiven) * 1.0 / sum(click) as cpagiven,
         |    sum(total_price) * 1.0 / sum(cv) as cpareal,
         |    sum(total_cpasuggest) * 1.0 / sum(click) as cpasuggest,
         |    sum(total_jfbfactor) * 1.0 / sum(click) as jfb_factor,
         |    sum(total_cvrfactor) * 1.0 / sum(click) as cvr_factor,
         |    sum(total_calipostcvr) * 1.0 / sum(click) as cali_postcvr,
         |    sum(total_smooth_factor) * 1.0 / sum(click) as smooth_factor,
         |    count(1) as hour_cnt
         |FROM
         |    base_data
         |GROUP BY unitid, userid, adclass, media, industry, conversion_goal, is_hidden
       """.stripMargin
    println(sqlRequest)
    val data = spark.sql(sqlRequest)

    val result = data
      .na.fill(0, Seq("cv"))
//      .withColumn("pay", udfCalculatePay()(col("cost"), col("cv"), col("cpagiven")))
    println("unit data:")
    result.show(10)
    result
  }

}
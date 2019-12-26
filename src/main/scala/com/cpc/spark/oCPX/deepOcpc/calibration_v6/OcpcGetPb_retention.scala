package com.cpc.spark.oCPX.deepOcpc.calibration_v6

import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.oCPX.OcpcTools._
import com.cpc.spark.oCPX.oCPC.calibration_by_tag.OcpcGetPb_weightv1.udfCalculateHourDiff
import com.typesafe.config.ConfigFactory
//import com.cpc.spark.oCPX.deepOcpc.calibration_v2.OcpcRetentionFactor._
//import com.cpc.spark.oCPX.deepOcpc.calibration_v2.OcpcShallowFactor._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}


object OcpcGetPb_retention {
  /*
  采用基于后验激活率的复合校准策略
  jfb_factor：正常计算
  cvr_factor：
  cvr_factor = (deep_cvr * post_cvr1) / pre_cvr1
  smooth_factor = 0.3
   */
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()
    Logger.getRootLogger.setLevel(Level.WARN)

    val date = args(0).toString
    val hour = args(1).toString
    val version = args(2).toString
    val expTag = args(3).toString
    val hourInt = args(4).toInt
    val minCV1 = args(5).toInt
    val minCV2 = args(6).toInt

    println("parameters:")
    println(s"date=$date, hour=$hour, version:$version, expTag:$expTag, hourInt:$hourInt")

    // base data
    val dataRaw = OcpcCalibrationBase(date, hour, 72, spark)

    /*
    jfb_factor calculation
     */

    /*
    cvr factor calculation
     */
    val cvrData = calculateCvrFactor(dataRaw, date, hour, spark)


  }

  def calculateCvrFactor(dataRaw: DataFrame, date: String, hour: String, spark: SparkSession) = {
    /*
    method to calculate cvr_factor: predict the retention cv

    cvr_factor = post_cvr1 * deep_cvr / pre_cvr2

    deep_cvr: calculate by natural day
    post_cvr1, pre_cvr2: calculate by sliding time window
     */

    // calculate post_cvr1, pre_cvr2
    val data1 = calculateDataCvr(dataRaw, 80, spark)

    // calculate deep_cvr
    val data2 = calculateDeepCvr(date, 3, spark)

    // data join
    val data = data1
      .join(data2, Seq("unitid", "media"), "inner")


  }

  // base data
  def OcpcCalibrationBase(date: String, hour: String, hourInt: Int, spark: SparkSession) = {
    val dateConverter = new SimpleDateFormat("yyyy-MM-dd HH")
    val newDate = date + " " + hour
    val today = dateConverter.parse(newDate)
    val calendar = Calendar.getInstance
    calendar.setTime(today)
    calendar.add(Calendar.HOUR, -hourInt)
    val yesterday = calendar.getTime
    val tmpDate = dateConverter.format(yesterday)
    val tmpDateValue = tmpDate.split(" ")
    val date1 = tmpDateValue(0)
    val hour1 = tmpDateValue(1)
    val selectCondition = getTimeRangeSqlDate(date1, hour1, date, hour)

    val sqlRequest =
      s"""
         |SELECT
         |  searchid,
         |  unitid,
         |  userid,
         |  isshow,
         |  isclick,
         |  deep_cvr * 1.0 / 1000000 as deep_cvr,
         |  bid_discounted_by_ad_slot as bid,
         |  price,
         |  media_appsid,
         |  conversion_goal,
         |  conversion_from,
         |  hidden_tax,
         |  date,
         |  hour
         |FROM
         |  dl_cpc.ocpc_base_unionlog
         |WHERE
         |  $selectCondition
         |AND
         |  is_deep_ocpc = 1
         |AND
         |  isclick = 1
         |AND
         |  deep_conversion_goal = 2
       """.stripMargin
    println(sqlRequest)
    val clickDataRaw = spark.sql(sqlRequest)

    // 清除米读异常数据
    val clickData = mapMediaName(clickDataRaw, spark)

    // 抽取cv数据
    val sqlRequest2 =
      s"""
         |SELECT
         |  searchid,
         |  conversion_goal,
         |  conversion_from,
         |  1 as iscvr
         |FROM
         |  dl_cpc.ocpc_cvr_log_hourly
         |WHERE
         |  $selectCondition
       """.stripMargin
    println(sqlRequest2)
    val cvData = spark.sql(sqlRequest2).distinct()


    // 数据关联
    val baseData = clickData
      .join(cvData, Seq("searchid", "conversion_goal", "conversion_from"), "left_outer")
      .na.fill(0, Seq("iscvr"))
      .withColumn("bid", udfCalculateBidWithHiddenTax()(col("date"), col("bid"), col("hidden_tax")))
      .withColumn("price", udfCalculatePriceWithHiddenTax()(col("price"), col("hidden_tax")))
      .withColumn("hour_diff", udfCalculateHourDiff(date, hour)(col("date"), col("hour")))

    // 计算结果
    val resultDF = calculateParameter(baseData, spark)

    resultDF
  }

  def getDataByHourDiff(dataRaw: DataFrame, leftHourBound: Int, rightHourBound: Int, spark: SparkSession) = {
    dataRaw
      .createOrReplaceTempView("raw_data")

    val selectCondition = s"hour_diff >= $leftHourBound and hour_diff < $rightHourBound"
    val sqlRequest =
      s"""
         |SELECT
         |    unitid,
         |    conversion_goal,
         |    media,
         |    sum(click) as click,
         |    sum(cv1) as cv1,
         |    sum(pre_cvr2 * click) * 1.0 / sum(click) as pre_cvr2,
         |    sum(cv1) * 1.0 / sum(click) as post_cvr1,
         |    sum(acb * click) * 1.0 / sum(click) as acb,
         |    sum(acp * click) * 1.0 / sum(click) as acp
         |FROM
         |    raw_data
         |WHERE
         |    $selectCondition
         |GROUP BY unitid, conversion_goal, media
         |""".stripMargin
    println(sqlRequest)
    val data = spark
      .sql(sqlRequest)
      .select("unitid", "conversion_goal", "media", "click", "cv1", "pre_cvr2", "post_cvr1")

    data
  }

  def calculateParameter(rawData: DataFrame, spark: SparkSession) = {
    val data  =rawData
      .filter(s"isclick=1")
      .groupBy("unitid", "conversion_goal", "media", "date", "hour", "hour_diff")
      .agg(
        sum(col("isclick")).alias("click"),
        sum(col("iscvr")).alias("cv1"),
        avg(col("bid")).alias("acb"),
        avg(col("price")).alias("acp"),
        avg(col("deep_cvr")).alias("pre_cvr2")
      )
      .select("unitid", "conversion_goal", "media", "click", "cv1", "pre_cvr2", "acb", "acp", "date", "hour", "hour_diff")

    data
  }


  // calculate post_cvr1, pre_cvr2 in sliding time window
  def calculateDataCvr(dataRaw: DataFrame, minCV: Int, spark: SparkSession) = {
    val dataRaw1 = getDataByHourDiff(dataRaw, 0, 6, spark)
    val data1 = dataRaw1
      .filter(s"cv1 >= 80")
      .withColumn("priority", lit(1))
    data1.show(10)

    val dataRaw2 = getDataByHourDiff(dataRaw, 0, 12, spark)
    val data2 = dataRaw2
      .filter(s"cv1 >= 80")
      .withColumn("priority", lit(2))
    data2.show(10)

    val dataRaw3 = getDataByHourDiff(dataRaw, 0, 24, spark)
    val data3 = dataRaw3
      .filter(s"cv1 >= 80")
      .withColumn("priority", lit(3))
    data3.show(10)

    val dataRaw4 = getDataByHourDiff(dataRaw, 0, 48, spark)
    val data4 = dataRaw4
      .filter(s"cv1 >= 80")
      .withColumn("priority", lit(4))
    data4.show(10)

    val dataRaw5 = getDataByHourDiff(dataRaw, 0, 72, spark)
    val data5 = dataRaw5
      .filter(s"cv1 > 0")
      .withColumn("priority", lit(5))
    data5.show(10)

    val data = data1.union(data2).union(data3).union(data4).union(data5)

    data.createOrReplaceTempView("data")

    val sqlRequest =
      s"""
         |SELECT
         |  unitid,
         |  conversion_goal,
         |  media,
         |  click,
         |  cv1,
         |  pre_cvr2,
         |  post_cvr1,
         |  priority,
         |  row_number() over(partition by unitid, conversion_goal, media order by priority) as seq
         |FROM
         |  data
         |""".stripMargin
    println(sqlRequest)
    val result = spark.sql(sqlRequest)

    result
  }

  def calculateDeepCvr(date: String, dayInt: Int, spark: SparkSession) = {
    // 取历史数据
    val dateConverter = new SimpleDateFormat("yyyy-MM-dd")
    val today = dateConverter.parse(date)
    val calendar = Calendar.getInstance
    calendar.setTime(today)
    calendar.add(Calendar.DATE, -1)
    val date1String = calendar.getTime
    val date1 = dateConverter.format(date1String)
    calendar.add(Calendar.DATE, -1)
    val date2String = calendar.getTime
    val date2 = dateConverter.format(date2String)
    calendar.add(Calendar.DATE, -dayInt)
    val date3String = calendar.getTime
    val date3 = dateConverter.format(date3String)

    // 激活数据
    val sqlRequest1 =
      s"""
         |SELECT
         |  searchid,
         |  unitid,
         |  userid,
         |  conversion_goal,
         |  media_appsid,
         |  1 as iscvr1
         |FROM
         |  dl_cpc.cpc_conversion
         |WHERE
         |  day between '$date3' and '$date2'
         |AND
         |  array_contains(conversion_target, 'api_app_active')
         |""".stripMargin
    println(sqlRequest1)
    val data1Raw = spark
      .sql(sqlRequest1)
      .distinct()

    val data1 = mapMediaName(data1Raw, spark)

    // 次留数据
    val sqlRequest2 =
      s"""
         |SELECT
         |  searchid,
         |  1 as iscvr2
         |FROM
         |  dl_cpc.cpc_conversion
         |WHERE
         |  day >= '$date3'
         |AND
         |  array_contains(conversion_target, 'api_app_retention')
         |""".stripMargin
    println(sqlRequest2)
    val data2 = spark
      .sql(sqlRequest2)
      .distinct()

    val data = data1
      .join(data2, Seq("searchid"), "left_outer")
      .groupBy("unitid", "media")
      .agg(
        sum(col("iscvr1")).alias("cv1"),
        sum(col("iscvr2")).alias("cv2")
      )
      .select("unitid", "media", "cv1", "cv2")
      .withColumn("deep_cvr", col("cv2") * 1.0 / col("cv1"))

    data
  }


}

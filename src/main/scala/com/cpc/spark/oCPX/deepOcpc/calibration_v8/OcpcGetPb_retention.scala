package com.cpc.spark.oCPX.deepOcpc.calibration_v8

import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.oCPX.OcpcTools._
import com.typesafe.config.ConfigFactory
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

    println("parameters:")
    println(s"date=$date, hour=$hour, version:$version, expTag:$expTag")

    // base data
    val dataRaw = OcpcCalibrationBase(date, hour, 96, spark)

    /*
    jfb_factor calculation
     */
    val jfbData = calcualteJfbFactor(dataRaw, spark)

    /*
    cvr factor calculation
     */
    val cvrData = calculateCvrFactor(dataRaw, date, hour, spark)

    val resultData = assemblyData(jfbData, cvrData, expTag, spark)

    val result = resultData
      .withColumn("cpagiven", lit(1.0))
      .withColumn("is_hidden", lit(0))
      .withColumn("date", lit(date))
      .withColumn("hour", lit(hour))
      .withColumn("version", lit(version))
      .select("identifier", "conversion_goal", "jfb_factor", "post_cvr", "smooth_factor", "cvr_factor", "high_bid_factor", "low_bid_factor", "cpagiven", "date", "hour", "exp_tag", "is_hidden", "version")

    val resultDF = result
      .select("identifier", "conversion_goal", "jfb_factor", "post_cvr", "smooth_factor", "cvr_factor", "high_bid_factor", "low_bid_factor", "cpagiven", "date", "hour", "exp_tag", "is_hidden", "version")


    resultDF
      .withColumn("deep_conversion_goal", lit(2))
      .repartition(1)
//      .write.mode("overwrite").insertInto("test.ocpc_deep_pb_data_hourly_exp")
      .write.mode("overwrite").insertInto("dl_cpc.ocpc_deep_pb_data_hourly_exp")


  }

  def assemblyData(jfbData: DataFrame, cvrData: DataFrame, expTag: String, spark: SparkSession) = {
    // 组装数据
    // set some default value
    // post_cvr: 0.0
    // smooth_factor: 0.3
    // high_bid_factor: 1.0
    // low_bid_factor: 1.0
    val data = jfbData
      .join(cvrData, Seq("unitid", "conversion_goal", "media"), "inner")
      .selectExpr("cast(unitid as string) identifier", "conversion_goal", "media", "jfb_factor", "cvr_factor")
      .withColumn("media", udfMediaName()(col("media")))
      .withColumn("exp_tag", udfSetExpTag(expTag)(col("media")))
      .withColumn("post_cvr", lit(0.0))
      .withColumn("smooth_factor", lit(0.3))
      .withColumn("high_bid_factor", lit(1.0))
      .withColumn("low_bid_factor", lit(1.0))
      .select("identifier", "conversion_goal", "exp_tag", "jfb_factor", "post_cvr", "smooth_factor", "cvr_factor", "high_bid_factor", "low_bid_factor")
      .na.fill(1.0, Seq("jfb_factor", "cvr_factor", "high_bid_factor", "low_bid_factor"))
      .na.fill(0.0, Seq("post_cvr", "smooth_factor"))


    data
  }


  // calculate jfb factor
  def calcualteJfbFactor(dataRaw: DataFrame, spark: SparkSession) = {
    val data = getDataByHourDiff(dataRaw, 0, 48, spark)

    val result = data
      .withColumn("jfb_factor", col("acb") * 1.0 / col("acp"))

    val resultDF = result.select("unitid", "conversion_goal", "media", "jfb_factor")

    resultDF
  }

  // calculate cvr factor
  def calculateCvrFactor(dataRaw: DataFrame, date: String, hour: String, spark: SparkSession) = {
    /*
    method to calculate cvr_factor: predict the retention cv

    calibration_value = post_cvr2 / pre_cvr2
    post_cvr2 = (cv1_t1 * deep_cvr + cv2_t2 * recall_t2 + cv2_t3 * recall_t3 + cv2_t4 * recall_t4) / (click_t1 + click_t2 + click_t3 + click_t4)
     */
    // calculate deep_cvr
    val deepCvr = calculateDeepCvr(date, 3, spark)

    // calculate cv2_t1
    val data1 = calculateCvrPart1(dataRaw, deepCvr, 10, spark)

    // calculate cv2_t2 ~ cv2_t4
    val data2 = calculateCvrPart2(dataRaw, 20, spark)

    // data join
    val data = data1.union(data2)

    data.createOrReplaceTempView("data")

    val sqlRequest =
      s"""
         |SELECT
         |  unitid,
         |  conversion_goal,
         |  media,
         |  sum(click) as click,
         |  sum(cv2) as cv2,
         |  sum(pre_cvr2 * click) * 1.0 / sum(click) as pre_cvr2,
         |  sum(cv2_recall) as cv2_recall
         |FROM
         |  result_table
         |GROUP BY unitid, conversion_goal, media
         |""".stripMargin
    println(sqlRequest)
    val result = spark.sql(sqlRequest)
      .withColumn("post_cvr2", col("cv2_recall") * 1.0 / col("click"))
      .withColumn("cvr_factor", col("post_cvr2") * 1.0 / col("pre_cvr2"))

    val resultDF = result
      .select("unitid", "conversion_goal", "media", "cvr_factor")

    resultDF
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
         |  deep_conversion_goal,
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

    // get shallow cv data
    val sqlRequest2 =
      s"""
         |SELECT
         |  searchid,
         |  conversion_goal,
         |  conversion_from,
         |  1 as iscvr1
         |FROM
         |  dl_cpc.ocpc_cvr_log_hourly
         |WHERE
         |  $selectCondition
       """.stripMargin
    println(sqlRequest2)
    val cvData1 = spark.sql(sqlRequest2).distinct()

    // get deep cv data
    val sqlRequest3 =
      s"""
         |SELECT
         |  searchid,
         |  deep_conversion_goal,
         |  1 as iscvr2
         |FROM
         |  dl_cpc.ocpc_label_deep_cvr_hourly
         |WHERE
         |  $selectCondition
       """.stripMargin
    println(sqlRequest3)
    val cvData2 = spark.sql(sqlRequest3).distinct()

    // 数据关联
    val baseData = clickData
      .join(cvData1, Seq("searchid", "conversion_goal", "conversion_from"), "left_outer")
      .join(cvData2, Seq("searchid", "deep_conversion_goal"), "left_outer")
      .na.fill(0, Seq("iscvr1", "iscvr2"))
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
         |    sum(cv2) as cv2,
         |    sum(pre_cvr2 * click) * 1.0 / sum(click) as pre_cvr2,
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
      .select("unitid", "conversion_goal", "media", "click", "cv1", "cv2", "pre_cvr2", "acb", "acp")

    data
  }

  def calculateParameter(rawData: DataFrame, spark: SparkSession) = {
    val data = rawData
      .filter(s"isclick=1")
      .groupBy("unitid", "deep_conversion_goal", "media", "date", "hour", "hour_diff")
      .agg(
        sum(col("isclick")).alias("click"),
        sum(col("iscvr1")).alias("cv1"),
        sum(col("iscvr2")).alias("cv2"),
        avg(col("bid")).alias("acb"),
        avg(col("price")).alias("acp"),
        avg(col("deep_cvr")).alias("pre_cvr2")
      )
      .withColumn("conversion_goal", col("deep_conversion_goal"))
      .select("unitid", "conversion_goal", "media", "click", "cv1", "cv2", "pre_cvr2", "acb", "acp", "date", "hour", "hour_diff")

    data
  }


  def calculateCvrPart2(dataRaw: DataFrame, minCV: Int, spark: SparkSession) = {
    // get cv data
    val dataRaw1 = getDataByHourDiff(dataRaw, 24, 48, spark)
    val dataRaw2 = getDataByHourDiff(dataRaw, 48, 72, spark)
    val dataRaw3 = getDataByHourDiff(dataRaw, 72, 96, spark)

    val data1 = dataRaw1
      .select("unitid", "conversion_goal", "media", "click", "cv2", "pre_cvr2")
      .withColumn("flag", when(col("cv2") >= minCV, 1).otherwise(0))
      .withColumn("hour_diff", lit(24))
      .select("unitid", "conversion_goal", "media", "click", "cv2", "pre_cvr2", "flag", "hour_diff")

    val data1Filter = data1
      .filter(s"flag = 0")
      .select("unitid", "conversion_goal", "media")

    val data2 = dataRaw2
      .join(data1Filter, Seq("unitid", "conversion_goal", "media"), "inner")
      .select("unitid", "conversion_goal", "media", "click", "cv2", "pre_cvr2")
      .withColumn("flag", when(col("cv2") >= minCV, 1).otherwise(0))
      .withColumn("hour_diff", lit(48))
      .select("unitid", "conversion_goal", "media", "click", "cv2", "pre_cvr2", "flag", "hour_diff")

    val data2Filter = data2
      .filter(s"flag = 0")
      .select("unitid", "conversion_goal", "media")

    val data3 = dataRaw3
      .join(data2Filter, Seq("unitid", "conversion_goal", "media"), "inner")
      .select("unitid", "conversion_goal", "media", "click", "cv2", "pre_cvr2")
      .withColumn("flag", when(col("cv2") >= minCV, 1).otherwise(0))
      .withColumn("hour_diff", lit(72))
      .select("unitid", "conversion_goal", "media", "click", "cv2", "pre_cvr2", "flag", "hour_diff")

    val data = data1.union(data2).union(data3)

    // get recall value
    val sqlRequest1 =
      s"""
         |SELECT
         |   conversion_goal,
         |   hour_diff,
         |   avg(value) as recall_value1
         |FROM
         |  dl_cpc.algo_recall_info_v1
         |WHERE
         |  version = 'v1'
         |GROUP BY conversion_goal, hour_diff
         |""".stripMargin
    println(sqlRequest1)
    val recallValue1 = spark.sql(sqlRequest1)

    val sqlRequest2 =
      s"""
         |SELECT
         |   conversion_goal,
         |   id as userid,
         |   hour_diff,
         |   avg(value) as recall_value2
         |FROM
         |  dl_cpc.algo_recall_info_v1
         |WHERE
         |  version = 'v_userid'
         |GROUP BY conversion_goal, id, hour_diff
         |""".stripMargin
    println(sqlRequest2)
    val recallValue2 = spark.sql(sqlRequest2)

    val unitInfoRaw = getConversionGoalNew(spark)
    val unitInfo = unitInfoRaw.select("unitid", "userid").distinct()

    val result = data
      .join(unitInfo, Seq("unitid"), "inner")
      .join(recallValue1, Seq("conversion_goal", "hour_diff"), "left_outer")
      .join(recallValue2, Seq("conversion_goal", "userid", "hour_diff"), "left_outer")
      .withColumn("recall_value", when(col("recall_value2").isNull, col("recall_value1")).otherwise(col("recall_value2")))
      .na.fill(1.0, Seq("recall_value"))
      .select("unitid", "conversion_goal", "media", "click", "cv2", "pre_cvr2", "flag", "hour_diff", "recall_value")
      .withColumn("cv2_recall", col("cv2") * col("recall_value"))
      .withColumn("tag", lit(2))
      .select("unitid", "conversion_goal", "media", "click", "cv2", "pre_cvr2", "cv2_recall", "tag")

//    result
//        .write.mode("overwrite").saveAsTable("test.ocpc_result_table20200102")

    result.createOrReplaceTempView("result_table")

    val sqlRequest3 =
      s"""
         |SELECT
         |  unitid,
         |  conversion_goal,
         |  media,
         |  sum(click) as click,
         |  sum(cv2) as cv2,
         |  sum(pre_cvr2 * click) * 1.0 / sum(click) as pre_cvr2,
         |  sum(cv2_recall) as cv2_recall,
         |  2 as tag
         |FROM
         |  result_table
         |GROUP BY unitid, conversion_goal, media
         |""".stripMargin
    println(sqlRequest3)
    val resultDF = spark
        .sql(sqlRequest3)
        .filter(s"cv2 >= $minCV")

    resultDF
  }


  def calculateCvrPart1(dataRaw: DataFrame, deepCvr: DataFrame, minCV: Int, spark: SparkSession) = {
    val data = getDataByHourDiff(dataRaw, 0, 24, spark)
    val result = data
      .join(deepCvr, Seq("unitid", "media"), "inner")
      .select("unitid", "conversion_goal", "media", "click", "cv1", "cv2", "pre_cvr2", "acb", "acp", "deep_cvr")
      .withColumn("cv2_recall", col("cv1") * col("deep_cvr"))
      .withColumn("tag", lit(1))
      .filter(s"cv1 >= $minCV")
      .select("unitid", "conversion_goal", "media", "click", "cv2", "pre_cvr2", "cv2_recall", "tag")

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
      .filter(s"cv2 >= 10")
      .withColumn("deep_cvr", col("cv2") * 1.0 / col("cv1"))

    val resultDF = data.select("unitid", "media", "deep_cvr")

    resultDF
  }


}

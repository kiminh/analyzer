package com.cpc.spark.oCPX.oCPC.calibration_by_tag

import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.oCPX.OcpcTools._
import com.cpc.spark.oCPX.cv_recall.shallow_cv.OcpcShallowCV_delay.getUserDelay
import com.cpc.spark.oCPX.cv_recall.shallow_cv.OcpcShallowCVrecall_predict.cvRecallPredict
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}


object OcpcGetPb_weightv6{
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()
    Logger.getRootLogger.setLevel(Level.WARN)

    val date = args(0).toString
    val hour = args(1).toString
    val date1 = args(2).toString
    val hour1 = args(3).toString
    val version = args(4).toString
    val expTag = args(5).toString

    // 主校准回溯时间长度
    val hourInt1 = args(6).toInt
    // 备用校准回溯时间长度
    val hourInt2 = args(7).toInt
    // 兜底校准时长
    val hourInt3 = args(8).toInt

    println("parameters:")
    println(s"date=$date, hour=$hour, date1=$date1, hour1=$hour1, version:$version, expTag:$expTag, hourInt1:$hourInt1, hourInt2:$hourInt2, hourInt3:$hourInt3")

    // 计费比系数模块
    val dataRaw = OcpcCalibrationBase(date1, hour1, hourInt3, spark).cache()
    dataRaw.show(10)
    val jfbDataRaw = OcpcJFBfactor(date1, hour1, expTag, dataRaw, hourInt1, hourInt2, hourInt3, spark)
    val jfbData = jfbDataRaw
      .withColumn("jfb_factor", lit(1.0) / col("jfb"))
      .select("unitid", "conversion_goal", "exp_tag", "jfb_factor")
      .cache()
    jfbData.show(10)

    // 校准系数模块
    val realtimeDataRaw = OcpcRealtimeCalibrationBase(date, hour, 100, spark).cache()
    realtimeDataRaw.show(10)
    val pcocDataRaw = OcpcCVRfactor(realtimeDataRaw, date, expTag, spark)
    val pcocData = pcocDataRaw
      .withColumn("cvr_factor", lit(1.0) / col("pcoc"))
      .select("unitid", "conversion_goal", "exp_tag", "cvr_factor")
      .cache()
    pcocData.show(10)

    val data = assemblyData(jfbData, pcocData, spark).cache()
    data.show(10)

    dataRaw.unpersist()

    // 明投单元
    val resultDF = data
      .withColumn("cpagiven", lit(1.0))
      .withColumn("is_hidden", lit(0))
      .withColumn("date", lit(date1))
      .withColumn("hour", lit(hour1))
      .withColumn("version", lit(version))
      .select("unitid", "conversion_goal", "jfb_factor", "post_cvr", "smooth_factor", "cvr_factor", "high_bid_factor", "low_bid_factor", "cpagiven", "date", "hour", "exp_tag", "is_hidden", "version")


    resultDF
      .repartition(1)
//      .write.mode("overwrite").insertInto("test.ocpc_pb_data_hourly")
      .write.mode("overwrite").insertInto("dl_cpc.ocpc_pb_data_hourly")


  }


  def assemblyData(jfbData: DataFrame, pcocData: DataFrame, spark: SparkSession) = {
    // 组装数据
    val data = jfbData
      .join(pcocData, Seq("unitid", "conversion_goal", "exp_tag"), "inner")
      .select("unitid", "conversion_goal", "exp_tag", "jfb_factor", "cvr_factor")
      .withColumn("high_bid_factor", lit(1.0))
      .withColumn("low_bid_factor", lit(1.0))
      .withColumn("post_cvr", lit(0.0))
      .withColumn("smooth_factor", lit(0.0))
      .na.fill(1.0, Seq("jfb_factor", "cvr_factor"))

    data
  }

  /*
  基础数据
   */
  def OcpcRealtimeCalibrationBase(date: String, hour: String, hourInt: Int, spark: SparkSession) = {
    val baseDataRaw = getBaseDataRealtime(hourInt, date, hour, spark)
    val baseData = baseDataRaw
      .withColumn("hour_diff", udfCalculateHourDiff(date, hour)(col("date"), col("hour"), lit(1)))

    baseData.createOrReplaceTempView("base_data")

    val sqlRequest =
      s"""
         |SELECT
         |  unitid,
         |  conversion_goal,
         |  media,
         |  date,
         |  hour,
         |  hour_diff,
         |  sum(click) as click,
         |  sum(cv) as cv,
         |  sum(pre_cvr * click) * 1.0 / sum(click) as pre_cvr
         |FROM
         |  base_data
         |GROUP BY unitid, conversion_goal, media, date, hour, hour_diff
         |""".stripMargin
    println(sqlRequest)
    val data  = spark.sql(sqlRequest)
      .select("unitid", "conversion_goal", "media", "click", "cv", "pre_cvr", "date", "hour", "hour_diff")

    // 预召回
    val recallValue1 = cvRecallPredictV1(date, spark)
    val recallValue2 = cvRecallPredictV2(date, spark)

    val unitUserInfoRaw = getConversionGoalNew(spark)
    val unitUserInfo = unitUserInfoRaw.select("unitid", "userid").distinct().cache()
    unitUserInfo.show(10)

    val recallData = data
      .join(unitUserInfo, Seq("unitid"), "inner")
      .join(recallValue1, Seq("conversion_goal", "date", "hour_diff"), "left_outer")
      .na.fill(1.0, Seq("recall_value1"))
      .join(recallValue2, Seq("userid", "conversion_goal", "date", "hour_diff"), "left_outer")
      .withColumn("recall_value", when(col("recall_value2").isNull, col("recall_value1")).otherwise(col("recall_value2")))
      .withColumn("cv_recall", col("cv") * col("recall_value"))
      .withColumn("cv_recall", when(col("cv_recall") > col("click"), col("click")).otherwise(col("cv_recall")))
      .cache()

    recallData.show(10)

    recallData
  }

  def OcpcCalibrationBase(date: String, hour: String, hourInt: Int, spark: SparkSession) = {
    /*
    动态计算alpha平滑系数
    1. 基于原始pcoc，计算预测cvr的量纲系数
    2. 二分搜索查找到合适的平滑系数
     */
    val baseDataRaw = getBaseData(hourInt, date, hour, spark)
    val baseData = baseDataRaw
      .withColumn("bid", udfCalculateBidWithHiddenTax()(col("date"), col("bid"), col("hidden_tax")))
      .withColumn("price", udfCalculatePriceWithHiddenTax()(col("price"), col("hidden_tax")))
      .withColumn("hour_diff", udfCalculateHourDiff(date, hour)(col("date"), col("hour"), lit(1)))

    // 计算结果
    val resultDF = calculateParameter(baseData, spark)

    resultDF
  }

  def udfCalculateHourDiff(date: String, hour: String) = udf((date1: String, hour1: String, conversionGoal: Int) => {
    // 取历史数据
    val dateConverter = new SimpleDateFormat("yyyy-MM-dd HH")

    val nowTime = dateConverter.parse(date + " " + hour)
    val ocpcTime = dateConverter.parse(date1 + " " + hour1)
    var hourDiff = (nowTime.getTime() - ocpcTime.getTime()) / (1000 * 60 * 60)

    if (conversionGoal == 2 || conversionGoal == 5) {
      hourDiff = hourDiff - 3
    }

    hourDiff
  })

  def calculateParameter(rawData: DataFrame, spark: SparkSession) = {
    val data  =rawData
      .filter(s"isclick=1")
      .groupBy("unitid", "conversion_goal", "media", "date", "hour", "hour_diff")
      .agg(
        sum(col("isclick")).alias("click"),
        sum(col("iscvr")).alias("cv"),
        avg(col("bid")).alias("acb"),
        avg(col("price")).alias("acp"),
        avg(col("exp_cvr")).alias("pre_cvr")
      )
      .withColumn("post_cvr", col("cv") * 1.0 / col("click"))
      .withColumn("pcoc", col("pre_cvr") * 1.0 / col("post_cvr"))
      .select("unitid", "conversion_goal", "media", "click", "cv", "pre_cvr", "post_cvr", "pcoc", "acb", "acp", "date", "hour", "hour_diff")

    data
  }

  def getDataByTimeSpan(dataRaw: DataFrame, date: String, hour: String, hourInt: Int, spark: SparkSession) = {
    dataRaw.createOrReplaceTempView("raw_data")

    // 取历史数据
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
         |    unitid,
         |    conversion_goal,
         |    media,
         |    sum(click) as click,
         |    sum(cv) as cv,
         |    sum(pre_cvr * click) * 1.0 / sum(click) as pre_cvr,
         |    sum(cv) * 1.0 / sum(click) as post_cvr,
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
      .withColumn("pcoc", col("pre_cvr") * 1.0 / col("post_cvr"))
      .select("unitid", "conversion_goal", "media", "click", "cv", "pre_cvr", "post_cvr", "pcoc", "acb", "acp")

    data
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
         |    sum(cv) as cv,
         |    sum(cv_recall) as cv_recall,
         |    sum(pre_cvr * click) * 1.0 / sum(click) as pre_cvr,
         |    sum(cv) * 1.0 / sum(click) as post_cvr,
         |    sum(cv_recall) * 1.0 / sum(click) as post_cvr_recall
         |FROM
         |    raw_data
         |WHERE
         |    $selectCondition
         |GROUP BY unitid, conversion_goal, media
         |""".stripMargin
    println(sqlRequest)
    val data = spark
      .sql(sqlRequest)
      .withColumn("pcoc", col("pre_cvr") * 1.0 / col("post_cvr_recall"))
      .select("unitid", "conversion_goal", "media", "click", "cv", "cv_recall", "pre_cvr", "post_cvr", "post_cvr_recall", "pcoc")

    data
  }

  /*
  校准件系数模块
   */
  def cvRecallPredictV1(date: String, spark: SparkSession) = {
    /*
    recall value by conversion_goal
     */
    val dateConverter = new SimpleDateFormat("yyyy-MM-dd")
    val today = dateConverter.parse(date)
    val calendar = Calendar.getInstance
    calendar.setTime(today)
    calendar.add(Calendar.DATE, -3)
    val yesterday = calendar.getTime
    val date1 = dateConverter.format(yesterday)

    val sqlRequest =
      s"""
         |SELECT
         |  conversion_goal,
         |  date_click,
         |  hour_diff,
         |  recall_ratio,
         |  date,
         |  row_number() over (partition by conversion_goal, date_click, hour_diff order by date desc) as seq
         |FROM
         |  dl_cpc.ocpc_cvr_pre_recall_ratio
         |WHERE
         |  date >= '$date1'
         |AND
         |  userid = 'all'
         |AND
         |  recall_ratio is not null
         |""".stripMargin
    println(sqlRequest)
    val data = spark
      .sql(sqlRequest)
      .filter(s"seq = 1 and conversion_goal in (2, 5)")
      .groupBy("conversion_goal", "date_click", "hour_diff")
      .agg(
        avg(col("recall_ratio")).alias("recall_ratio")
      )
      .withColumn("date", col("date_click"))
      .withColumn("recall_value1", lit(1) * 1.0 / col("recall_ratio"))
      .filter(s"recall_value1 is not null")
      .select("conversion_goal", "date", "hour_diff", "recall_value1")

    data
  }

  def cvRecallPredictV2(date: String, spark: SparkSession) = {
    /*
    recall value by conversion_goal
     */
    val dateConverter = new SimpleDateFormat("yyyy-MM-dd")
    val today = dateConverter.parse(date)
    val calendar = Calendar.getInstance
    calendar.setTime(today)
    calendar.add(Calendar.DATE, -3)
    val yesterday = calendar.getTime
    val date1 = dateConverter.format(yesterday)

    val sqlRequest =
      s"""
         |SELECT
         |  conversion_goal,
         |  cast(userid as int) as userid,
         |  date_click,
         |  hour_diff,
         |  recall_ratio,
         |  date,
         |  row_number() over (partition by conversion_goal, userid, date_click, hour_diff order by date desc) as seq
         |FROM
         |  dl_cpc.ocpc_cvr_pre_recall_ratio
         |WHERE
         |  date >= '$date1'
         |AND
         |  userid != 'all'
         |AND
         |  recall_ratio is not null
         |""".stripMargin
    println(sqlRequest)
    val data = spark
      .sql(sqlRequest)
      .filter(s"seq = 1 and conversion_goal in (2, 5)")
      .groupBy("conversion_goal", "userid", "date_click", "hour_diff")
      .agg(
        avg(col("recall_ratio")).alias("recall_ratio")
      )
      .withColumn("date", col("date_click"))
      .withColumn("recall_value2", lit(1) * 1.0 / col("recall_ratio"))
      .filter(s"recall_value2 is not null")
      .select("conversion_goal", "userid", "date", "hour_diff", "recall_value2")

    data
  }

  def OcpcCVRfactor(dataRaw: DataFrame, date: String, expTag: String, spark: SparkSession) = {
    // todo
    /*
    calculate the calibration value based on weighted calibration:
    case1: 0 ~ 5: 0.4
    case2: 0 ~ 12: 0.3
    case3: 0 ~ 24: 0.2
    case4: 0 ~ 48: 0.05
    case5: 0 ~ 84: 0.05

    use 80 as cv threshold
    if the cv < min_cv, rollback to the upper layer(case1 -> case2, etc.)
     */

    val dataRaw1 = getDataByHourDiff(dataRaw, 0, 6, spark)
    val data1 = dataRaw1
      .withColumn("media", udfMediaName()(col("media")))
      .withColumn("exp_tag", udfSetExpTag(expTag)(col("media")))
      .filter(s"cv >= 80")
    data1.show(10)

    val dataRaw2 = getDataByHourDiff(dataRaw, 0, 12, spark)
    val data2 = dataRaw2
      .withColumn("media", udfMediaName()(col("media")))
      .withColumn("exp_tag", udfSetExpTag(expTag)(col("media")))
      .filter(s"cv >= 80")
    data2.show(10)

    val dataRaw3 = getDataByHourDiff(dataRaw, 0, 24, spark)
    val data3 = dataRaw3
      .withColumn("media", udfMediaName()(col("media")))
      .withColumn("exp_tag", udfSetExpTag(expTag)(col("media")))
      .filter(s"cv >= 80")
    data3.show(10)

    val dataRaw4 = getDataByHourDiff(dataRaw, 0, 48, spark)
    val data4 = dataRaw4
      .withColumn("media", udfMediaName()(col("media")))
      .withColumn("exp_tag", udfSetExpTag(expTag)(col("media")))
      .filter(s"cv >= 80")
    data4.show(10)

    val dataRaw5 = getDataByHourDiff(dataRaw, 0, 84, spark)
    val data5 = dataRaw5
      .withColumn("media", udfMediaName()(col("media")))
      .withColumn("exp_tag", udfSetExpTag(expTag)(col("media")))
      .filter(s"cv >= 80")
    data5.show(10)


    // 计算最终值
    val calibration = calculateCalibrationValueCVR(data1, data2, data3, data4, data5, spark)

    val resultDF = calibration
      .select("unitid", "conversion_goal", "exp_tag", "pcoc")

    resultDF

  }

  def calculateCalibrationValueCVR(dataRaw1: DataFrame, dataRaw2: DataFrame, dataRaw3: DataFrame, dataRaw4: DataFrame, dataRaw5: DataFrame, spark: SparkSession) = {
    /*
    calculate the calibration value based on weighted calibration:
    case1: 0 ~ 5: 0.4
    case2: 0 ~ 12: 0.3
    case3: 0 ~ 24: 0.2
    case4: 0 ~ 48: 0.05
    case5: 0 ~ 84: 0.05

    pcoc = 0.4 * pcoc1 + 0.3 * pcoc2 + 0.2 * pcoc3 + 0.1 * pcoc4
     */
    // case1
    val data1 = dataRaw1
      .withColumn("pcoc1", col("pcoc"))
      .select("unitid", "conversion_goal", "exp_tag", "pcoc1")

    // case2
    val data2 = dataRaw2
      .withColumn("pcoc2", col("pcoc"))
      .select("unitid", "conversion_goal", "exp_tag", "pcoc2")

    // case3
    val data3 = dataRaw3
      .withColumn("pcoc3", col("pcoc"))
      .select("unitid", "conversion_goal", "exp_tag", "pcoc3")

    // case4
    val data4 = dataRaw4
      .withColumn("pcoc4", col("pcoc"))
      .select("unitid", "conversion_goal", "exp_tag", "pcoc4")

    // case5
    val data5 = dataRaw5
      .withColumn("pcoc5", col("pcoc"))
      .select("unitid", "conversion_goal", "exp_tag", "pcoc5")


    val baseData = data5
        .join(data4, Seq("unitid", "conversion_goal", "exp_tag"), "left_outer")
        .join(data3, Seq("unitid", "conversion_goal", "exp_tag"), "left_outer")
        .join(data2, Seq("unitid", "conversion_goal", "exp_tag"), "left_outer")
        .join(data1, Seq("unitid", "conversion_goal", "exp_tag"), "left_outer")
        .withColumn("pcoc4_old", col("pcoc4"))
        .withColumn("pcoc3_old", col("pcoc3"))
        .withColumn("pcoc2_old", col("pcoc2"))
        .withColumn("pcoc1_old", col("pcoc1"))
        .withColumn("pcoc4", when(col("pcoc4").isNull, col("pcoc5")).otherwise(col("pcoc4")))
        .withColumn("pcoc3", when(col("pcoc3").isNull, col("pcoc4")).otherwise(col("pcoc3")))
        .withColumn("pcoc2", when(col("pcoc2").isNull, col("pcoc3")).otherwise(col("pcoc2")))
        .withColumn("pcoc1", when(col("pcoc1").isNull, col("pcoc2")).otherwise(col("pcoc1")))

//    baseData
//        .write.mode("overwrite").saveAsTable("test.ocpc_check_data20191224a")
    baseData.createOrReplaceTempView("base_data")

    val sqlRequest =
      s"""
         |SELECT
         |  unitid,
         |  conversion_goal,
         |  exp_tag,
         |  pcoc1,
         |  pcoc2,
         |  pcoc3,
         |  pcoc4,
         |  pcoc5,
         |  (0.4 * pcoc1 + 0.3 * pcoc2 + 0.2 * pcoc3 + 0.05 * pcoc4 + 0.05 * pcoc5) as pcoc
         |FROM
         |  base_data
         |""".stripMargin
    println(sqlRequest)
    val data = spark.sql(sqlRequest)

    val resultDF = data.cache()

    resultDF.show()

    resultDF

  }

  /*
  计费比系数模块
   */
  def OcpcJFBfactor(date: String, hour: String, expTag: String, dataRaw: DataFrame, hourInt1: Int, hourInt2: Int, hourInt3: Int, spark: SparkSession) = {
    val dataRaw1 = getDataByTimeSpan(dataRaw, date, hour, hourInt1, spark)
    val data1 = dataRaw1
      .withColumn("jfb", col("acp") * 1.0 / col("acb"))
      .withColumn("media", udfMediaName()(col("media")))
      .withColumn("exp_tag", udfSetExpTag(expTag)(col("media")))
      .withColumn("min_cv", lit(80))
      .filter(s"cv > 0")
      .withColumn("priority", lit(1))
    data1.show(10)

    val dataRaw2 = getDataByTimeSpan(dataRaw, date, hour, hourInt2, spark)
    val data2 = dataRaw2
      .withColumn("jfb", col("acp") * 1.0 / col("acb"))
      .withColumn("media", udfMediaName()(col("media")))
      .withColumn("exp_tag", udfSetExpTag(expTag)(col("media")))
      .withColumn("min_cv", lit(80))
      .filter(s"cv > 0")
      .withColumn("priority", lit(2))
    data2.show(10)

    val dataRaw3 = getDataByTimeSpan(dataRaw, date, hour, hourInt3, spark)
    val data3 = dataRaw3
      .withColumn("jfb", col("acp") * 1.0 / col("acb"))
      .withColumn("media", udfMediaName()(col("media")))
      .withColumn("exp_tag", udfSetExpTag(expTag)(col("media")))
      .withColumn("min_cv", lit(80))
      .filter(s"cv > 0")
      .withColumn("priority", lit(3))
    data3.show(10)


    // 计算最终值
    val calibration = calculateCalibrationValueJFB(data1, data2, data3, spark)

    calibration.show(10)

    val resultDF = calibration
      .select("unitid", "conversion_goal", "exp_tag", "jfb")


    resultDF
  }


  def calculateCalibrationValueJFB(dataRaw1: DataFrame, dataRaw2: DataFrame, dataRaw3: DataFrame, spark: SparkSession) = {
    /*
    "identifier", "click", "cv", "pre_cvr", "total_price", "total_bid"
     */

    // 主校准模型
    val data1 = dataRaw1
      .filter(s"cv >= min_cv")
      .select("unitid", "conversion_goal", "exp_tag", "jfb", "priority")

    // 备用校准模型
    val data2 = dataRaw2
      .filter(s"cv >= min_cv")
      .select("unitid", "conversion_goal", "exp_tag", "jfb", "priority")

    // 兜底校准模型
    val data3 = dataRaw3
      .select("unitid", "conversion_goal", "exp_tag", "jfb", "priority")

    // 数据筛选
    val baseData = data1.union(data2).union(data3)
    baseData.createOrReplaceTempView("base_data")

    val sqlRequest =
      s"""
         |SELECT
         |  unitid,
         |  conversion_goal,
         |  exp_tag,
         |  jfb,
         |  priority,
         |  row_number() over(partition by unitid, conversion_goal, exp_tag order by priority) as seq
         |FROM
         |  base_data
         |""".stripMargin
    println(sqlRequest)
    val data = spark.sql(sqlRequest)

    val resultDF = data
        .filter(s"seq = 1")

    resultDF.show()

    resultDF

  }



}



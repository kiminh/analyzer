package com.cpc.spark.oCPX.oCPC.calibration_x.realtime.pid_calibration

import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.oCPX.OcpcTools._
import com.cpc.spark.oCPX.oCPC.calibration_all.OcpcBIDfactor.{calculateData1, calculateData2}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}


object OcpcGetPb_pidrealtime {
  /*
  新增部分媒体id采用暗投
   */
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()
    Logger.getRootLogger.setLevel(Level.WARN)

    val date = args(0).toString
    val hour = args(1).toString
    val date1 = args(2).toString
    val hour1 = args(3).toString
    val version = args(4).toString
    val expTag = args(5).toString
    val hourInt = args(6).toInt
    val minCV = args(7).toInt
    val kp = args(8).toDouble
    val ki = args(9).toDouble
    val kd = args(10).toDouble

    println("parameters:")
    println(s"date=$date, hour=$hour, date1:$date1, hour1:$hour1, version:$version, expTag:$expTag, hourInt:$hourInt, minCV:$minCV")

    // 计算jfb_factor,
    val jfbData = OcpcJFBfactor(date, hour, spark)

    // 计算pcoc
    val pidData = OcpcPIDfactor(date1, hour1, hourInt, 1, minCV, kp, ki, kd, spark)
    pidData
      .write.mode("overwrite").saveAsTable("test.check_ocpc_pid_data20191128")

    // 分段校准
    val bidFactor = OcpcBIDfactor(date, hour, version, expTag, 48, spark)

    val data = assemblyData(jfbData, pidData, bidFactor, expTag, spark)

    val result = data
      .select("identifier", "conversion_goal", "exp_tag", "jfb_factor", "post_cvr", "smooth_factor", "cvr_factor", "high_bid_factor", "low_bid_factor")
      .withColumn("cpagiven", lit(1.0))
      .withColumn("is_hidden", lit(0))
      .withColumn("date", lit(date))
      .withColumn("hour", lit(hour))
      .withColumn("version", lit(version))
      .selectExpr("identifier", "cast(identifier as int) unitid", "conversion_goal", "jfb_factor", "post_cvr", "smooth_factor", "cvr_factor", "high_bid_factor", "low_bid_factor", "cpagiven", "date", "hour", "exp_tag", "is_hidden", "version")

    val resultDF = result
      .select("unitid", "conversion_goal", "jfb_factor", "post_cvr", "smooth_factor", "cvr_factor", "high_bid_factor", "low_bid_factor", "cpagiven", "date", "hour", "exp_tag", "is_hidden", "version")

    resultDF
      .repartition(1)
      .write.mode("overwrite").insertInto("test.ocpc_pb_data_hourly_exp")
//      .write.mode("overwrite").insertInto("dl_cpc.ocpc_pb_data_hourly_exp")
//

  }

  def assemblyData(jfbData: DataFrame, pcocData: DataFrame, bidFactor: DataFrame, expTag: String, spark: SparkSession) = {
    // 组装数据
    val result = pcocData
      .join(jfbData, Seq("identifier", "conversion_goal", "media"), "inner")
      .withColumn("media", udfMediaName()(col("media")))
      .withColumn("exp_tag", udfSetExpTag(expTag)(col("media")))
      .join(bidFactor, Seq("identifier", "conversion_goal", "exp_tag"), "left_outer")
      .select("identifier", "conversion_goal", "exp_tag", "jfb_factor", "cvr_factor", "post_cvr", "high_bid_factor", "low_bid_factor")
      .na.fill(1.0, Seq("high_bid_factor", "low_bid_factor"))
      .na.fill(0.0, Seq("post_cvr"))
      .withColumn("smooth_factor", lit(0.0))

    result
  }

  /*
  分段校准
   */
  def OcpcBIDfactor(date: String, hour: String, version: String, expTag: String, hourInt: Int, spark:SparkSession) = {
    /*
    计算新版的cvr平滑策略：
    1. 抽取基础数据
    2. 计算该维度下pcoc与计费比、后验cvr等等指标
    3. 计算该维度下根据给定highBidFactor计算出的lowBidFactor
     */

    // 抽取基础数据
    val baseDataRaw = getBaseData(hourInt, date, hour, spark)
    baseDataRaw
      .withColumn("bid", udfCalculateBidWithHiddenTax()(col("date"), col("bid"), col("hidden_tax")))
      .withColumn("price", udfCalculatePriceWithHiddenTax()(col("price"), col("hidden_tax")))
      .createOrReplaceTempView("base_data_raw")

    val sqlRequest =
      s"""
         |SELECT
         |  *
         |FROM
         |  base_data_raw
       """.stripMargin
    println(sqlRequest)
    val baseData = spark
      .sql(sqlRequest)
      .selectExpr("searchid", "cast(unitid as string) identifier", "conversion_goal", "media", "isshow", "isclick", "iscvr", "bid", "price", "exp_cvr", "date", "hour")


    // 计算各维度下的pcoc、jfb以及后验cvr等指标
    val dataRaw1 = calculateData1(baseData, version, expTag, date, hour, spark)

    val data1 = dataRaw1
      .filter(s"cv >= min_cv")
      .cache()
    data1.show(10)


    // 计算该维度下根据给定highBidFactor计算出的lowBidFactor
    val baseData2 = baseData
      .withColumn("media", udfMediaName()(col("media")))
      .join(data1, Seq("identifier", "conversion_goal", "media"), "inner")

    val data2 = calculateData2(baseData2, date, hour, spark)

    val resultDF = data1
      .select("identifier", "conversion_goal", "media", "exp_tag", "cv", "min_cv", "post_cvr")
      .join(data2, Seq("identifier", "conversion_goal", "media"), "inner")
      .selectExpr("identifier", "conversion_goal", "exp_tag", "high_bid_factor", "low_bid_factor", "post_cvr", "cv", "min_cv")
      .withColumn("version", lit(version))

    resultDF

  }

  /*
  pid校准系数
   */
  def udfCalculatePID(kp: Double, ki: Double, kd: Double) = udf((currentError: Double, prevError: Double, lastError: Double) => {
    var result = kp * (currentError - prevError) + ki * currentError + kd * (currentError - 2.0 * prevError + lastError)
    result
  })

  def udfCalculateError() = udf((cpagiven: Double, cpareal: Double) => {
    val result = 1.0 - cpareal / cpagiven
    result
  })

  def calculateError(date: String, hour: String, hourInt: Int, minCV: Int, spark: SparkSession) = {
    val baseDataRaw = getRealtimeData(hourInt, date, hour, spark)

    // todo
    // filter the null value
    baseDataRaw
      .na.fill(0, Seq("hidden_tax"))
      .filter(s"bid_ocpc is not null") // todo
      .withColumn("price", udfCalculatePriceWithHiddenTax()(col("price"), col("hidden_tax")))
      .withColumn("cpagiven", col("bid_ocpc"))
      .withColumn("cvr_factor", col("price") / (col("cpagiven") * col("exp_cvr")))
      .createOrReplaceTempView("base_data")

    val sqlRequest1 =
      s"""
         |SELECT
         |  cast(unitid as string) as identifier,
         |  conversion_goal,
         |  media,
         |  sum(case when isclick=1 then price else 0 end) * 1.0 / sum(iscvr) as cpareal,
         |  sum(case when isclick=1 then cpagiven else 0 end) * 1.0 / sum(isclick) as cpagiven,
         |  sum(case when isclick=1 then cvr_factor else 0 end) * 1.0 / sum(isclick) as cvr_factor,
         |  sum(iscvr) as cv
         |FROM
         |  base_data
         |GROUP BY cast(unitid as string), conversion_goal, media
       """.stripMargin
    val data = spark.sql(sqlRequest1)

    val currentError = data
      .select("identifier", "conversion_goal", "media", "cpareal", "cpagiven", "cvr_factor", "cv")
      .filter(s"cv > $minCV")
      .withColumn("error", udfCalculateError()(col("cpagiven"), col("cpareal")))

    currentError
  }

  def OcpcPIDfactor(date: String, hour: String, hourInt: Int, hourDiff: Int, minCV: Int, kp: Double, ki: Double, kd: Double, spark: SparkSession) = {
    val currentErrorRaw = calculateError(date, hour, hourInt, minCV, spark)
    val currentError = currentErrorRaw
      .withColumn("current_error", col("error"))
      .withColumn("prev_cali", col("cvr_factor"))
      .select("identifier", "conversion_goal", "media", "prev_cali", "current_error")

    val dateConverter = new SimpleDateFormat("yyyy-MM-dd HH")
    val newDate = date + " " + hour
    val today = dateConverter.parse(newDate)
    val calendar = Calendar.getInstance
    calendar.setTime(today)
    calendar.add(Calendar.HOUR, -hourDiff)
    val yesterday1 = calendar.getTime
    val tmpDate1 = dateConverter.format(yesterday1)
    val tmpDateValue1 = tmpDate1.split(" ")
    val date1 = tmpDateValue1(0)
    val hour1 = tmpDateValue1(1)
    val prevErrorRaw = calculateError(date1, hour1, hourInt, minCV, spark)
    val prevError = prevErrorRaw
      .withColumn("prev_error", col("error"))
      .select("identifier", "conversion_goal", "media", "prev_error")

    calendar.add(Calendar.HOUR, -hourDiff)
    val yesterday2 = calendar.getTime
    val tmpDate2 = dateConverter.format(yesterday2)
    val tmpDateValue2 = tmpDate2.split(" ")
    val date2 = tmpDateValue2(0)
    val hour2 = tmpDateValue2(1)
    val lastErrorRaw = calculateError(date2, hour2, hourInt, minCV, spark)
    val lastError = lastErrorRaw
      .withColumn("last_error", col("error"))
      .select("identifier", "conversion_goal", "media", "last_error")


    val data = currentError
      .join(prevError, Seq("identifier", "conversion_goal", "media"), "inner")
      .join(lastError, Seq("identifier", "conversion_goal", "media"), "inner")
      .select("identifier", "conversion_goal", "media", "current_error", "prev_error", "last_error", "prev_cali")
      .na.fill(0, Seq("prev_cali", "prev_error", "last_error"))
      .withColumn("increment_value", udfCalculatePID(kp, ki, kd)(col("current_error"), col("prev_error"), col("last_error")))
      .withColumn("kp", lit(kp))
      .withColumn("ki", lit(ki))
      .withColumn("kd", lit(kd))
      .withColumn("current_cali", col("increment_value") + col("prev_cali"))
      .select("identifier", "conversion_goal", "media", "current_error", "prev_error", "last_error", "kp", "ki", "kd", "increment_value", "current_cali", "prev_cali")
      .withColumn("cvr_factor", col("current_cali"))

    data
  }


  /*
  计费比系数
   */
  def OcpcJFBfactor(date: String, hour: String, spark: SparkSession) = {
    val baseDataRaw = getBaseData(24, date, hour, spark)
    baseDataRaw
      .withColumn("bid", udfCalculateBidWithHiddenTax()(col("date"), col("bid"), col("hidden_tax")))
      .withColumn("price", udfCalculatePriceWithHiddenTax()(col("price"), col("hidden_tax")))
      .createOrReplaceTempView("base_data_raw")

    val sqlRequest =
      s"""
         |SELECT
         |  cast(unitid as string) identifier,
         |  userid,
         |  conversion_goal,
         |  media,
         |  sum(isclick) as click,
         |  avg(bid) as acb,
         |  avg(price) as acp
         |FROM
         |  base_data_raw
         |WHERE
         |  isclick=1
         |GROUP BY cast(unitid as string), userid, conversion_goal, media
       """.stripMargin
    println(sqlRequest)
    val baseData = spark
      .sql(sqlRequest)
      .select("identifier", "userid", "conversion_goal", "media", "click", "acb", "acp")
      .withColumn("jfb_factor", col("acb") * 1.0 / col("acp") )

    val resultDF = baseData
      .select("identifier", "userid", "conversion_goal", "media", "click", "acb", "acp", "jfb_factor")
      .withColumn("jfb_factor", udfCheckBound(1.0, 3.0)(col("jfb_factor")))
      .filter(s"jfb_factor is not null")

    resultDF
  }

  def udfCheckBound(minValue: Double, maxValue: Double) = udf((jfbFactor: Double) => {
    var result = math.min(math.max(minValue, jfbFactor), maxValue)
    result
  })


}



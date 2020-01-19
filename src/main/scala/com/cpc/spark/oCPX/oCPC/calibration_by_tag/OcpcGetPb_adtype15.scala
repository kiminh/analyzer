package com.cpc.spark.oCPX.oCPC.calibration_by_tag

import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.oCPX.OcpcTools.{getBaseData, getBaseDataDelay, getTimeRangeSqlDate, udfCalculateBidWithHiddenTax, udfCalculatePriceWithHiddenTax, udfMediaName, udfSetExpTag}
import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}


object OcpcGetPb_adtype15 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()
    Logger.getRootLogger.setLevel(Level.WARN)

    val date = args(0).toString
    val hour = args(1).toString
    val version = args(2).toString
    val expTag = args(3).toString

    // 主校准回溯时间长度
    val hourInt1 = args(4).toInt
    // 备用校准回溯时间长度
    val hourInt2 = args(5).toInt
    // 兜底校准时长
    val hourInt3 = args(6).toInt

    println("parameters:")
    println(s"date=$date, hour=$hour, version:$version, expTag:$expTag, hourInt1:$hourInt1, hourInt2:$hourInt2, hourInt3:$hourInt3")

    // 基础数据
    val dataRaw = OcpcCalibrationBase(date, hour, hourInt3, spark).cache()
    dataRaw.show(10)

    // 计费比系数模块
    val jfbDataRaw = OcpcJFBfactor(date, hour, expTag, dataRaw, hourInt1, hourInt2, hourInt3, spark)
    val jfbData = jfbDataRaw
      .withColumn("jfb_factor", lit(1.0) / col("jfb"))
      .select("unitid", "conversion_goal", "exp_tag", "jfb_factor")
      .cache()
    jfbData.show(10)

    // 校准系数模块
    val pcocDataRaw = OcpcCVRfactor(date, hour, expTag, dataRaw, hourInt1, hourInt2, hourInt3, spark)
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
      .withColumn("date", lit(date))
      .withColumn("hour", lit(hour))
      .withColumn("version", lit(version))
      .select("unitid", "conversion_goal", "jfb_factor", "post_cvr", "smooth_factor", "cvr_factor", "high_bid_factor", "low_bid_factor", "cpagiven", "date", "hour", "exp_tag", "is_hidden", "version")


    resultDF
      .repartition(1)
//      .write.mode("overwrite").insertInto("test.ocpc_pb_data_hourly_exp")
      .write.mode("overwrite").insertInto("dl_cpc.ocpc_pb_data_hourly_exp")


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
  def OcpcCalibrationBase(date: String, hour: String, hourInt: Int, spark: SparkSession) = {
    /*
    动态计算alpha平滑系数
    1. 基于原始pcoc，计算预测cvr的量纲系数
    2. 二分搜索查找到合适的平滑系数
     */
    val baseDataRaw = getBaseDataDelay(hourInt, date, hour, spark)
    val baseData = baseDataRaw
      .withColumn("bid", udfCalculateBidWithHiddenTax()(col("date"), col("bid"), col("hidden_tax")))
      .withColumn("price", udfCalculatePriceWithHiddenTax()(col("price"), col("hidden_tax")))

    // 计算结果
    val resultDF = calculateParameter(baseData, spark)

    resultDF
  }

  def calculateParameter(rawData: DataFrame, spark: SparkSession) = {
    val data  =rawData
      .filter(s"isclick=1")
      .groupBy("unitid", "conversion_goal", "media", "date", "hour")
      .agg(
        sum(col("isclick")).alias("click"),
        sum(col("iscvr")).alias("cv"),
        avg(col("bid")).alias("acb"),
        avg(col("price")).alias("acp"),
        avg(col("exp_cvr")).alias("pre_cvr")
      )
      .withColumn("post_cvr", col("cv") * 1.0 / col("click"))
      .withColumn("pcoc", col("pre_cvr") * 1.0 / col("post_cvr"))
      .select("unitid", "conversion_goal", "media", "click", "cv", "pre_cvr", "post_cvr", "pcoc", "acb", "acp", "date", "hour")

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

  /*
  校准件系数模块
   */
  def OcpcCVRfactor(date: String, hour: String, expTag: String, dataRaw: DataFrame, hourInt1: Int, hourInt2: Int, hourInt3: Int, spark: SparkSession) = {
    // cvr实验配置文件
    // min_cv:配置文件中如果为负数或空缺，则用默认值40，其他情况使用设定值
    val expConf = getCvrExpConf(spark)

    val dataRaw1 = getDataByTimeSpan(dataRaw, date, hour, hourInt1, spark)
    val data1 = dataRaw1
      .withColumn("media", udfMediaName()(col("media")))
      .withColumn("exp_tag", udfSetExpTag(expTag)(col("media")))
      .join(expConf, Seq("conversion_goal", "exp_tag"), "left_outer")
      .na.fill(40, Seq("min_cv"))
      .filter(s"cv >= 80")
      .withColumn("priority", lit(1))
    data1.show(10)

    val dataRaw2 = getDataByTimeSpan(dataRaw, date, hour, hourInt2, spark)
    val data2 = dataRaw2
      .withColumn("media", udfMediaName()(col("media")))
      .withColumn("exp_tag", udfSetExpTag(expTag)(col("media")))
      .join(expConf, Seq("conversion_goal", "exp_tag"), "left_outer")
      .na.fill(40, Seq("min_cv"))
      .filter(s"cv >= 80")
      .withColumn("priority", lit(2))
    data2.show(10)

    val dataRaw3 = getDataByTimeSpan(dataRaw, date, hour, hourInt3, spark)
    val data3 = dataRaw3
      .withColumn("media", udfMediaName()(col("media")))
      .withColumn("exp_tag", udfSetExpTag(expTag)(col("media")))
      .join(expConf, Seq("conversion_goal", "exp_tag"), "left_outer")
      .na.fill(40, Seq("min_cv"))
      .filter(s"cv >= 80")
      .withColumn("priority", lit(3))
    data3.show(10)

    // 计算最终值
    val calibration = calculateCalibrationValueCVR(data1, data2, data3, spark)

    calibration.show(10)

    val resultDF = calibration
      .select("unitid", "conversion_goal", "exp_tag", "pcoc")

    resultDF

  }

  def getCvrExpConf(spark: SparkSession) ={
    // 从配置文件读取数据
    val conf = ConfigFactory.load("ocpc")
    val confPath = conf.getString("exp_config_v2.cvr_factor")
    val rawData = spark.read.format("json").json(confPath)
    val data = rawData
      .groupBy("exp_tag", "conversion_goal")
      .agg(min(col("min_cv")).alias("min_cv"))
      .distinct()

    println("cvr factor config:")
    data.show(10)

    data
  }

  def calculateCalibrationValueCVR(dataRaw1: DataFrame, dataRaw2: DataFrame, dataRaw3: DataFrame, spark: SparkSession) = {
    // 主校准模型
    val data1 = dataRaw1
      .filter(s"cv >= min_cv")
      .select("unitid", "conversion_goal", "exp_tag", "pcoc", "priority")

    // 备用校准模型
    val data2 = dataRaw2
      .filter(s"cv >= min_cv")
      .select("unitid", "conversion_goal", "exp_tag", "pcoc", "priority")

    // 兜底校准模型
    val data3 = dataRaw3
      .select("unitid", "conversion_goal", "exp_tag", "pcoc", "priority")

    // 数据筛选
    val baseData = data1.union(data2).union(data3)
    baseData.createOrReplaceTempView("base_data")

    val sqlRequest =
      s"""
         |SELECT
         |  unitid,
         |  conversion_goal,
         |  exp_tag,
         |  pcoc,
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

  /*
  计费比系数模块
   */
  def OcpcJFBfactor(date: String, hour: String, expTag: String, dataRaw: DataFrame, hourInt1: Int, hourInt2: Int, hourInt3: Int, spark: SparkSession) = {
    // smooth实验配置文件
    // min_cv:配置文件中如果为负数或空缺，则用默认值0，其他情况使用设定值
    // smooth_factor：配置文件中如果为负数或空缺，则用默认值(由udfSelectSmoothFactor函数决定)，其他情况使用设定值
    val expConf = getJfbExpConf(spark)

    val dataRaw1 = getDataByTimeSpan(dataRaw, date, hour, hourInt1, spark)
    val data1 = dataRaw1
      .withColumn("jfb", col("acp") * 1.0 / col("acb"))
      .withColumn("media", udfMediaName()(col("media")))
      .withColumn("exp_tag", udfSetExpTag(expTag)(col("media")))
      .join(expConf, Seq("conversion_goal", "exp_tag"), "left_outer")
      .na.fill(0, Seq("min_cv"))
      .withColumn("min_cv", udfSetMinCV()(col("min_cv")))
      .filter(s"cv > 0")
      .withColumn("priority", lit(1))
    data1.show(10)

    val dataRaw2 = getDataByTimeSpan(dataRaw, date, hour, hourInt2, spark)
    val data2 = dataRaw2
      .withColumn("jfb", col("acp") * 1.0 / col("acb"))
      .withColumn("media", udfMediaName()(col("media")))
      .withColumn("exp_tag", udfSetExpTag(expTag)(col("media")))
      .join(expConf, Seq("conversion_goal", "exp_tag"), "left_outer")
      .na.fill(0, Seq("min_cv"))
      .withColumn("min_cv", udfSetMinCV()(col("min_cv")))
      .filter(s"cv > 0")
      .withColumn("priority", lit(2))
    data2.show(10)

    val dataRaw3 = getDataByTimeSpan(dataRaw, date, hour, hourInt3, spark)
    val data3 = dataRaw3
      .withColumn("jfb", col("acp") * 1.0 / col("acb"))
      .withColumn("media", udfMediaName()(col("media")))
      .withColumn("exp_tag", udfSetExpTag(expTag)(col("media")))
      .join(expConf, Seq("conversion_goal", "exp_tag"), "left_outer")
      .na.fill(0, Seq("min_cv"))
      .withColumn("min_cv", udfSetMinCV()(col("min_cv")))
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

  def getJfbExpConf(spark: SparkSession) ={
    // 从配置文件读取数据
    val conf = ConfigFactory.load("ocpc")
    val confPath = conf.getString("exp_config_v2.jfb_factor")
    val rawData = spark.read.format("json").json(confPath)
    val data = rawData
      .groupBy("exp_tag", "conversion_goal")
      .agg(min(col("min_cv")).alias("min_cv"))
      .distinct()

    println("jfb factor: config")
    data.show(10)

    data
  }

  def udfSetMinCV() = udf((minCV: Int) => {
    var result = minCV
    if (result < 0) {
      result = 0
    }
    result
  })

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



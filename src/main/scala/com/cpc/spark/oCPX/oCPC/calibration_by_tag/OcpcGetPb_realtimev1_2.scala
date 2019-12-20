package com.cpc.spark.oCPX.oCPC.calibration_by_tag

import com.cpc.spark.oCPX.OcpcTools._
import com.cpc.spark.oCPX.oCPC.calibration_all.OcpcBIDfactor.{calculateData1, calculateData2}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import com.cpc.spark.oCPX.oCPC.calibration_by_tag.OcpcGetPb_adtype15.{OcpcCalibrationBase, getDataByTimeSpan}

object OcpcGetPb_realtimev1_2 {
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

    // 主校准回溯时间长度
    val hourInt1 = 24 //args(4).toInt
    // 备用校准回溯时间长度
    val hourInt2 = 48 //args(5).toInt
    // 兜底校准时长
    val hourInt3 = 84 //args(6).toInt

    println("parameters:")
    println(s"date=$date, hour=$hour, date1:$date1, hour1:$hour1, version:$version, expTag:$expTag, hourInt:$hourInt, minCV:$minCV")

    // 基础数据
    val dataRaw = OcpcCalibrationBase(date, hour, hourInt3, spark).cache()
    dataRaw.show(10)

    // 计算jfb_factor,
    val jfbData = OcpcJFBfactor(date, hour, spark)

    // 计算pcoc
    val pcocDataRaw = OcpcCVRfactor(date1, hour1, hourInt, minCV, spark)
    val pcocData = pcocDataRaw
      .selectExpr("identifier", "conversion_goal", "media", "cvr_factor as cvr_factor2" )
      .cache()
    pcocData.show(10)

    // 计算pcoc_base
    val pcocData_baseRaw = OcpcCVRfactor_base(date1, hour1, expTag, dataRaw, hourInt1, hourInt2, hourInt3, spark)
    val pcocData_base = pcocData_baseRaw
      .withColumn("cvr_factor1", lit(1.0) / col("pcoc"))
      .selectExpr("cast(unitid as string) identifier", "conversion_goal", "media", "cvr_factor1")
      .cache()
    pcocData_base.show(10)


    // 数据关联
    val pcocData_final = pcocData
      .join(pcocData_base, Seq("identifier", "conversion_goal", "media" ), "outer")
      .withColumn("cvr_factor", when(col("cvr_factor2").isNull, col("cvr_factor1")).otherwise(col("cvr_factor2")))

    val pcocData_finalDF = pcocData_final
      .select("identifier", "conversion_goal", "media", "cvr_factor", "cvr_factor1", "cvr_factor2")

    pcocData_finalDF
      .repartition(1)
      .write.mode("overwrite").insertInto("test.pcocData_final_20191219")

    // 分段校准
    val bidFactor = OcpcBIDfactor(date, hour, version, expTag, 48, spark)

    val data = assemblyData(jfbData, pcocData_final, bidFactor, expTag, spark)

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
  cvr校准系数
   */
  def OcpcCVRfactor(date: String, hour: String, hourInt: Int, minCV: Int, spark: SparkSession) = {
    val baseDataRaw = getRealtimeData(24, date, hour, spark)
    baseDataRaw.createOrReplaceTempView("base_data_raw")

    val sqlRequest =
      s"""
         |SELECT
         |  cast(unitid as string) identifier,
         |  userid,
         |  conversion_goal,
         |  media,
         |  sum(exp_cvr) as total_pre_cvr,
         |  sum(isclick) as click,
         |  sum(iscvr) as cv
         |FROM
         |  base_data_raw
         |WHERE
         |  isclick=1
         |GROUP BY cast(unitid as string), userid, conversion_goal, media
       """.stripMargin
    println(sqlRequest)

    val baseData = spark
      .sql(sqlRequest)
      .select("identifier", "userid", "conversion_goal", "media", "total_pre_cvr", "click", "cv")
      .withColumn("pre_cvr", col("total_pre_cvr") * 1.0 / col("click"))
      .withColumn("post_cvr", col("cv") * 1.0 / col("click"))
      .withColumn("cvr_factor", col("post_cvr") * 1.0 / col("pre_cvr"))

    val resultDF = baseData
      .select("identifier", "userid", "conversion_goal", "media", "total_pre_cvr", "click", "cv", "cvr_factor")
      .withColumn("cvr_factor", udfCheckBound(0.5, 2.0)(col("cvr_factor")))
      .filter(s"cv > $minCV and cvr_factor is not null")

    resultDF
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

  /*
校准件系数模块
 */
  def OcpcCVRfactor_base(date: String, hour: String, expTag: String, dataRaw: DataFrame, hourInt1: Int, hourInt2: Int, hourInt3: Int, spark: SparkSession) = {
    // cvr实验配置文件
    // min_cv:配置文件中如果为负数或空缺，则用默认值40，其他情况使用设定值
    //val expConf = getCvrExpConf(spark)

    val dataRaw1 = getDataByTimeSpan(dataRaw, date, hour, hourInt1, spark)
    val data1 = dataRaw1
      .withColumn("min_cv", lit(80))
      .filter(s"cv > 0")
      .withColumn("priority", lit(1))
    data1.show(10)

    val dataRaw2 = getDataByTimeSpan(dataRaw, date, hour, hourInt2, spark)
    val data2 = dataRaw2
      .withColumn("min_cv", lit(80))
      .filter(s"cv > 0")
      .withColumn("priority", lit(2))
    data2.show(10)

    val dataRaw3 = getDataByTimeSpan(dataRaw, date, hour, hourInt3, spark)
    val data3 = dataRaw3
      .withColumn("min_cv", lit(80))
      .filter(s"cv > 0")
      .withColumn("priority", lit(3))
    data3.show(10)

    // 计算最终值
    //val calibration = calculateCalibrationValueCVR(data1, data2, data3, spark)
    val calibration = calculateCalibrationValueCVR(data1, data2, data3, spark)

    calibration.show(10)

    val resultDF = calibration
      .select("unitid", "conversion_goal", "media", "pcoc")

    resultDF

  }


  def calculateCalibrationValueCVR(dataRaw1: DataFrame, dataRaw2: DataFrame, dataRaw3: DataFrame, spark: SparkSession) = {
    // 主校准模型
    val data1 = dataRaw1
      .filter(s"cv >= min_cv")
      .select("unitid", "conversion_goal", "media", "pcoc", "priority")

    // 备用校准模型
    val data2 = dataRaw2
      .filter(s"cv >= min_cv")
      .select("unitid", "conversion_goal", "media", "pcoc", "priority")

    // 兜底校准模型
    val data3 = dataRaw3
      .select("unitid", "conversion_goal", "media", "pcoc", "priority")

    // 数据筛选
    val baseData = data1.union(data2).union(data3)
    baseData.createOrReplaceTempView("base_data")

    val sqlRequest =
      s"""
         |SELECT
         |  unitid,
         |  conversion_goal,
         |  media,
         |  pcoc,
         |  priority,
         |  row_number() over(partition by unitid, conversion_goal, media order by priority) as seq
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



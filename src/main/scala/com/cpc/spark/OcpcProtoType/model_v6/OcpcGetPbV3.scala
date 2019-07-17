package com.cpc.spark.OcpcProtoType.model_v6

import com.cpc.spark.OcpcProtoType.OcpcTools.getConversionGoal
import com.cpc.spark.OcpcProtoType.model_v6.OcpcCalculateCalibration.OcpcCalculateCalibrationMain
import com.cpc.spark.OcpcProtoType.model_v6.OcpcGetPbWeishiV2.OcpcGetPbWeishiMain
import com.cpc.spark.OcpcProtoType.model_v6.OcpcRangeCalibrationV2.OcpcRangeCalibrationMain
import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}


object OcpcGetPbV3 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()
    Logger.getRootLogger.setLevel(Level.WARN)

    val date = args(0).toString
    val hour = args(1).toString
    val version = args(2).toString
    val media = args(3).toString
    val hourInt = args(4).toInt
    val minCV = args(5).toInt
    val expTag = args(6).toString
    val isHidden = 0

    // 主校准回溯时间长度
    val hourInt1 = args(7).toInt
    // 备用校准回溯时间长度
    val hourInt2 = args(8).toInt
    // 兜底校准时长
    val hourInt3 = args(9).toInt

    println("parameters:")
    println(s"date=$date, hour=$hour, version:$version, media:$media, hourInt:$hourInt, minCV:$minCV, expTag:$expTag, hourInt1:$hourInt1, hourInt2:$hourInt2, hourInt3:$hourInt3")

    val calibraionData = OcpcCalculateCalibrationMain(date, hour, version, media, minCV, hourInt1, hourInt2, hourInt3, spark)
    val factorData = OcpcRangeCalibrationMain(date, hour, version, media, hourInt, 40, expTag, spark)

    val result1 = calibraionData
      .join(factorData.select("identifier", "conversion_goal", "high_bid_factor", "low_bid_factor"), Seq("identifier", "conversion_goal"), "left_outer")
      .na.fill(1.0, Seq("high_bid_factor", "low_bid_factor"))
      .withColumn("cpagiven", lit(1.0))
      .cache()

    val result2 = OcpcGetPbWeishiMain(date, hour, version, media, expTag, hourInt, minCV, hourInt1, hourInt2, hourInt3, spark)

    val resultDF = selectWeishiCali(expTag, result1, result2, date, hour, spark)


    resultDF.show(10)
    resultDF
      .select("identifier", "pcoc", "jfb", "post_cvr", "high_bid_factor", "low_bid_factor", "cpagiven", "conversion_goal")
      .withColumn("is_hidden", lit(isHidden))
      .withColumn("exp_tag", lit(expTag))
      .withColumn("date", lit(date))
      .withColumn("hour", lit(hour))
      .withColumn("version", lit(version))
      .select("identifier", "pcoc", "jfb", "post_cvr", "high_bid_factor", "low_bid_factor", "cpagiven", "is_hidden", "exp_tag", "conversion_goal", "date", "hour", "version")
      .repartition(5)
//      .write.mode("overwrite").insertInto("test.ocpc_param_calibration_hourly_v2")
      .write.mode("overwrite").insertInto("dl_cpc.ocpc_param_calibration_hourly_v2")


    println("successfully save data into hive")

  }

  def selectWeishiCali(expTag: String, dataRaw1: DataFrame, dataRaw2: DataFrame, date: String, hour: String, spark: SparkSession) = {
    // 获取oCPC单元中userid与unitid的映射关系
    val useridToUnitid = getConversionGoal(date, hour, spark)
    val useridUnitid = useridToUnitid
      .selectExpr("cast(unitid as string) identifier", "userid")
      .select("identifier", "userid")
      .distinct()

    // 从配置文件获取需要特殊化配置的广告主id（微视广告主）
    val conf = ConfigFactory.load("ocpc")
    val confPath = conf.getString("exp_tag.weishi")
    val rawData = spark.read.format("json").json(confPath)
    rawData.show(10)
    val confData = rawData
      .filter(s"exp_tag = '$expTag'")
      .select("userid")
      .distinct()

    val flagData = useridUnitid
      .join(confData, Seq("userid"), "inner")
      .select("identifier", "userid")
      .distinct()
    flagData.show(10)

    val data2 = dataRaw2
      .join(flagData, Seq("identifier"), "inner")
      .filter(s"conversion_goal = 2")
      .withColumn("pcoc_bak", col("pcoc"))
      .withColumn("jfb_bak", col("jfb"))
      .withColumn("post_cvr_bak", col("post_cvr"))
      .withColumn("high_bid_factor_bak", col("high_bid_factor"))
      .withColumn("low_bid_factor_bak", col("low_bid_factor"))
      .withColumn("flag", lit(1))
      .select("identifier", "pcoc_bak", "jfb_bak", "post_cvr_bak", "high_bid_factor_bak", "low_bid_factor_bak", "flag", "conversion_goal")

    val data1 = dataRaw1
      .withColumn("pcoc_orig", col("pcoc"))
      .withColumn("jfb_orig", col("jfb"))
      .withColumn("post_cvr_orig", col("post_cvr"))
      .withColumn("high_bid_factor_orig", col("high_bid_factor"))
      .withColumn("low_bid_factor_orig", col("low_bid_factor"))
      .select("identifier", "pcoc_orig", "jfb_orig", "post_cvr_orig", "high_bid_factor_orig", "low_bid_factor_orig", "cpagiven", "conversion_goal")

    val data = data1
      .join(data2, Seq("identifier", "conversion_goal"), "left_outer")
      .na.fill(0, Seq("pcoc_bak", "jfb_bak", "post_cvr_bak", "high_bid_factor_bak", "low_bid_factor_bak", "flag"))
      .withColumn("pcoc", udfSelectValue()(col("flag"), col("pcoc_orig"), col("pcoc_bak")))
      .withColumn("jfb", udfSelectValue()(col("flag"), col("jfb_orig"), col("jfb_bak")))
      .withColumn("post_cvr", udfSelectValue()(col("flag"), col("post_cvr_orig"), col("post_cvr_bak")))
      .withColumn("high_bid_factor", udfSelectValue()(col("flag"), col("high_bid_factor_orig"), col("high_bid_factor_bak")))
      .withColumn("low_bid_factor", udfSelectValue()(col("flag"), col("low_bid_factor_orig"), col("low_bid_factor_bak")))
      .cache()

    data.show(10)

    val result = data
      .select("identifier", "pcoc", "jfb", "post_cvr", "high_bid_factor", "low_bid_factor", "cpagiven", "conversion_goal")

    result


  }

  def udfSelectValue() = udf((flag: Int, valueOrigin: Double, valueBak: Double) => {
    var result = valueOrigin
    if (flag == 1) {
      result = valueBak
    } else {
      result = valueOrigin
    }
    result
  })


}



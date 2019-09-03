package com.cpc.spark.oCPX.oCPC.calibration_alltype

import com.cpc.spark.oCPX.OcpcTools._
import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}


object OcpcMergeDelayData {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()
    Logger.getRootLogger.setLevel(Level.WARN)

    val date0 = args(0).toString
    val hour0 = args(1).toString
    val date1 = args(2).toString
    val hour1 = args(3).toString
    val version = args(4).toString
    val expTag = args(5).toString

    println("parameters:")
    println(s"date0=$date0, hour0=$hour0, date1=$date1, hour1=$hour1, version:$version, expTag:$expTag")

    val data0 = getData(date0, hour0, version, expTag, spark)
    val data1 = getData(date1, hour1, version, expTag, spark)

    val date = date0
    val hour = hour0
    val data = selectWeishiCali(expTag, data0, data1, date, hour, spark)
    data
      .repartition(10)
      .write.mode("overwrite").saveAsTable("test.check_ocpc_merge_delay20190806")


    val resultDF = data
      .withColumn("date", lit(date))
      .withColumn("hour", lit(hour))
      .withColumn("version", lit(version))
      .select("identifier", "conversion_goal", "jfb_factor", "post_cvr", "smooth_factor", "cvr_factor", "high_bid_factor", "low_bid_factor", "cpagiven", "date", "hour", "exp_tag", "is_hidden", "version")
      .cache()
    resultDF.show(10)

    resultDF
      .repartition(1)
//      .write.mode("overwrite").insertInto("test.ocpc_pb_data_hourly_alltype")
      .write.mode("overwrite").insertInto("dl_cpc.ocpc_pb_data_hourly_alltype")


  }

  def getExpTags(expTag: String, spark: SparkSession) = {
    val qtt = expTag + "Qtt"
    val midu = expTag + "MiDu"
    val hottopic = expTag + "HT66"
    val result = s"exp_tag in ('$qtt', '$midu', '$hottopic')"
    result
  }

  def getData(date: String, hour: String, version: String, expTag: String, spark: SparkSession) = {
    val expTagSelection = getExpTags(expTag, spark)
    val sqlRequest =
      s"""
         |SELECT
         |  *,
         |  cast(split(identifier, '&')[0] as int) unitid
         |FROM
         |  dl_cpc.ocpc_pb_data_hourly_exp_alltype
         |WHERE
         |  `date` = '$date'
         |AND
         |  `hour` = '$hour'
         |AND
         |  version = '$version'
         |AND
         |  $expTagSelection
       """.stripMargin
    println(sqlRequest)
    val data = spark.sql(sqlRequest).cache()
    data.show(10)
    data
  }

  def selectWeishiCali(expTag: String, dataRaw1: DataFrame, dataRaw2: DataFrame, date: String, hour: String, spark: SparkSession) = {
    // 获取oCPC单元中userid与unitid的映射关系
    val useridToUnitid = getConversionGoal(date, hour, spark)
    val useridUnitid = useridToUnitid
      .select("unitid", "userid")
      .distinct()

    // 从配置文件获取需要特殊化配置的广告主id（微视广告主）
    val conf = ConfigFactory.load("ocpc")
    val confPath = conf.getString("exp_tag.weishi")
    val rawData = spark.read.format("json").json(confPath)
    val confData = rawData
      .select("userid", "exp_tag")
      .distinct()

    val flagData = useridUnitid
      .join(confData, Seq("userid"), "inner")
      .select("unitid", "userid", "exp_tag")
      .distinct()

    val data2 = dataRaw2
      .join(flagData, Seq("unitid", "exp_tag"), "inner")
      .filter(s"conversion_goal = 2")
      .withColumn("cvr_factor_bak", col("cvr_factor"))
      .withColumn("jfb_factor_bak", col("jfb_factor"))
      .withColumn("post_cvr_bak", col("post_cvr"))
      .withColumn("high_bid_factor_bak", col("high_bid_factor"))
      .withColumn("low_bid_factor_bak", col("low_bid_factor"))
      .withColumn("flag", lit(1))
      .select("identifier", "cvr_factor_bak", "jfb_factor_bak", "post_cvr_bak", "high_bid_factor_bak", "low_bid_factor_bak", "flag", "conversion_goal", "exp_tag", "is_hidden", "unitid")

    println("weishi data")
    data2.show(10)

    val data1 = dataRaw1
      .withColumn("cvr_factor_orig", col("cvr_factor"))
      .withColumn("jfb_factor_orig", col("jfb_factor"))
      .withColumn("post_cvr_orig", col("post_cvr"))
      .withColumn("high_bid_factor_orig", col("high_bid_factor"))
      .withColumn("low_bid_factor_orig", col("low_bid_factor"))
      .select("identifier", "cvr_factor_orig", "jfb_factor_orig", "post_cvr_orig", "high_bid_factor_orig", "low_bid_factor_orig", "conversion_goal", "exp_tag", "is_hidden", "smooth_factor", "cpagiven")
    println("complete data")
    data1.show(10)

    val data = data1
      .join(data2, Seq("identifier", "conversion_goal", "exp_tag", "is_hidden"), "left_outer")
      .na.fill(0, Seq("cvr_factor_bak", "jfb_factor_bak", "post_cvr_bak", "high_bid_factor_bak", "low_bid_factor_bak", "flag"))
      .withColumn("cvr_factor", udfSelectValue()(col("flag"), col("cvr_factor_orig"), col("cvr_factor_bak")))
      .withColumn("jfb_factor", udfSelectValue()(col("flag"), col("jfb_factor_orig"), col("jfb_factor_bak")))
      .withColumn("post_cvr", udfSelectValue()(col("flag"), col("post_cvr_orig"), col("post_cvr_bak")))
      .withColumn("high_bid_factor", udfSelectValue()(col("flag"), col("high_bid_factor_orig"), col("high_bid_factor_bak")))
      .withColumn("low_bid_factor", udfSelectValue()(col("flag"), col("low_bid_factor_orig"), col("low_bid_factor_bak")))

    val result = data
      .select("identifier", "cvr_factor", "jfb_factor", "post_cvr", "high_bid_factor", "low_bid_factor", "conversion_goal", "exp_tag", "smooth_factor", "is_hidden", "cpagiven", "unitid")

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



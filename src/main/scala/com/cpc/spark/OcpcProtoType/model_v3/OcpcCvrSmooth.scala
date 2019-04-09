package com.cpc.spark.OcpcProtoType.model_v3

import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.ocpc.OcpcUtils.getTimeRangeSql2
import com.cpc.spark.udfs.Udfs_wj._
import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import com.cpc.spark.OcpcProtoType.model_v3.OcpcSmoothFactor.{getBaseData, calculatePCOC}
import scala.collection.mutable.ListBuffer
import ocpcCpcBid.ocpccpcbid.{OcpcCpcBidList, SingleOcpcCpcBid}
import java.io.FileOutputStream

object OcpcCvrSmooth {
  def main(args: Array[String]): Unit = {
    /*
    计算重构后的cvr平滑数据:
    程序：
      ocpcV3.ocpc.filter.OcpcCvrSmooth
      ocpcV3.ocpc.filter.OcpcCPCbidV2
      identifier      string  NULL
      min_bid double  NULL
      cvr1    double  NULL
      cvr2    double  NULL
      cvr3    double  NULL
      min_cpm double  NULL
      factor1 double  NULL
      factor2 double  NULL
      factor3 double  NULL
      cpc_bid double  NULL
      cpa_suggest     double  NULL
      param_t double  NULL
      cali_value      double  NULL
     */
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()
    Logger.getRootLogger.setLevel(Level.WARN)

    val date = args(0).toString
    val hour = args(1).toString
    val media = args(2).toString
    val version = args(3).toString

    val conf = ConfigFactory.load("ocpc")
    val expDataPath = conf.getString("ocpc_all.ocpc_cpcbid.path_v2")
    val fileName = conf.getString("ocpc_all.ocpc_cpcbid.pbfile_v2")
    val smoothDataPath = conf.getString("ocpc_all.ocpc_cpcbid.factor_path")
    val suggestCpaPath = conf.getString("ocpc_all.ocpc_cpcbid.suggestcpa_path")
    println(s"cpcBid path is: $expDataPath")
    println(s"fileName is: $fileName")
    println(s"smooth factor path is $smoothDataPath")
    println(s"suggest cpa path is $suggestCpaPath")

    println("parameters:")
    println(s"date=$date, hour=$hour, media:$media, version:$version")

    // 获取postcvr数据
//    val cv1Data = getBaseData(media, "cvr1", hourInt, date, hour, spark)
//    val cv2Data = getBaseData(media, "cvr2", hourInt, date, hour, spark)
//    val cv3Data = getBaseData(media, "cvr3", hourInt, date, hour, spark)
    val cvr1 = getPostCvr(1, date, hour, spark)
    val cvr2 = getPostCvr(2, date, hour, spark)
    val cvr3 = getPostCvr(3, date, hour, spark)
//    val cvr1 = calculatePCOC(cv1Data, spark).withColumn("cvr1", col("post_cvr"))
//    val cvr2 = calculatePCOC(cv2Data, spark).withColumn("cvr2", col("post_cvr"))
//    val cvr3 = calculatePCOC(cv3Data, spark).withColumn("cvr3", col("post_cvr"))
    val cvrData = cvr1
      .join(cvr2, Seq("identifier"), "outer")
      .join(cvr3, Seq("identifier"), "outer")
      .select("identifier", "cvr1", "cvr2", "cvr3")
      .na.fill(0.0, Seq("cvr1", "cvr2", "cvr3"))

    // 获取factor数据
    val factorData = getCvrAlphaData(smoothDataPath, date, hour, spark)

    // 获取cpc_bid数据
    val expData = getCpcBidData(expDataPath, date, hour, spark)

    // 获取cpa_suggest和param_t数据
    val suggestCPA = getCPAsuggestV2(suggestCpaPath, date, hour, spark)

    // 获取cali_value
    val caliValue = getCaliValue(date, hour, spark)

    // 组装数据
    val result = assemblyData(cvrData, factorData, expData, suggestCPA, caliValue, spark)

    val resultDF = result
      .withColumn("date", lit(date))
      .withColumn("hour", lit(hour))
      .withColumn("version", lit("qtt_demo"))

    resultDF
//            .repartition(10).write.mode("overwrite").saveAsTable("test.ocpc_post_cvr_unitid_hourly20190304")
      .repartition(10).write.mode("overwrite").insertInto("dl_cpc.ocpc_post_cvr_unitid_hourly")

    savePbPack(resultDF, fileName)

  }

  def getPostCvr(conversionGoal: Int, date: String, hour: String, spark: SparkSession) = {
    val cvrType = "cvr" + conversionGoal.toString
    val sqlRequest =
      s"""
         |SELECT
         |  identifier,
         |  post_cvr
         |FROM
         |  dl_cpc.ocpc_pcoc_jfb_hourly
         |WHERE
         |  `date` = '$date'
         |AND
         |  `hour` = '$hour'
         |AND
         |  version in ('qtt_demo', 'qtt_hidden')
         |AND
         |  conversion_goal = $conversionGoal
       """.stripMargin
    println(sqlRequest)
    val data = spark.sql(sqlRequest)

    val resultDF = data
      .select("identifier", "post_cvr")
      .groupBy("identifier")
      .agg(avg(col("post_cvr")).alias(cvrType))
      .select("identifier", cvrType)

    resultDF
  }

  def savePbPack(dataset: DataFrame, filename: String): Unit = {
    var list = new ListBuffer[SingleOcpcCpcBid]
    println("size of the dataframe")
    val resultData = dataset.selectExpr("identifier", "cast(cpc_bid as double) cpc_bid", "cast(min_bid as double) min_bid", "cvr1", "cvr2", "cvr3", "cast(min_cpm as double) as min_cpm", "factor1", "factor2", "factor3", "cast(cpa_suggest as double) cpa_suggest", "cast(param_t as double) param_t", "cast(cali_value as double) cali_value")
    println(resultData.count)
    resultData.show(10)
    resultData.printSchema()
    var cnt = 0

    for (record <- resultData.collect()) {
      val unit_id = record.getAs[String]("identifier").toLong
      val cpc_bid = record.getAs[Double]("cpc_bid")
      val min_bid = record.getAs[Double]("min_bid")
      val post_cvr1 = record.getAs[Double]("cvr1")
      val post_cvr2 = record.getAs[Double]("cvr2")
      val post_cvr3 = record.getAs[Double]("cvr3")
      val min_cpm = record.getAs[Double]("min_cpm").toLong
      val factor1 = record.getAs[Double]("factor1")
      val factor2 = record.getAs[Double]("factor2")
      val factor3 = record.getAs[Double]("factor3")
      val cpa_suggest = record.getAs[Double]("cpa_suggest")
      val param_t = record.getAs[Double]("param_t")
      val caliValue = record.getAs[Double]("cali_value")


      if (cnt % 100 == 0) {
        println(s"unit_id:$unit_id, cpc_bid:$cpc_bid, post_cvr1:$post_cvr1, post_cvr2:$post_cvr2, post_cvr3:$post_cvr3, min_cpm:$min_cpm, factor1:$factor1, factor2:$factor2, factor3:$factor3, min_bid:$min_bid, cpa_suggest:$cpa_suggest, param_t:$param_t, caliValue:$caliValue")
      }
      cnt += 1


      cnt += 1
      val currentItem = SingleOcpcCpcBid(
        unitid = unit_id,
        cpcBid = cpc_bid,
        cvGoal1PostCvr = post_cvr1,
        cvGoal2PostCvr = post_cvr2,
        cvGoal3PostCvr = post_cvr3,
        minCpm = min_cpm,
        cvGoal1Smooth = factor1,
        cvGoal2Smooth = factor2,
        cvGoal3Smooth = factor3,
        minBid = min_bid,
        cpaSuggest = cpa_suggest,
        paramT = param_t,
        cvrCalFactor = caliValue
      )
      list += currentItem

    }

    val result = list.toArray[SingleOcpcCpcBid]
    val adRecordList = OcpcCpcBidList(
      adrecord = result
    )

    println("length of the array")
    println(result.length)
    adRecordList.writeTo(new FileOutputStream(filename))

    println("complete save data into protobuffer")

  }


  def assemblyData(cvrData: DataFrame, factorData: DataFrame, expData: DataFrame, suggestCPA: DataFrame, caliValue: DataFrame, spark: SparkSession) = {
    /*
      identifier      string  NULL
      min_bid double  NULL
      cvr1    double  NULL
      cvr2    double  NULL
      cvr3    double  NULL
      min_cpm double  NULL
      factor1 double  NULL
      factor2 double  NULL
      factor3 double  NULL
      cpc_bid double  NULL
      cpa_suggest     double  NULL
      param_t double  NULL
      cali_value      double  NULL
     */
    val result = cvrData
      .join(factorData, Seq("identifier"), "outer")
      .join(expData, Seq("identifier"), "outer")
      .join(suggestCPA, Seq("identifier"), "outer")
      .join(caliValue, Seq("identifier"), "left_outer")
      .select("identifier", "cvr1", "cvr2", "cvr3", "factor1", "factor2", "factor3", "cpc_bid", "cpa_suggest", "param_t", "cali_value")
      .withColumn("min_bid", lit(0))
      .withColumn("min_cpm", lit(0))
      .na.fill(0, Seq("min_bid", "min_cpm", "cpc_bid", "cpa_suggest", "param_t"))
      .na.fill(0.0, Seq("cvr1", "cvr2", "cvr3"))
      .na.fill(0.2, Seq("factor1"))
      .na.fill(0.5, Seq("factor2", "factor3"))
      .na.fill(1.0, Seq("cali_value"))
      .selectExpr("identifier", "cast(min_bid as double) min_bid", "cvr1", "cvr2", "cvr3", "cast(min_cpm as double) as min_cpm", "cast(factor1 as double) factor1", "cast(factor2 as double) as factor2", "cast(factor3 as double) factor3", "cast(cpc_bid as double) cpc_bid", "cpa_suggest", "param_t", "cali_value")

    result
  }

  def getCaliValue(date: String, hour: String, spark: SparkSession) = {
    val sqlRequest =
      s"""
         |SELECT
         |  identifier,
         |  1.0 / pcoc as cali_value,
         |  jfb,
         |  kvalue,
         |  conversion_goal,
         |  version
         |FROM
         |  dl_cpc.ocpc_kvalue_smooth_strat
         |WHERE
         |  `date` = '$date'
         |AND
         |  `hour` = '$hour'
         |AND
         |  version in ('qtt_hidden', 'qtt_demo')
       """.stripMargin
    println(sqlRequest)
    val rawData = spark.sql(sqlRequest)
    val result = rawData
      .select("identifier", "conversion_goal", "cali_value")
      .groupBy("identifier", "conversion_goal")
      .agg(avg(col("cali_value")).alias("cali_value"))
      .select("identifier", "cali_value")

    result
  }


  def getConversionGoal(date: String, hour: String, spark: SparkSession) = {
    val url = "jdbc:mysql://rr-2zehhy0xn8833n2u5.mysql.rds.aliyuncs.com:3306/adv?useUnicode=true&characterEncoding=utf-8"
    val user = "adv_live_read"
    val passwd = "seJzIPUc7xU"
    val driver = "com.mysql.jdbc.Driver"
    val table = "(select id, user_id, ideas, bid, ocpc_bid, ocpc_bid_update_time, cast(conversion_goal as char) as conversion_goal, status from adv.unit where ideas is not null and is_ocpc=1) as tmp"

    val data = spark.read.format("jdbc")
      .option("url", url)
      .option("driver", driver)
      .option("user", user)
      .option("password", passwd)
      .option("dbtable", table)
      .load()

    val resultDF = data
      .withColumn("unitid", col("id"))
      .withColumn("userid", col("user_id"))
      .select("unitid",  "conversion_goal")
      .distinct()

    resultDF.show(10)
    resultDF
  }

  def getCPAsuggestV2(suggestCpaPath: String, date: String, hour: String, spark: SparkSession) = {
    /*
    两条来源：
    1. 从mysql读取实时正在跑ocpc的广告单元和对应转化目标，按照unitid和conversion_goal设置对应推荐cpa
    2. 从配置文件读取推荐cpa
    3. 数据关联，优先配置文件的推荐cpa
     */

    // 从推荐cpa表中读取：dl_cpc.ocpc_suggest_cpa_k_once
    val sqlRequest =
      s"""
         |SELECT
         |  cast(identifier as int) unitid,
         |  conversion_goal,
         |  cpa_suggest as cpa_suggest2,
         |  duration
         |FROM
         |  dl_cpc.ocpc_suggest_cpa_k_once
       """.stripMargin
    println(sqlRequest)
    val data1 = spark
      .sql(sqlRequest)
      .filter(s"duration <= 15")
      .withColumn("param_t2", lit(10))
      .select("unitid", "conversion_goal", "cpa_suggest2", "param_t2")

    val cvGoal = getConversionGoal(date, hour, spark)
    val suggestData = data1
      .join(cvGoal, Seq("unitid", "conversion_goal"), "inner")
      .selectExpr("cast(unitid as string) identifier", "cpa_suggest2", "param_t2")


    // 从配置文件读取数据
    val data2 = spark.read.format("json").json(suggestCpaPath)
    val confData = data2
      .groupBy("identifier")
      .agg(
        min(col("cpa_suggest")).alias("cpa_suggest1"),
        min(col("param_t")).alias("param_t1")
      )
      .select("identifier", "cpa_suggest1", "param_t1")

    // 数据关联：优先配置文件
    val data = suggestData
      .join(confData, Seq("identifier"), "outer")
      .selectExpr("identifier", "cpa_suggest1", "param_t1", "cpa_suggest2", "param_t2")
      .withColumn("cpa_suggest", when(col("cpa_suggest1").isNotNull, col("cpa_suggest1")).otherwise(col("cpa_suggest2")))
      .withColumn("param_t", when(col("param_t1").isNotNull, col("param_t1")).otherwise(col("param_t2")))

    data.show(10)

    val resultDF = data.select("identifier", "cpa_suggest", "param_t")

    resultDF
  }


  def getCpcBidData(dataPath: String, date: String, hour: String, spark: SparkSession) = {
    val data = spark.read.format("json").json(dataPath)

    val resultDF = data
      .groupBy("identifier")
      .agg(
        min(col("cpc_bid")).alias("cpc_bid")
      )
      .select("identifier", "cpc_bid")

    resultDF.show(10)
    resultDF
  }

  def getCvrAlphaData(dataPath: String, date: String, hour: String, spark: SparkSession) = {
    val data = spark.read.format("json").json(dataPath)

    val resultDF = data
      .groupBy("unitid")
      .agg(
        min(col("factor1")).alias("factor1"),
        min(col("factor2")).alias("factor2"),
        min(col("factor3")).alias("factor3")
      )
      .withColumn("identifier", col("unitid"))
      .selectExpr("cast(identifier as string) identifier", "factor1", "factor2", "factor3")

    resultDF.show(10)
    resultDF
  }


}

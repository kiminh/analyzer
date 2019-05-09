package com.cpc.spark.OcpcProtoType.model_novel_v2

import java.io.FileOutputStream

import com.typesafe.config.ConfigFactory
import ocpcnovelv2.ocpcnovelv2.{OcpcCpcBidList, SingleOcpcCpcBid}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable.ListBuffer

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
    val fileName = "ocpc_novel_v2.pb"

    println("parameters:")
    println(s"date=$date, hour=$hour, media:$media, version:$version")

    // 获取postcvr数据
    val cvr1 = getPostCvr(version, 1, date, hour, spark)
    val cvr2 = getPostCvr(version, 2, date, hour, spark)
    val cvr3 = getPostCvr(version, 3, date, hour, spark)
    val cvr4 = getPostCvr(version, 4, date, hour, spark)

    val cvrData = cvr1
      .join(cvr2, Seq("identifier"), "outer")
      .join(cvr3, Seq("identifier"), "outer")
      .join(cvr4, Seq("identifier"), "outer")
      .select("identifier", "cvr1", "cvr2", "cvr3", "cvr4")
      .na.fill(0.0, Seq("cvr1", "cvr2", "cvr3", "cvr4"))

    // 获取cali_value
    val caliValue = getCaliValue(version, date, hour, spark)

    // 组装数据
    val result = assemblyData(cvrData, caliValue, spark)

    val resultDF = result
      .withColumn("date", lit(date))
      .withColumn("hour", lit(hour))
      .withColumn("version", lit(version))

    resultDF
      //      .repartition(10).write.mode("overwrite").saveAsTable("test.ocpc_post_cvr_unitid_hourly20190304")
      .repartition(10).write.mode("overwrite").insertInto("dl_cpc.ocpc_post_cvr_unitid_hourly_novel")

    savePbPack(resultDF, fileName)

  }

  def getPostCvr(version: String, conversionGoal: Int, date: String, hour: String, spark: SparkSession) = {
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
         |  version = '$version'
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
    val resultData = dataset.selectExpr("identifier", "cast(cpc_bid as double) cpc_bid", "cast(min_bid as double) min_bid", "cvr1", "cvr2", "cvr3", "cvr4", "cast(min_cpm as double) as min_cpm", "factor1", "factor2", "factor3", "factor4", "cast(cpa_suggest as double) cpa_suggest", "cast(param_t as double) param_t", "cast(cali_value as double) cali_value")
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
      val post_cvr4 = record.getAs[Double]("cvr4")
      val min_cpm = record.getAs[Double]("min_cpm").toLong
      val factor1 = record.getAs[Double]("factor1")
      val factor2 = record.getAs[Double]("factor2")
      val factor3 = record.getAs[Double]("factor3")
      val factor4 = record.getAs[Double]("factor4")
      val cpa_suggest = record.getAs[Double]("cpa_suggest")
      val param_t = record.getAs[Double]("param_t")
      val caliValue = record.getAs[Double]("cali_value")


      if (cnt % 100 == 0) {
        println(s"unit_id:$unit_id, cpc_bid:$cpc_bid, post_cvr1:$post_cvr1, post_cvr2:$post_cvr2, post_cvr3:$post_cvr3, post_cvr4:$post_cvr4,min_cpm:$min_cpm, factor1:$factor1, factor2:$factor2, factor3:$factor3, factor4:$factor4, min_bid:$min_bid, cpa_suggest:$cpa_suggest, param_t:$param_t, caliValue:$caliValue")
      }
      cnt += 1


      cnt += 1
      val currentItem = SingleOcpcCpcBid(
        unitid = unit_id,
        cpcBid = cpc_bid,
        cvGoal1PostCvr = post_cvr1,
        cvGoal2PostCvr = post_cvr2,
        cvGoal3PostCvr = post_cvr3,
        cvGoal4PostCvr = post_cvr4,
        minCpm = min_cpm,
        cvGoal1Smooth = factor1,
        cvGoal2Smooth = factor2,
        cvGoal3Smooth = factor3,
        cvGoal4Smooth = factor4,
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

  def assemblyData(cvrData: DataFrame, caliValue: DataFrame, spark: SparkSession) = {
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
    val data = cvrData
      .withColumn("factor1", lit(0.5))
      .withColumn("factor2", lit(0.5))
      .withColumn("factor3", lit(0.5))
      .withColumn("factor4", lit(0.5))
      .withColumn("cpc_bid", lit(0))
      .withColumn("cpa_suggest", lit(0))
      .withColumn("param_t", lit(0))
      .join(caliValue, Seq("identifier"), "left_outer")
      .select("identifier", "cvr1", "cvr2", "cvr3","cvr4", "factor1", "factor2", "factor3", "factor4","cpc_bid", "cpa_suggest", "param_t", "cali_value")
      .withColumn("min_bid", lit(0))
      .withColumn("min_cpm", lit(0))
      .na.fill(0, Seq("min_bid", "min_cpm", "cpc_bid", "cpa_suggest", "param_t"))
      .na.fill(0.0, Seq("cvr1", "cvr2", "cvr3", "cvr4"))
      .na.fill(1.0, Seq("cali_value"))
      .selectExpr("identifier", "cast(min_bid as double) min_bid", "cvr1", "cvr2", "cvr3","cvr4", "cast(min_cpm as double) as min_cpm", "cast(factor1 as double) factor1", "cast(factor2 as double) as factor2", "cast(factor3 as double) factor3", "cast(factor4 as double) factor4","cast(cpc_bid as double) cpc_bid", "cpa_suggest", "param_t", "cali_value")

    // 如果cali_value在1/1.3到1.3之间，则factor变成0.2
    val result = data
      .withColumn("factor3", udfSetFactor3()(col("factor3"), col("cali_value")))

    result
  }

  def udfSetFactor3() = udf((factor3: Double, caliValue: Double) => {
    var factor = 0.5
    if (caliValue <= 1.3 && caliValue >= 0.769) {
      factor = 0.2
    } else if (caliValue <= 2.0 && caliValue >= 0.5) {
      factor = 0.35
    } else {
      factor = 0.5
    }
    factor
  })

  def getCaliValue(version: String, date: String, hour: String, spark: SparkSession) = {
    val sqlRequest =
      s"""
         |SELECT
         |  identifier,
         |  1.0 / pcoc as cali_value,
         |  jfb,
         |  kvalue,
         |  conversion_goal,
         |  version,
         |  row_number() over (partition by identifier order by conversion_goal) as seq
         |FROM
         |  dl_cpc.ocpc_kvalue_smooth_strat
         |WHERE
         |  `date` = '$date'
         |AND
         |  `hour` = '$hour'
         |AND
         |  version = '$version'
         |AND
         |  pcoc is not null
         |AND
         |  jfb is not null
       """.stripMargin
    println(sqlRequest)
    val rawData = spark.sql(sqlRequest)
      .filter(s"seq = 1")
      .select("identifier", "conversion_goal", "cali_value")

    val result = rawData
      .groupBy("identifier")
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

}

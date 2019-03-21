package com.cpc.spark.ocpcV3.ocpc.filter

import java.io.FileOutputStream
import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.ocpcV3.ocpc.OcpcUtils._
import com.cpc.spark.udfs.Udfs_wj.udfStringToMap
import com.typesafe.config.ConfigFactory

import ocpcCpcBid.ocpccpcbid.{OcpcCpcBidList, SingleOcpcCpcBid}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._

import scala.collection.mutable.ListBuffer


object OcpcCPCbidV2 {
  def main(args: Array[String]): Unit = {
    // 计算日期周期
    val date = args(0).toString
    val hour = args(1).toString

    // spark app name
//    scp ocpc_cpc_bid_v2.pb cpc@192.168.80.23:/home/cpc/model_server/data/ocpc_cpc_bid_v2.pb
    val spark = SparkSession.builder().appName(s"OcpcMinBid: $date, $hour").enableHiveSupport().getOrCreate()

    val conf = ConfigFactory.load("ocpc")
    val expDataPath = conf.getString("ocpc_all.ocpc_cpcbid.path_v2")
    val fileName = conf.getString("ocpc_all.ocpc_cpcbid.pbfile_v2")
    val smoothDataPath = conf.getString("ocpc_all.ocpc_cpcbid.factor_path")
    val suggestCpaPath = conf.getString("ocpc_all.ocpc_cpcbid.suggestcpa_path")
    println(s"cpcBid path is: $expDataPath")
    println(s"fileName is: $fileName")
    println(s"smooth factor path is $smoothDataPath")
    println(s"suggest cpa path is $suggestCpaPath")

    val expData = getCpcBidData(expDataPath, date, hour, spark)
    val cvrData = getCvrData(date, hour, spark)
    val cpmData = getCpmData(date, hour, spark)
    val cvrAlphaData = getCvrAlphaData(smoothDataPath, date, hour, spark)
    val suggestCPA = getCPAsuggestV2(suggestCpaPath, date, hour, spark)

    val data = expData
      .join(cvrData, Seq("identifier"), "outer")
      .join(cpmData, Seq("identifier"), "outer")
      .join(cvrAlphaData, Seq("identifier"), "left_outer")
      .join(suggestCPA, Seq("identifier"), "left_outer")
      .select("identifier", "cpc_bid", "cvr1", "cvr2", "cvr3", "min_bid", "min_cpm", "factor1", "factor2", "factor3", "cpa_suggest", "param_t")
      .withColumn("cvr1", when(col("identifier") === "270", 0.5).otherwise(col("cvr1")))
      .withColumn("cvr2", when(col("identifier") === "270", 0.5).otherwise(col("cvr2")))
      .withColumn("cvr3", when(col("identifier") === "270", 0.5).otherwise(col("cvr3")))
      .na.fill(0, Seq("min_bid", "cvr1", "cvr2", "cvr3", "min_cpm", "cpc_bid", "cpa_suggest", "param_t"))
      .na.fill(0.2, Seq("factor1", "factor2", "factor3"))


    val resultDF = data
      .selectExpr("identifier", "cast(min_bid as double) min_bid", "cvr1", "cvr2", "cvr3", "cast(min_cpm as double) as min_cpm", "cast(factor1 as double) factor1", "cast(factor2 as double) as factor2", "cast(factor3 as double) factor3", "cast(cpc_bid as double) cpc_bid", "cpa_suggest", "param_t")
      .withColumn("date", lit(date))
      .withColumn("hour", lit(hour))
      .withColumn("version", lit("qtt_demo"))
//      .repartition(10).write.mode("overwrite").saveAsTable("test.ocpc_post_cvr_unitid_hourly20190304")
      .repartition(10).write.mode("overwrite").insertInto("dl_cpc.ocpc_post_cvr_unitid_hourly")

    savePbPack(data, fileName)
  }

  def getConversionGoal(date: String, hour: String, spark: SparkSession) = {
    val url = "jdbc:mysql://rr-2zehhy0xn8833n2u5.mysql.rds.aliyuncs.com:3306/adv?useUnicode=true&characterEncoding=utf-8"
    val user = "adv_live_read"
    val passwd = "seJzIPUc7xU"
    val driver = "com.mysql.jdbc.Driver"
    val table = "(select id, user_id, ideas, bid, ocpc_bid, ocpc_bid_update_time, cast(conversion_goal as char) as conversion_goal, status from adv.unit where ideas is not null) as tmp"

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
      .filter(s"duration <= 3")
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

  def getCPAsuggest(suggestCpaPath: String, date: String, hour: String, spark: SparkSession) = {
    val data = spark.read.format("json").json(suggestCpaPath)

    val resultDF = data
      .groupBy("identifier")
      .agg(
        min(col("cpa_suggest")).alias("cpa_suggest"),
        min(col("param_t")).alias("param_t")
      )
      .select("identifier", "cpa_suggest", "param_t")

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
      .select("identifier", "factor1", "factor2", "factor3")

    resultDF.show(10)
    resultDF
  }

  def getCpmData(date: String, hour: String, spark: SparkSession) = {
//    val data = spark
//        .table("test.ocpc_pcvr_smooth_cpm")
//        .select("identifier", "min_bid", "min_cpm")
//    1854873
//    1702796
//    1875122


    val sqlRequest =
      s"""
         |SELECT
         |  identifier,
         |  (case when identifier in (1854873, 1702796, 1875122) then 5 else 0 end)  as min_bid,
         |  0 as min_cpm
         |FROM
         |  test.ocpc_pcvr_smooth_cpm
       """.stripMargin
    println(sqlRequest)
    val data = spark.sql(sqlRequest)

    data
  }

  def getCvrData(date: String, hour: String, spark: SparkSession) = {
    val selectCondition = s"`date` = '$date' and `hour` = '$hour' and version = 'qtt_demo'"
    val tableName = "dl_cpc.ocpc_pcvr_smooth_hourly"
    val sqlRequest =
      s"""
         |SELECT
         |  identifier,
         |  cvr1,
         |  cvr2,
         |  cvr3
         |FROM
         |  $tableName
         |WHERE
         |  $selectCondition
       """.stripMargin
    println(sqlRequest)
    val data = spark.sql(sqlRequest)
    data

  }

  def savePbPack(dataset: DataFrame, filename: String): Unit = {
    var list = new ListBuffer[SingleOcpcCpcBid]
    println("size of the dataframe")
    val resultData = dataset.selectExpr("identifier", "cast(cpc_bid as double) cpc_bid", "cast(min_bid as double) min_bid", "cvr1", "cvr2", "cvr3", "cast(min_cpm as double) as min_cpm", "factor1", "factor2", "factor3", "cast(cpa_suggest as double) cpa_suggest", "cast(param_t as double) param_t")
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


      if (cnt % 100 == 0) {
        println(s"unit_id:$unit_id, cpc_bid:$cpc_bid, post_cvr1:$post_cvr1, post_cvr2:$post_cvr2, post_cvr3:$post_cvr3, min_cpm:$min_cpm, factor1:$factor1, factor2:$factor2, factor3:$factor3, min_bid:$min_bid, cpa_suggest:$cpa_suggest, param_t:$param_t")
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
        paramT = param_t
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

  def getCpcBidData(expDataPath: String, date: String, hour: String, spark: SparkSession) = {
    val data = spark.read.format("json").json(expDataPath)

    val resultDF = data
      .groupBy("identifier")
      .agg(
        min(col("cpc_bid")).alias("cpc_bid")
      )
      .select("identifier", "cpc_bid")

    resultDF.show(10)
    resultDF

  }

}

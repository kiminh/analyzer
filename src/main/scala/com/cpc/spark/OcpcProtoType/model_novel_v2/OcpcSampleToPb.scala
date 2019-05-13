package com.cpc.spark.OcpcProtoType.model_novel_v2

import java.io.FileOutputStream
import ocpcParams.ocpcParams.{OcpcParamsList,SingleItem}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import scala.collection.mutable.ListBuffer

object OcpcSampleToPb {
  def main(args: Array[String]): Unit = {
    /*
    pb文件格式：
    string expTag = 1;
    string key = 2;
    double cvrCalFactor = 3;
    double jfbFactor = 4;
    double smoothFactor = 5;
    double postCvr = 6;
    double cpaGiven = 7;
    double cpaSuggest = 8;
    double paramT = 9;
    double highBidFactor = 10;
    double lowBidFactor = 11;
    int64 ocpcMincpm = 12;
    int64 ocpcMinbid = 13;
    int64 cpcbid = 14;
	  int64 maxbid = 15;

    key: oCPCNovel&unitid&isHiddenOcpc
     */
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()
    Logger.getRootLogger.setLevel(Level.WARN)

    val date = args(0).toString
    val hour = args(1).toString
    val version = args(2).toString
    val isKnown = args(3).toInt

    println("parameters:")
    println(s"date=$date, hour=$hour, version=$version, isKnown:$isKnown")

    val data = getPostCvrAndK(date, hour, version, spark)

    println("NewK")
    println(data.count())
    data.show(10)

    data
      .select("identifier", "pcoc", "jfb", "kvalue", "conversion_goal","post_cvr")
      .na.fill(0.0, Seq("post_cvr"))

    data.write.mode("overwrite").saveAsTable("test.wy00")

//    val cvGoal = getConversionGoal(date, hour, spark)
//    // 获取postcvr数据
//
//    // 组装数据
//    val result = assemblyData(cvrData, spark)
//
//    val resultDF = result.
//
//    savePbPack(resultDF, version, isKnown)
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
      .withColumn("cv_flag", lit(1))
      .selectExpr("cast(unitid as string) identifier",  "cast(conversion_goal as int) conversion_goal", "cv_flag")
      .distinct()

    resultDF.show(10)
    resultDF
  }

  def getPostCvrAndK(date: String, hour: String, version: String, spark: SparkSession) = {
    /*
    1. 从配置文件和dl_cpc.ocpc_pcoc_jfb_hourly表中抽取需要的jfb数据
    2. 计算新的kvalue
     */
    // 从表中抽取数据
    val sqlRequest =
      s"""
         |SELECT
         |  identifier,
         |  pcoc,
         |  jfb,
         |  1.0 / jfb as kvalue,
         |  conversion_goal,
         |  post_cvr
         |FROM
         |  dl_cpc.ocpc_pcoc_jfb_hourly
         |WHERE
         |  date` = '$date' and `hour` = '$hour'
         |AND
         |  version = '$version'
         |AND
         |  jfb > 0
       """.stripMargin

    println(sqlRequest)
    val data = spark.sql(sqlRequest)

    val resultDF  =data
      .select("identifier", "conversion_goal", "pcoc", "jfb", "kvalue")

    resultDF

  }


//  def savePbPack(dataset: DataFrame, version: String, isKnown: Int): Unit = {
//    var list = new ListBuffer[SingleItem]
//    var filename = ""
//    if (isKnown == 1) {
//      filename = s"Ocpc_" + version + "_known.pb"
//    } else {
//      filename = s"Ocpc_" + version + "_unknown.pb"
//    }
//    println("size of the dataframe")
//    println(dataset.count)
//    println(s"filename: $filename")
//    dataset.show(10)
//    dataset.printSchema()
//    var cnt = 0
//
//    for (record <- dataset.collect()) {
//      val identifier = record.getAs[String]("identifier")
//      val cpaGiven = record.getAs[Double]("cpagiven")
//      val kvalue = record.getAs[Double]("kvalue")
//      val cvrCnt = record.getAs[Long]("cvrcnt")
//      val conversionGoal = record.getAs[Int]("conversion_goal")
//
//      if (cnt % 100 == 0) {
//        println(s"identifier:$identifier, conversionGoal:$conversionGoal, cpaGiven:$cpaGiven, kvalue:$kvalue, cvrCnt:$cvrCnt")
//      }
//      cnt += 1
//
//      val currentItem = SingleItem(
//        identifier = identifier,
//        conversiongoal = conversionGoal,
//        kvalue = kvalue,
//        cpagiven = cpaGiven,
//        cvrcnt = cvrCnt
//      )
//      list += currentItem
//
//    }
//    val result = list.toArray[SingleRecord]
//    val adRecordList = OcpcList(
//      adrecord = result
//    )
//
//    println("length of the array")
//    println(result.length)
//    adRecordList.writeTo(new FileOutputStream(filename))
//
//    println("complete save data into protobuffer")
//
//  }

  def assemblyData(cvrData: DataFrame, spark: SparkSession) = {
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
}


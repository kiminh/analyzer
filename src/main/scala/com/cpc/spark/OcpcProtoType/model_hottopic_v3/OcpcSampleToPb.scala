package com.cpc.spark.OcpcProtoType.model_hottopic_v3

import java.io.FileOutputStream
import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.common.Utils.getTimeRangeSql
import com.cpc.spark.ocpc.OcpcUtils.{getTimeRangeSql2, getTimeRangeSql3}
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import ocpc.ocpc.{OcpcList, SingleRecord}

import scala.collection.mutable.ListBuffer
import org.apache.log4j.{Level, Logger}


object OcpcSampleToPb {
  def main(args: Array[String]): Unit = {
    /*
    pb文件格式：
    string identifier = 1;
    int32 conversiongoal = 2;
    double kvalue = 3;
    double cpagiven = 4;
    int64 cvrcnt = 5;
    对于明投广告，cpagiven=1， cvrcnt使用ocpc广告记录进行关联，k需要进行计算

    将文件从dl_cpc.ocpc_pb_result_hourly_v2表中抽出，存入pb文件，需要过滤条件：
    kvalue>0
     */
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()
    Logger.getRootLogger.setLevel(Level.WARN)

    // bash: 2019-01-02 12 hottopic_test 1
    val date = args(0).toString
    val hour = args(1).toString
    val version = args(2).toString
    val isKnown = args(3).toInt

    println("parameters:")
    println(s"date=$date, hour=$hour, version=$version, isKnown:$isKnown")
    val result1raw  = getPbData(version, date, hour, spark)
    val result1 = result1raw
      .withColumn("kvalue1", col("kvalue"))
      .select("identifier", "conversion_goal", "cpagiven", "cvrcnt", "kvalue1")

    println("result1")
    result1.show(10)

    val result2 = getNewK(date, hour, version, spark)
    val result = result1
      .join(result2, Seq("identifier", "conversion_goal"), "outer")
      .select("identifier", "conversion_goal", "cpagiven", "cvrcnt", "kvalue1", "kvalue2", "flag", "pcoc", "jfb")
      .na.fill(0.0, Seq("cpagiven", "cvrcnt", "kvalue1", "kvalue2", "flag", "pcoc", "jfb"))
      .withColumn("kvalue", when(col("flag") === 1 && col("kvalue2").isNotNull, col("kvalue2")).otherwise(col("kvalue1")))
      .filter(s"kvalue > 0")

    println("result")
    result.show(10)
    val smoothData = result
      .filter(s"flag = 1 and kvalue2 is not null")
      .select("identifier", "pcoc", "jfb", "kvalue", "conversion_goal")

    smoothData
      .withColumn("date", lit(date))
      .withColumn("hour", lit(hour))
      .withColumn("version", lit(version))
//      .repartition(5).write.mode("overwrite").saveAsTable("test.ocpc_kvalue_smooth_strat")
      .repartition(5).write.mode("overwrite").insertInto("dl_cpc.ocpc_kvalue_smooth_strat")

    val resultData1 = result
      .select("identifier", "conversion_goal", "cvrcnt", "kvalue")
    val resultData2 = getCPAgivenFromSuggest("hottopic_test", 1, date, hour, spark)

    val resultJoin = resultData1
      .join(resultData2, Seq("identifier", "conversion_goal"), "inner")
      .select("identifier", "conversion_goal", "cpagiven", "cvrcnt", "kvalue")
      .withColumn("conversion_goal", lit(0))

//    resultJoin.write.mode("overwrite").saveAsTable("test.check_data_table20190418")

    val resultNew = resultData1
      .join(resultData2, Seq("identifier", "conversion_goal"), "left_outer")
      .select("identifier", "conversion_goal", "cpagiven", "cvrcnt", "kvalue")
      .na.fill(1.0, Seq("cpagiven"))

    val resultDF = resultNew
      .union(resultJoin)
      .select("identifier", "conversion_goal", "cpagiven", "cvrcnt", "kvalue")

//    val resultDF = resultNew.union(resultJoin)

    resultDF
        .withColumn("version", lit(version))
        .select("identifier", "conversion_goal", "cpagiven", "cvrcnt", "kvalue", "version")
//        .repartition(10).write.mode("overwrite").saveAsTable("test.ocpc_prev_pb_once20190310")
        .repartition(10).write.mode("overwrite").insertInto("dl_cpc.ocpc_prev_pb_once")

    savePbPack(resultDF, version, isKnown)
  }

  def getCPAgivenFromSuggest(version: String, conversionGoal: Int, date: String, hour: String, spark: SparkSession) = {
    // 取历史数据
    val dateConverter = new SimpleDateFormat("yyyy-MM-dd")
    val today = dateConverter.parse(date)
    val calendar = Calendar.getInstance
    calendar.setTime(today)
    calendar.add(Calendar.DATE, -1)
    val startdate = calendar.getTime
    val date1 = dateConverter.format(startdate)

    val sqlRequest =
      s"""
         |SELECT
         |    cast(unitid as string) as identifier,
         |    conversion_goal,
         |    cpa * 1.0 as cpagiven
         |FROM
         |    dl_cpc.ocpc_suggest_cpa_recommend_hourly
         |WHERE
         |    date = '$date1'
         |AND
         |    `hour` = '06'
         |and is_recommend = 1
         |and version = '$version'
         |and industry = 'feedapp'
         |and conversion_goal = $conversionGoal
         |and unitid in (2008738, 2061698, 2059113, 2009502)
       """.stripMargin
    println(sqlRequest)
    val data = spark.sql(sqlRequest)


    data
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
      .selectExpr("cast(unitid as string) identifier",  "conversion_goal", "cv_flag")
      .distinct()

    resultDF.show(10)
    resultDF
  }

  def getNewK(date: String, hour: String, version: String, spark: SparkSession) = {
    /*
    1. 从配置文件和dl_cpc.ocpc_pcoc_jfb_hourly表中抽取需要的jfb数据
    2. 计算新的kvalue
     */
    // 从表中抽取数据
    val selectCondition = s"`date` = '$date' and `hour` = '$hour'"
    val sqlRequest =
      s"""
         |SELECT
         |  identifier,
         |  pcoc,
         |  jfb,
         |  1.0 / jfb as kvalue2,
         |  conversion_goal
         |FROM
         |  dl_cpc.ocpc_pcoc_jfb_hourly
         |WHERE
         |  $selectCondition
         |AND
         |  version = '$version'
         |AND
         |  pcoc > 0
         |AND
         |  jfb > 0
       """.stripMargin
//    2018851
//    1995684
//    1971569
//    1981958
//    2010532
    println(sqlRequest)
    val data = spark.sql(sqlRequest)

    val resultDF  =data
      .select("identifier", "conversion_goal", "pcoc", "jfb", "kvalue2")
      .withColumn("flag", lit(1))
      .select("identifier", "conversion_goal", "kvalue2", "flag", "pcoc", "jfb")

    resultDF

  }

  def getPbData(version: String, date: String, hour: String, spark: SparkSession) = {
    /*
    string identifier = 1;
    int32 conversiongoal = 2;
    double kvalue = 3;
    double cpagiven = 4;
    int64 cvrcnt = 5;
    1. 从dl_cpc.ocpc_pb_result_hourly_v2中抽取数据
    2. 按照实验配置文件给出cpagiven
     */
    // 从dl_cpc.ocpc_pb_result_hourly_v2中抽取数据
    val selectCondition = s"`date`='$date' and `hour`='$hour' and version='$version'"

    val sqlRequest =
      s"""
         |SELECT
         |  identifier,
         |  conversion_goal,
         |  kvalue,
         |  cpagiven as cpagiven,
         |  cvrcnt
         |FROM
         |  dl_cpc.ocpc_pb_result_hourly_v2
         |WHERE
         |  $selectCondition
         |AND
         |  kvalue > 0
       """.stripMargin
    println(sqlRequest)
    val data = spark.sql(sqlRequest)

    val result = data
    result.printSchema()
    result.show(10)

    val resultDF = result.select("identifier", "conversion_goal", "kvalue", "cpagiven", "cvrcnt")


    resultDF
  }

  def savePbPack(dataset: DataFrame, version: String, isKnown: Int): Unit = {
    var list = new ListBuffer[SingleRecord]
    var filename = ""
    if (isKnown == 1) {
      filename = s"Ocpc_" + version + "_known.pb"
    } else {
      filename = s"Ocpc_" + version + "_unknown.pb"
    }
    println("size of the dataframe")
    println(dataset.count)
    println(s"filename: $filename")
    dataset.show(10)
    dataset.printSchema()
    var cnt = 0

    for (record <- dataset.collect()) {
      val identifier = record.getAs[String]("identifier")
      val cpaGiven = record.getAs[Double]("cpagiven")
      val kvalue = record.getAs[Double]("kvalue")
      val cvrCnt = record.getAs[Long]("cvrcnt")
      val conversionGoal = record.getAs[Int]("conversion_goal")

      if (cnt % 100 == 0) {
        println(s"identifier:$identifier, conversionGoal:$conversionGoal, cpaGiven:$cpaGiven, kvalue:$kvalue, cvrCnt:$cvrCnt")
      }
      cnt += 1

      //      string identifier = 1;
      //      int32 conversiongoal = 2;
      //      double kvalue = 3;
      //      double cpagiven = 4;
      //      int64 cvrcnt = 5;

      val currentItem = SingleRecord(
        identifier = identifier,
        conversiongoal = conversionGoal,
        kvalue = kvalue,
        cpagiven = cpaGiven,
        cvrcnt = cvrCnt
      )
      list += currentItem

    }
    val result = list.toArray[SingleRecord]
    val adRecordList = OcpcList(
      adrecord = result
    )

    println("length of the array")
    println(result.length)
    adRecordList.writeTo(new FileOutputStream(filename))

    println("complete save data into protobuffer")

  }

}


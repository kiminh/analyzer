package com.cpc.spark.OcpcProtoType.model_qtt

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

    // bash: 2019-01-02 12 qtt_demo 1
    val date = args(0).toString
    val hour = args(1).toString
    val version = args(2).toString
    val isKnown = args(3).toInt

    println("parameters:")
    println(s"date=$date, hour=$hour, version=$version, isKnown:$isKnown")
    val resultDF  = getPbData(version, date, hour, spark)

    resultDF
        .withColumn("version", lit(version))
        .select("identifier", "conversion_goal", "cpagiven", "cvrcnt", "kvalue", "version")
        .repartition(10).write.mode("overwrite").saveAsTable("test.ocpc_prev_pb_once20190310")
//        .repartition(10).write.mode("overwrite").insertInto("dl_cpc.ocpc_prev_pb_once")

    savePbPack(resultDF, version, isKnown)
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

//    // 按照实验配置文件给出cpagiven
//    val cpaGivenTable = getCPAgivenV3(date, spark)
//    val cpaGivenConf = getCPAgivenV4(spark)
//    val cpaGiven = cpaGivenTable
//      .join(cpaGivenConf, Seq("identifier", "conversion_goal"), "outer")
//      .withColumn("cpagiven2", when(col("cpagiven2_conf").isNotNull, col("cpagiven2_conf")).otherwise(col("cpagiven2_table")))
//      .select("identifier", "conversion_goal", "cpagiven2")
//
//    // 数据关联
//    val result1 = data
//        .join(cpaGiven, Seq("identifier", "conversion_goal"), "left_outer")
//        .withColumn("cpagiven", when(col("cpagiven2").isNotNull, col("cpagiven2")).otherwise(col("cpagiven1")))
//        .select("identifier", "conversion_goal", "kvalue", "cpagiven", "cvrcnt", "cpagiven1", "cpagiven2")
//
//    // 数据关联
//    val result2 = result1.filter("cpagiven2 is not null")
//        .withColumn("conversion_goal", lit(0))
//        .select("identifier", "conversion_goal", "kvalue", "cpagiven", "cvrcnt", "cpagiven1", "cpagiven2")
//
//    val result = result1.union(result2)
    val result = data
    result.printSchema()
    result.show(10)

    val resultDF = result.select("identifier", "conversion_goal", "kvalue", "cpagiven", "cvrcnt")


    resultDF
  }

//  def getCPAgivenV4(spark: SparkSession) = {
//    val conf = ConfigFactory.load("ocpc")
//    val conf_key = "ocpc_all.unitid_abtest_path"
//    val path = conf.getString(conf_key)
//
//    val resultDF = spark
//      .read.format("json").json(path)
//      .select("unitid", "cpa", "conversion_goal")
//      .selectExpr("cast(unitid as string) as identifier", "conversion_goal", "cpa as cpagiven2_conf")
//
//    resultDF.show(10)
//    resultDF
//  }
//
//  def getCPAgivenV3(date: String, spark: SparkSession) = {
//    // 时间分区
//    val dateConverter = new SimpleDateFormat("yyyy-MM-dd")
//    val today = dateConverter.parse(date)
//    val calendar = Calendar.getInstance
//    calendar.setTime(today)
//    calendar.add(Calendar.DATE, -1)
//    val yesterday = calendar.getTime
//    val date1 = dateConverter.format(yesterday)
//    val selectCondition = s"`date` = '$date1' and `hour` = '06' and version = 'qtt_demo'"
//    val sqlRequest =
//      s"""
//         |SELECT
//         |  cast(unitid as string) identifier,
//         |  conversion_goal,
//         |  cpa as cpagiven2_table
//         |FROM
//         |  dl_cpc.ocpc_auto_budget_hourly
//         |WHERE
//         |  $selectCondition
//         |AND
//         |  industry in ('elds', 'feedapp')
//       """.stripMargin
//    println(sqlRequest)
//    val result = spark.sql(sqlRequest)
//    result
//  }
//
//  def getCPAgivenV2(spark: SparkSession) = {
//    val sqlRequest =
//      s"""
//         |SELECT
//         |  cast(unitid as string) identifier,
//         |  conversion_goal,
//         |  cpa as cpagiven2
//         |FROM
//         |  dl_cpc.ocpc_auto_budget_once
//         |WHERE
//         |  industry = 'elds'
//       """.stripMargin
//    println(sqlRequest)
//    val result = spark.sql(sqlRequest)
//    result
//  }
//
//  def getCPAgiven(spark: SparkSession) = {
//    // 从实验配置文件读取配置的CPAgiven
//    val conf = ConfigFactory.load("ocpc")
//    val expDataPath = conf.getString("ocpc_all.ocpc_abtest.cpagiven_path")
//    println(expDataPath)
//    val data = spark.read.format("json").json(expDataPath)
//
//    val resultDF = data
//      .select("identifier", "cpa_given", "conversion_goal")
//      .groupBy("identifier", "conversion_goal")
//      .agg(avg(col("cpa_given")).alias("cpagiven2"))
//      .select("identifier", "conversion_goal", "cpagiven2")
//
//    resultDF
//  }

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


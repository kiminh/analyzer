package com.cpc.spark.OcpcProtoType.model_v4

import java.io.FileOutputStream

import ocpc.ocpc.{OcpcList, SingleRecord}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable.ListBuffer


object OcpcSampleToPbV2 {
  def main(args: Array[String]): Unit = {
    /*
    pb文件格式：
    string identifier = 1;
    int32 conversiongoal = 2;
    double kvalue = 3;
    double cpagiven = 4;
    int64 cvrcnt = 5;
    对于明投广告，cpagiven=1， cvrcnt使用ocpc广告记录进行关联，k需要进行计算

    将文件从dl_cpc.ocpc_pcoc_jfb_hourly表中抽出，存入pb文件，需要过滤条件：
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

    val data = getBaseData(version, date, hour, spark)
    val result = data
      .withColumn("cpagiven", lit(1))
      .withColumn("cvrcnt", lit(30))
      .select("identifier", "conversion_goal", "cpagiven", "cvrcnt", "kvalue", "pcoc", "jfb")

    result.show(10)

    val smoothData = result
      .select("identifier", "pcoc", "jfb", "kvalue", "conversion_goal")

    smoothData
      .withColumn("date", lit(date))
      .withColumn("hour", lit(hour))
      .withColumn("version", lit(version))
      .repartition(5).write.mode("overwrite").saveAsTable("test.ocpc_kvalue_smooth_strat")
//      .repartition(5).write.mode("overwrite").insertInto("dl_cpc.ocpc_kvalue_smooth_strat")
//
//    val resultDF = result
//      .select("identifier", "conversion_goal", "cpagiven", "cvrcnt", "kvalue")
//
//    resultDF
//        .withColumn("version", lit(version))
//        .select("identifier", "conversion_goal", "cpagiven", "cvrcnt", "kvalue", "version")
////        .repartition(10).write.mode("overwrite").saveAsTable("test.ocpc_prev_pb_once20190310")
//        .repartition(10).write.mode("overwrite").insertInto("dl_cpc.ocpc_prev_pb_once")
//
//    savePbPack(resultDF, version, isKnown)
  }

  def getBaseData(version: String, date: String, hour: String, spark: SparkSession) = {
    val sqlRequest =
      s"""
         |SELECT
         |  identifier,
         |  pcoc,
         |  jfb,
         |  post_cvr,
         |  1.0 / jfb as kvalue
         |FROM
         |  dl_cpc.ocpc_pcoc_jfb_hourly
         |WHERE
         |  `date` = '$date'
         |AND
         |  `hour` = '$hour'
         |AND
         |  version = '$version'
       """.stripMargin
    println(sqlRequest)
    val data = spark.sql(sqlRequest)
    data
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


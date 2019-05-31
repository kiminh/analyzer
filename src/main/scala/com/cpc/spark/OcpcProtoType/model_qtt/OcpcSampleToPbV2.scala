package com.cpc.spark.OcpcProtoType.model_qtt

import java.io.FileOutputStream

import ocpc.ocpc.{OcpcList, SingleRecord}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import com.cpc.spark.OcpcProtoType.model_v4.OcpcSampleToPbV2._
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
      .withColumn("cpagiven", lit(1.0))
      .withColumn("cvrcnt", lit(30))
      .select("identifier", "conversion_goal", "cpagiven", "cvrcnt", "kvalue", "pcoc", "jfb")

    result.show(10)

    val smoothData = result
      .select("identifier", "pcoc", "jfb", "kvalue", "conversion_goal")

    smoothData
      .withColumn("date", lit(date))
      .withColumn("hour", lit(hour))
      .withColumn("version", lit(version))
//      .repartition(5).write.mode("overwrite").saveAsTable("test.ocpc_kvalue_smooth_strat")
      .repartition(5).write.mode("overwrite").insertInto("dl_cpc.ocpc_kvalue_smooth_strat")

    val resultDF = result
      .select("identifier", "conversion_goal", "cpagiven", "cvrcnt", "kvalue")

    resultDF
        .withColumn("version", lit(version))
        .select("identifier", "conversion_goal", "cpagiven", "cvrcnt", "kvalue", "version")
//        .repartition(10).write.mode("overwrite").saveAsTable("test.ocpc_prev_pb_once20190310")
        .repartition(10).write.mode("overwrite").insertInto("dl_cpc.ocpc_prev_pb_once")

    savePbPack(resultDF, version, isKnown)
  }
}


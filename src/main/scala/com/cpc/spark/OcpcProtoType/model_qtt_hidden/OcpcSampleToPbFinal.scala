

package com.cpc.spark.OcpcProtoType.model_qtt_hidden

import java.io.FileOutputStream
import java.text.SimpleDateFormat
import java.util.Calendar

import ocpc.ocpc.{OcpcList, SingleRecord}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable.ListBuffer


object OcpcSampleToPbFinal {
  def main(args: Array[String]): Unit = {
    /*
    pb文件格式：
    string identifier = 1;
    int32 conversiongoal = 2;
    double kvalue = 3;
    double cpagiven = 4;
    int64 cvrcnt = 5;
    对于明投广告，cpagiven=1， cvrcnt使用ocpc广告记录进行关联，k需要进行计算


     */
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()

    // bash: 2019-01-02 12 1
    val date = args(0).toString
    val hour = args(1).toString
    val version = args(2).toString
    val isKnown = args(3).toInt

    println("parameters:")
    println(s"date=$date, hour=$hour, version=$version, isKnown:$isKnown")

    val result1 = getPbData1(date, hour, spark)
    val result2 = getPbData2(date, hour, spark)

    val resultDF = assemblyResult(result1, result2)

    resultDF
        .select("identifier", "cpagiven", "cvrcnt", "kvalue", "conversion_goal")
        .withColumn("date", lit(date))
        .withColumn("hour", lit(hour))
        .withColumn("version", lit("qtt_v1"))
//        .repartition(10).write.mode("overwrite").insertInto("dl_cpc.ocpc_pb_result_hourly_v2")
        .repartition(2).write.mode("overwrite").saveAsTable("test.check_data_pb_20190312")


    savePbPack(resultDF, version, isKnown)
  }

  def assemblyResult(data1: DataFrame, data2: DataFrame) = {
    val data = data1
      .join(data2, Seq("identifier", "conversion_goal"), "outer")
      .select("identifier", "conversion_goal", "cpagiven1", "cvrcnt1", "kvalue1", "version1", "cpagiven2", "cvrcnt2", "kvalue2", "version2")
      .withColumn("cpagiven", when(col("cpagiven2").isNotNull, col("cpagiven2")).otherwise(col("cpagiven1")))
      .withColumn("cvrcnt", when(col("cvrcnt2").isNotNull, col("cvrcnt2")).otherwise(col("cvrcnt1")))
      .withColumn("kvalue", when(col("kvalue2").isNotNull, col("kvalue2")).otherwise(col("kvalue1")))
      .withColumn("version", when(col("version2").isNotNull, col("version2")).otherwise(col("version1")))

    data.write.mode("overwrite").saveAsTable("test.check_data_pb_20190319")

    data
  }

  def getPbData2(date: String, hour: String, spark: SparkSession) = {
    val data = spark
      .table("dl_cpc.ocpc_prev_pb_once")
      .where(s"version = 'wz' and cpagiven > 0")
      .withColumn("conversion_goal", lit(1))
      .withColumn("cpagiven2", col("cpagiven"))
      .withColumn("cvrcnt2", col("cvrcnt"))
      .withColumn("kvalue2", col("kvalue"))
      .withColumn("version2", col("version"))
      .select("identifier", "conversion_goal", "cvrcnt2", "cpagiven2", "kvalue2", "version2")

    data.show(10)
    data
  }

  def getPbData1(date: String, hour: String, spark: SparkSession) = {
    /*
    1. 从pb文件抽取数据
    2. 从预算控制表抽取数据
     */
    val resultDF = spark
      .table("dl_cpc.ocpc_prev_pb_once")
      .where(s"version = qtt_hidden'")
      .withColumn("cpagiven1", col("cpagiven"))
      .withColumn("cvrcnt1", col("cvrcnt"))
      .withColumn("kvalue1", col("kvalue"))
      .withColumn("version1", col("version"))
      .select("identifier", "conversion_goal", "cpagiven1", "cvrcnt1", "kvalue1", "version1")

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



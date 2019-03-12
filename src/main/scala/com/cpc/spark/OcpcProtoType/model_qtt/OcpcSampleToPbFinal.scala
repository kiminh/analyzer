

package com.cpc.spark.OcpcProtoType.model_qtt

import java.io.FileOutputStream
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import ocpc.ocpc.{OcpcList, SingleRecord}

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

    val resultDF = result1.union(result2)
    resultDF
        .select("identifier", "cpagiven", "cvrcnt", "kvalue", "conversion_goal")
        .withColumn("date", lit(date))
        .withColumn("hour", lit(hour))
        .withColumn("version", lit("qtt_v1"))
        .repartition(10).write.mode("overwrite").insertInto("dl_cpc.ocpc_pb_result_hourly_v2")
//        .repartition(2).write.mode("overwrite").saveAsTable("test.check_data_pb_20190312")


    savePbPack(resultDF, version, isKnown)
  }

  def getPbData2(date: String, hour: String, spark: SparkSession) = {
    val data = spark
      .table("dl_cpc.ocpc_prev_pb_once")
      .where(s"version = 'wz' and cpagiven > 0")
      .withColumn("conversion_goal", lit(0))
      .select("identifier", "conversion_goal", "cpagiven", "cvrcnt", "kvalue", "version")

    data.show(10)
    data
  }

  def getPbData1(date: String, hour: String, spark: SparkSession) = {
    val data = spark
      .table("dl_cpc.ocpc_prev_pb_once")
      .where(s"version = 'qtt_demo'")
      .select("identifier", "conversion_goal", "cpagiven", "cvrcnt", "kvalue", "version")

    data.show(10)
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



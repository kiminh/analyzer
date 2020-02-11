package com.cpc.spark.ml.dnn.baseData

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.commons.codec.binary.Base64
import org.apache.hadoop.io.BytesWritable
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions._
import org.tensorflow.example.Example
import org.tensorflow.spark.datasources.tfrecords.TensorFlowInferSchema
import org.tensorflow.spark.datasources.tfrecords.serde.DefaultTfRecordRowDecoder

/**
  * 提取样本的hashcode
  * created time : 2020/02/09 16:38
  * @author duanguangdong
  * @version 2.0
  *
  */

object DnnTrainingData {
  Logger.getRootLogger.setLevel(Level.WARN)

  def generateSql(model_name: String, curday: String, sample_path: String): String = {
    val sql = s"select example from dl_cpc.$sample_path where dt='$curday' and task='$model_name'"
    sql
  }

  def main(args: Array[String]): Unit = {
    if (args.length != 10) {
      System.err.println(
        """
          |you have to input 10 parameters !!!
        """.stripMargin)
      System.exit(1)
    }
    val tuid = args(0).toInt
    val userid = args(1).toInt
    val adclass = args(2).toInt
    val planid = args(3).toInt
    val unitid = args(4).toInt
    val ideaid = args(5).toInt
    val model_name = args(6)
    val sample_path = args(7)
    val curday = args(8)
    val pt = args(9)
    println(args)
    val spark = SparkSession.builder().appName("feature monitor").enableHiveSupport().getOrCreate()
    val cal = Calendar.getInstance()
    cal.setTime(new SimpleDateFormat("yyyy-MM-dd").parse(s"$curday"))
    cal.add(Calendar.DATE, -1)
    val oneday = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime)
    var importedDf: DataFrame = null

    if (sample_path.startsWith("hdfs://")) {
      importedDf = spark.read.format("tfrecords").option("recordType", "Example").load(sample_path).repartition(3000)
    } else {
      val rdd = spark.sql(generateSql(model_name, oneday, sample_path))
        .rdd.map(x => Base64.decodeBase64(x.getString(0)))
        .filter(_ != null)
      val exampleRdd = rdd.map(x => Example.parseFrom(new BytesWritable(x).getBytes))
      val finalSchema = TensorFlowInferSchema(exampleRdd)
      val rowRdd = exampleRdd.map(example => DefaultTfRecordRowDecoder.decodeExample(example, finalSchema))
      importedDf = spark.createDataFrame(rowRdd, finalSchema).repartition(3000)
    }

    println("file read finish")
    importedDf.createOrReplaceTempView("training_data")

    spark.sql(
      s"""
         |insert into dl_cpc.cpc_training_data partition(day="$oneday")
         |select dense[$tuid], dense[$userid], dense[$adclass], dense[$planid], dense[$unitid], dense[$ideaid], '$model_name' from training_data
         |""".stripMargin)
  }
}

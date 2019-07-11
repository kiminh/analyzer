package com.cpc.spark.ml.dnn.baseData

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.log4j.{Level, Logger}
import scala.collection.JavaConversions._;
import scala.collection.JavaConverters._;
import collection.JavaConversions._
import org.apache.log4j.{ Level, Logger }
import org.apache.spark.SparkConf

import org.apache.spark.sql.SparkSession

import org.apache.spark.sql.{ DataFrame, Row }
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.types._

object ReadExampleFromHdfs {
  def main(args: Array[String]): Unit = {
    Logger.getRootLogger.setLevel(Level.WARN)
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()

    var path = "hdfs://emr-cluster/user/cpc/aiclk_dataflow/daily/adlist-v4/2019-06-11-bak/"

    //Read TFRecords into DataFrame.
    //The DataFrame schema is inferred from the TFRecords if no custom schema is provided.
    val importedDf0: DataFrame = spark.read.format("tfrecords").option("recordType", "Example").load(path)
    importedDf0.show(10)

    //val new_path = "hdfs://emr-cluster/user/cpc/fhb/adlist-v4/2019-06-11"
    //importedDf1.repartition(100).saveAs

    path = "hdfs://emr-cluster/user/cpc/fenghuabin/adlist_tf_decode"

    val testRows: Array[Row] = Array(
      new GenericRow(Array[Any](11, 1, 23L, 10.0F, 14.0, List(1.0, 2.0), "r1")),
      new GenericRow(Array[Any](21, 2, 24L, 12.0F, 15.0, List(2.0, 2.0), "r2")))

    val schema = StructType(List(
      StructField("id", IntegerType),
      StructField("IntegerCol", IntegerType),
      StructField("LongCol", LongType),
      StructField("FloatCol", FloatType),
      StructField("DoubleCol", DoubleType),
      StructField("VectorCol", ArrayType(DoubleType, true)),
      StructField("StringCol", StringType)))

    val rdd = spark.sparkContext.parallelize(testRows)

    //Save DataFrame as TFRecords
    val df: DataFrame = spark.createDataFrame(rdd, schema)
    df.write.format("tfrecords").option("recordType", "Example").save(path)

    //Read TFRecords into DataFrame.
    //The DataFrame schema is inferred from the TFRecords if no custom schema is provided.
    val importedDf1: DataFrame = spark.read.format("tfrecords").option("recordType", "Example").load(path)
    importedDf1.show()

    //Read TFRecords into DataFrame using custom schema
    val importedDf2: DataFrame = spark.read.format("tfrecords").schema(schema).load(path)
    importedDf2.show()
  }
}
package com.cpc.spark.ml.video_model

import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.common.Murmur3Hash
import com.cpc.spark.ml.dnn.Utils.CommonUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel

object video_sample {
  Logger.getRootLogger.setLevel(Level.WARN)
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("video sample").enableHiveSupport().getOrCreate()
    val curday = args(0)
    val cal = Calendar.getInstance()
    cal.setTime(new SimpleDateFormat("yyyy-MM-dd").parse(s"$curday"))
    cal.add(Calendar.DATE, -1)
    val oneday = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime)

    val sample = getSample(spark, oneday)
    sample.repartition(500).write
      .mode("overwrite")
      .format("tfrecords")
      .option("recordType", "Example")
      .save(s"hdfs://emr-cluster/user/cpc/aiclk_dataflow/daily/videoctr-v1/$oneday/")
    val CountPathTmpName = s"hdfs://emr-cluster/user/cpc/aiclk_dataflow/daily/videoctr-v1/tmp/"
    val CountPathName = s"hdfs://emr-cluster/user/cpc/aiclk_dataflow/daily/videoctr-v1/$oneday/count"
    val count = spark.read.format("tfrecords").option("recordType", "Example").load(s"hdfs://emr-cluster/user/cpc/aiclk_dataflow/daily/videoctr-v1/$oneday/part*").count()
    CommonUtils.writeCountToFile(spark, count, CountPathTmpName, CountPathName)
  }
  def getSample(spark: SparkSession, date: String): DataFrame = {
    import spark.implicits._
    var original_sample: DataFrame = null
    original_sample = spark.read.format("tfrecords").option("recordType", "Example").load(s"hdfs://emr-cluster/user/cpc/aiclk_dataflow/daily/adlist-v4/$date/part*").
      select($"sample_idx",$"idx0",$"idx1",$"idx2",$"id_arr", $"label", $"dense",expr("dense[8]").alias("adtypehash"))
    val adtype8_hash = Murmur3Hash.stringHash64("f8" + "#" + "8", 0)
    val adtype10_hash = Murmur3Hash.stringHash64("f8" + "#" + "10", 0)
    val video_sample = original_sample.where(s"adtypehash in ($adtype8_hash, $adtype10_hash)").select($"sample_idx",$"idx0",$"idx1",$"idx2",$"id_arr", $"label", $"dense")
    video_sample
  }

}

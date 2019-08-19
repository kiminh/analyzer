package com.cpc.spark.ml.video_model

import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.common.Murmur3Hash
import com.cpc.spark.ml.dnn.Utils.CommonUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object video_sample_v2 {
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
      .save(s"hdfs://emr-cluster/user/cpc/aiclk_dataflow/daily/videoctr-v2/$oneday/")
    val CountPathTmpName = s"hdfs://emr-cluster/user/cpc/aiclk_dataflow/daily/videoctr-v2/tmp/"
    val CountPathName = s"hdfs://emr-cluster/user/cpc/aiclk_dataflow/daily/videoctr-v2/$oneday/count"
    val count = spark.read.format("tfrecords").option("recordType", "Example").load(s"hdfs://emr-cluster/user/cpc/aiclk_dataflow/daily/videoctr-v2/$oneday/part*").count()
    CommonUtils.writeCountToFile(spark, count, CountPathTmpName, CountPathName)
  }
  def getSample(spark: SparkSession, date: String): DataFrame = {
    import spark.implicits._
    var original_sample: DataFrame = null
    original_sample = spark.read.format("tfrecords").option("recordType", "Example").load(s"hdfs://emr-cluster/user/cpc/aiclk_dataflow/daily/adlist-v4/$date/part*").
      select($"sample_idx",$"idx0",$"idx1",$"idx2",$"id_arr", $"label", $"dense",expr("dense[8]").alias("adtypehash"),expr("dense[11]").alias("ideaidhash"))
    val adtype8_hash = Murmur3Hash.stringHash64("f8" + "#" + "8", 0)
    val adtype10_hash = Murmur3Hash.stringHash64("f8" + "#" + "10", 0)
    val video_sample = original_sample.where(s"adtypehash in ($adtype8_hash, $adtype10_hash)").select($"sample_idx",$"idx0",$"idx1",$"idx2",$"id_arr", $"label", $"dense", $"ideaidhash")
    val ideaid_feature = spark.sql(s"""select ideaid, resource from dl_cpc.resource where model='video'""").select(hash("f11")($"ideaid").alias("ideaidhash"),$"resource")
    val result = video_sample.join(ideaid_feature, Seq("ideaidhash"), "left_outer").
      select($"sample_idx", $"idx0", $"idx1", $"idx2", $"id_arr", $"label", $"dense", array(hash("f" + 28)($"resource")).alias("resourceidhash")).rdd.map{
      r =>
        val sample_idx = r.getAs[Long]("sample_idx")
        val label = r.getAs[Seq[Long]]("label")
        val idx0 = r.getAs[Seq[Long]]("idx0")
        val idx1 = r.getAs[Seq[Long]]("idx1")
        val idx2 = r.getAs[Seq[Long]]("idx2")
        val id_arr = r.getAs[Seq[Long]]("id_arr")
        val dense = r.getAs[Seq[Long]]("dense")
        val resourceidhash = r.getAs[Seq[Long]]("resourceidhash")
        (sample_idx, label, dense ++ resourceidhash, idx0, idx1, idx2, id_arr)
    }.toDF("sample_idx", "label", "dense", "idx0", "idx1", "idx2", "id_arr")
    result
  }

  private def hash(prefix: String) = udf {
    num: String =>
      if (num != null) Murmur3Hash.stringHash64(prefix + "#" + num, 0) else Murmur3Hash.stringHash64(prefix + "#", 0)
  }

}

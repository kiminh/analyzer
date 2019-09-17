package com.cpc.spark.ml.dyrec

import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.common.Murmur3Hash
import com.cpc.spark.ml.dnn.Utils.CommonUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object sample_test {
  Logger.getRootLogger.setLevel(Level.WARN)
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("video sample").enableHiveSupport().getOrCreate()
    import spark.implicits._
    val curday = args(0)
    val cal = Calendar.getInstance()
    cal.setTime(new SimpleDateFormat("yyyy-MM-dd").parse(s"$curday"))
    cal.add(Calendar.DATE, -1)
    val oneday = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime)
    val adtype15_hash = Murmur3Hash.stringHash64("f8" + "#" + "15", 0)
    val adslot_id_hash = Murmur3Hash.stringHash64("f5" + "#" + "7268884", 0)

    val today_sample = spark.read.format("tfrecords").option("recordType", "Example").load(s"hdfs://emr-cluster/user/cpc/sample/recall/featureSystem/offlineAuc/rec_test/$oneday/part*")

    today_sample.select($"sample_idx",$"idx0",$"idx1",$"idx2",$"id_arr", $"label", $"dense",expr("dense[5]").alias("adslotidhash"),expr("dense[8]").alias("adtypehash")).where(s"adtypehash != $adtype15_hash and adslotidhash != $adslot_id_hash").select($"sample_idx",$"idx0",$"idx1",$"idx2",$"id_arr", $"label", $"dense").write
      .mode("overwrite")
      .format("tfrecords")
      .option("recordType", "Example")
      .save(s"hdfs://emr-cluster/user/cpc/sample/recall/featureSystem/offlineAuc/rec_test/$oneday-test")
    val CountPathTmpName = s"hdfs://emr-cluster/user/cpc/sample/recall/featureSystem/offlineAuc/rec_test/tmp/"
    val CountPathName = s"hdfs://emr-cluster/user/cpc/sample/recall/featureSystem/offlineAuc/rec_test/$oneday-test/count"
    val count = spark.read.format("tfrecords").option("recordType", "Example").load(s"hdfs://emr-cluster/user/cpc/sample/recall/featureSystem/offlineAuc/rec_test/$oneday-test/part*").count()
    CommonUtils.writeCountToFile(spark, count, CountPathTmpName, CountPathName)
  }

}

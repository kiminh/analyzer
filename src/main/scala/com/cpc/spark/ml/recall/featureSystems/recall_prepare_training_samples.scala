package com.cpc.spark.ml.recall.featureSystems

import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.common.Murmur3Hash
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions._

import scala.collection.mutable

object recall_prepare_training_samples {
  Logger.getRootLogger.setLevel(Level.WARN)

  //multi hot 特征默认hash code
  private val default_hash = for (i <- 1 to 37) yield Seq((i.toLong - 1, 0.toLong, Murmur3Hash.stringHash64("m" + i, 0)))

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("recall_prepare_training_samples")
      .enableHiveSupport()
      .getOrCreate()
    val featureName = args(0)
    val model_version = "adlist-v4"
    val cal1 = Calendar.getInstance()
    cal1.add(Calendar.DATE, -1)
    val oneday = new SimpleDateFormat("yyyy-MM-dd").format(cal1.getTime)
    getSample(spark, model_version, oneday).repartition(1000)
      .write
      .mode("overwrite")
      .format("tfrecords")
      .option("recordType", "Example")
      .save(s"hdfs://emr-cluster/user/cpc/sample/recall/featureSystem/offlineAuc/$oneday")

    cal1.add(Calendar.DATE, -1)
    val twoday = new SimpleDateFormat("yyyy-MM-dd").format(cal1.getTime)
    getSample(spark, model_version, twoday).repartition(1000)
      .write
      .mode("overwrite")
      .format("tfrecords")
      .option("recordType", "Example")
      .save(s"hdfs://emr-cluster/user/cpc/sample/recall/featureSystem/offlineAuc/$twoday")

    cal1.add(Calendar.DATE, -1)
    val threeday = new SimpleDateFormat("yyyy-MM-dd").format(cal1.getTime)
    getSample(spark, model_version, threeday).repartition(1000)
      .write
      .mode("overwrite")
      .format("tfrecords")
      .option("recordType", "Example")
      .save(s"hdfs://emr-cluster/user/cpc/sample/recall/featureSystem/offlineAuc/$threeday")

    cal1.add(Calendar.DATE, -1)
    val fourday = new SimpleDateFormat("yyyy-MM-dd").format(cal1.getTime)
    getSample(spark, model_version, fourday).repartition(1000)
      .write
      .mode("overwrite")
      .format("tfrecords")
      .option("recordType", "Example")
      .save(s"hdfs://emr-cluster/user/cpc/sample/recall/featureSystem/offlineAuc/$fourday")

  }
  def getSample(spark: SparkSession, model_version: String, featureName: String, date: String): DataFrame = {
    import spark.implicits._
    val original_sample = spark.read.format("tfrecords").option("recordType", "Example").load(s"hdfs://emr-cluster/user/cpc/aiclk_dataflow/daily/$model_version/$date/part*").
      select($"sample_idx",$"idx0",$"idx1",$"idx2",$"id_arr", $"label", $"dense",expr("dense[25]").alias("uidhash"))
    val multihot_feature = original_sample.limit(10).cache().select(expr("max(idx1[size(idx1)-1])").alias("idx1")).collect()
    val multihot_feature_number = multihot_feature(0)(0).toString.toInt + 1

    val onehot_feature = original_sample.limit(10).cache().select(expr("max(size(dense))").alias("idx1")).collect()
    val onehot_feature_number = onehot_feature(0)(0).toString.toInt

    val feature_onehot1 = spark.sql(
      s"""
         |select uid from dl_cpc.recall_test_feature where dt='$date' and feature_onehot1 is not null and featureName='$featureName'
       """.stripMargin).count()
    val feature_onehot2 = spark.sql(
      s"""
         |select uid from dl_cpc.recall_test_feature where dt='$date' and feature_onehot2 is not null and featureName='$featureName'
       """.stripMargin).count()
    val feature_onehot3 = spark.sql(
      s"""
         |select uid from dl_cpc.recall_test_feature where dt='$date' and feature_onehot3 is not null and featureName='$featureName'
       """.stripMargin).count()
    var feature_onehot = ""
    if(feature_onehot1>0){
      feature_onehot = "array(feature_onehot1) as one_hot"
      if(feature_onehot2>0){
        feature_onehot = "array(feature_onehot1, feature_onehot2) as one_hot"
        if(feature_onehot3>0){
          feature_onehot = "array(feature_onehot1, feature_onehot2, feature_onehot3) as one_hot"
        }
      }
    }
    val feature_multihot1 = spark.sql(
      s"""
         |select uid from dl_cpc.recall_test_feature where dt='$date' and feature_multihot1 is not null and featureName='$featureName'
       """.stripMargin).count()
    val feature_multihot2 = spark.sql(
      s"""
         |select uid from dl_cpc.recall_test_feature where dt='$date' and feature_multihot2 is not null and featureName='$featureName'
       """.stripMargin).count()
    val feature_multihot3 = spark.sql(
      s"""
         |select uid from dl_cpc.recall_test_feature where dt='$date' and feature_multihot3 is not null and featureName='$featureName'
       """.stripMargin).count()
    var feature_multihot = ""
    if(feature_multihot1>0){
      feature_multihot = "array(feature_multihot1) as multi_hot"
      if(feature_multihot2>0){
        feature_multihot = "array(feature_multihot1, feature_multihot2) as multi_hot"
        if(feature_multihot3>0){
          feature_multihot = "array(feature_multihot1, feature_multihot2, feature_multihot3) as multi_hot"
        }
      }
    }
    var feature = ""
    if(feature_onehot!="" && feature_multihot!=""){
      feature = feature_onehot + "," + feature_multihot
    } else if(feature_onehot!=""){
      feature = feature_onehot
    } else if(feature_multihot!=""){
      feature = feature_multihot
    }

    spark.sql(
      s"""
         |select uid, feature_onehot1, feature_onehot2, feature_onehot3, feature_multihot1, feature_multihot2,
         |feature_multihot3 from dl_cpc.recall_test_feature where dt='$date' and featureName='$featureName'
       """.stripMargin).select($"uid", hash("f25")($"uid").alias("uidhash"),$"feature_onehot1", $"feature_onehot2",$"feature_onehot3",$"feature_multihot1",$"feature_multihot2",$"feature_multihot3").
      join(original_sample, Seq("uidhash"), "right_outer").
      select($"uid", $"uidhash",
        hash("f" + onehot_feature_number)($"feature_onehot1").alias("feature_onehot1"),
        hash("f" + (onehot_feature_number+1))($"feature_onehot2").alias("feature_onehot2"),
        hash("f" + (onehot_feature_number+2))($"feature_onehot3").alias("feature_onehot3"),
        hashSeq("m" + multihot_feature_number, "string")($"feature_multihot1").alias("feature_multihot1"),
        hashSeq("m" + (multihot_feature_number+1), "string")($"feature_multihot2").alias("feature_multihot2"),
        hashSeq("m" + (multihot_feature_number+2), "string")($"feature_multihot3").alias("feature_multihot3"),
        $"sample_idx",$"idx0",$"idx1",$"idx2",$"id_arr", $"label", $"dense").createOrReplaceTempView("temp_feature")
    var new_feature: DataFrame = null
    var newSample: DataFrame = null
    if(feature_multihot!="" && feature_multihot!=""){
      new_feature = spark.sql(
        s"""
           |select uid, uidhash,
           |$feature,
           | sample_idx, idx0, idx1, idx2, id_arr, label, dense from temp_feature
       """.stripMargin).
        select($"uid", $"uidhash", $"one_hot", mkSparseFeature_m(multihot_feature_number)($"multi_hot").alias("sparse"),
          $"sample_idx",$"idx0",$"idx1",$"idx2",$"id_arr", $"label", $"dense").
        select($"uid", $"uidhash", $"one_hot".alias("dense_new"),$"sparse".getField("_1").alias("idx0_new"),
          $"sparse".getField("_2").alias("idx1_new"),
          $"sparse".getField("_3").alias("idx2_new"),
          $"sparse".getField("_4").alias("id_arr_new"),
          $"sample_idx",$"idx0",$"idx1",$"idx2",$"id_arr", $"label", $"dense")
      newSample = new_feature.rdd.map{
        r =>
          val uid = r.getAs[String]("uid")
          val sample_idx = r.getAs[Long]("sample_idx")
          val label = r.getAs[Seq[Long]]("label")
          val idx0 = r.getAs[Seq[Long]]("idx0")
          val idx1 = r.getAs[Seq[Long]]("idx1")
          val idx2 = r.getAs[Seq[Long]]("idx2")
          val id_arr = r.getAs[Seq[Long]]("id_arr")
          val dense = r.getAs[Seq[Long]]("dense")
          val idx0_new = r.getAs[Seq[Long]]("idx0_new")
          val idx1_new = r.getAs[Seq[Long]]("idx1_new")
          val idx2_new = r.getAs[Seq[Long]]("idx2_new")
          val id_arr_new = r.getAs[Seq[Long]]("id_arr_new")
          val dense_new = r.getAs[Seq[Long]]("dense_new")
          (sample_idx, label, dense ++ dense_new, idx0 ++ idx0_new, idx1 ++ idx1_new, idx2 ++ idx2_new, id_arr ++ id_arr_new)
      }.toDF("sample_idx", "label", "dense", "idx0", "idx1", "idx2", "id_arr")
    } else if (feature_onehot!="") {
      new_feature = spark.sql(
        s"""
           |select uid, uidhash,
           |$feature,
           | sample_idx, idx0, idx1, idx2, id_arr, label, dense from temp_feature
       """.stripMargin).
        select($"uid", $"uidhash", $"one_hot",
          $"sample_idx",$"idx0",$"idx1",$"idx2",$"id_arr", $"label", $"dense").
        select($"uid", $"uidhash", $"one_hot".alias("dense_new"),
          $"sample_idx",$"idx0",$"idx1",$"idx2",$"id_arr", $"label", $"dense")
      newSample = new_feature.rdd.map{
        r =>
          val uid = r.getAs[String]("uid")
          val sample_idx = r.getAs[Long]("sample_idx")
          val label = r.getAs[Seq[Long]]("label")
          val idx0 = r.getAs[Seq[Long]]("idx0")
          val idx1 = r.getAs[Seq[Long]]("idx1")
          val idx2 = r.getAs[Seq[Long]]("idx2")
          val id_arr = r.getAs[Seq[Long]]("id_arr")
          val dense = r.getAs[Seq[Long]]("dense")
          val idx0_new = r.getAs[Seq[Long]]("idx0_new")
          val idx1_new = r.getAs[Seq[Long]]("idx1_new")
          val idx2_new = r.getAs[Seq[Long]]("idx2_new")
          val id_arr_new = r.getAs[Seq[Long]]("id_arr_new")
          val dense_new = r.getAs[Seq[Long]]("dense_new")
          (sample_idx, label, dense ++ dense_new, idx0 ++ idx0_new, idx1 ++ idx1_new, idx2 ++ idx2_new, id_arr ++ id_arr_new)
      }.toDF("sample_idx", "label", "dense", "idx0", "idx1", "idx2", "id_arr")
    } else if (feature_onehot!="") {
      new_feature = spark.sql(
        s"""
           |select uid, uidhash,
           |$feature,
           | sample_idx, idx0, idx1, idx2, id_arr, label, dense from temp_feature
       """.stripMargin).
        select($"uid", $"uidhash", $"one_hot",
          $"sample_idx",$"idx0",$"idx1",$"idx2",$"id_arr", $"label", $"dense").
        select($"uid", $"uidhash", $"one_hot".alias("dense_new"),
          $"idx0",$"idx1",$"idx2",$"id_arr", $"label", $"dense")
      newSample = new_feature.rdd.map{
        r =>
          val uid = r.getAs[String]("uid")
          val sample_idx = r.getAs[Long]("sample_idx")
          val label = r.getAs[Seq[Long]]("label")
          val idx0 = r.getAs[Seq[Long]]("idx0")
          val idx1 = r.getAs[Seq[Long]]("idx1")
          val idx2 = r.getAs[Seq[Long]]("idx2")
          val id_arr = r.getAs[Seq[Long]]("id_arr")
          val dense = r.getAs[Seq[Long]]("dense")
          val dense_new = r.getAs[Seq[Long]]("dense_new")
          (sample_idx, label, dense ++ dense_new, idx0, idx1, idx2, id_arr)
      }.toDF("sample_idx", "label", "dense", "idx0", "idx1", "idx2", "id_arr")
    }

//    val newFeature_sample = original_sample.join(new_feature, Seq("uidhash"), "left_outer")
    newSample
  }
  private def mkSparseFeature_m(origin_num: Int) = udf {
    features: Seq[Seq[Long]] =>
      var i = origin_num
      var re = Seq[(Long, Long, Long)]()
      for (feature <- features) {
        re = re ++
          (if (feature != null) feature.zipWithIndex.map(x => (i.toLong, x._2.toLong, x._1)) else default_hash(i))
        i = i + 1
      }
      val c = re.map(x => (0.toLong, x._1, x._2, x._3))
      (c.map(_._1), c.map(_._2), c.map(_._3), c.map(_._4))
  }
  private def hash(prefix: String) = udf {
    num: String =>
      if (num != null) Murmur3Hash.stringHash64(prefix + "#" + num, 0) else Murmur3Hash.stringHash64(prefix + "#", 0)
  }

  /**
    * 获取hash code
    *
    * @param prefix ：前缀
    * @param t      ：类型
    * @return
    */
  private def hashSeq(prefix: String, t: String) = {
    t match {
      case "int" => udf {
        seq: Seq[Int] =>
          val re = if (seq != null && seq.nonEmpty) for (i <- seq) yield Murmur3Hash.stringHash64(prefix + i, 0)
          else Seq(Murmur3Hash.stringHash64(prefix, 0))
          re.slice(0, 1000)
      }
      case "string" => udf {
        seq: Seq[String] =>
          val re = if (seq != null && seq.nonEmpty) for (i <- seq) yield Murmur3Hash.stringHash64(prefix + i, 0)
          else Seq(Murmur3Hash.stringHash64(prefix, 0))
          re.slice(0, 1000)
      }
    }
  }
}

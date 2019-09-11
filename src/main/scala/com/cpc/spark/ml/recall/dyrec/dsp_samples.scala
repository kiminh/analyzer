package com.cpc.spark.ml.recall.dyrec

import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.common.Murmur3Hash
import com.cpc.spark.ml.dnn.Utils.CommonUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.{udf, _}
import org.apache.spark.sql.{DataFrame, SparkSession}

object dsp_samples {
  Logger.getRootLogger.setLevel(Level.WARN)

  //multi hot 特征默认hash code
  private val default_hash = for (i <- 1 to 37) yield Seq((i.toLong - 1, 0.toLong, Murmur3Hash.stringHash64("m" + i, 0)))

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("wy_dsp_samples")
      .enableHiveSupport()
      .getOrCreate()
    val curday = args(0)
    val model_version = "cpc_tensorflow_example_half"
    val cal1 = Calendar.getInstance()
    cal1.setTime(new SimpleDateFormat("yyyy-MM-dd").parse(s"$curday"))
    val today = args(0)
    val hour = args(1)
    val minute = args(2)
    print(today + " " + hour + " " + minute)
    cal1.add(Calendar.DATE, -1)
    val oneday = new SimpleDateFormat("yyyy-MM-dd").format(cal1.getTime)
    var Type = "2"
    if(minute == "15") {
      Type = "0"
    } else if (minute == "45") {
      Type = "1"
    }

    cal1.add(Calendar.DATE, -1)
    val twoday = new SimpleDateFormat("yyyy-MM-dd").format(cal1.getTime)
    getSample(spark, model_version, Type, today, oneday, hour).repartition(1000)
      .write
      .mode("overwrite")
      .format("tfrecords")
      .option("recordType", "Example")
      .save(s"hdfs://emr-cluster/user/cpc/aiclk_dataflow/realtime/adlist-v4-dsp/$today/$hour/$Type")
    val CountPathTmpName = s"hdfs://emr-cluster/user/cpc/aiclk_dataflow/daily/adlist-v4-dsp/tmp/"
    val CountPathName = s"hdfs://emr-cluster/user/cpc/aiclk_dataflow/realtime/adlist-v4-dsp/$today/$hour/$Type/count"
    val count = spark.read.format("tfrecords").option("recordType", "Example").load(s"hdfs://emr-cluster/user/cpc/aiclk_dataflow/realtime/adlist-v4-dsp/$today/$hour/$Type/part*").count()
    CommonUtils.writeCountToFile(spark, count, CountPathTmpName, CountPathName)
  }

  def getSample(spark: SparkSession, model_version: String, Type: String, today: String, oneday: String, hour: String): DataFrame = {
    import spark.implicits._
    var original_sample: DataFrame = null
    original_sample = spark.read.format("tfrecords").option("recordType", "Example").load(s"hdfs://emr-cluster2ns2/user/$model_version/$today/$hour/$Type/part*").
      select($"sample_idx", $"idx0", $"idx1", $"idx2", $"id_arr", $"label", $"dense", expr("dense[25]").alias("uidhash"))
    val multihot_feature = original_sample.limit(10).cache().select(expr("max(idx1[size(idx1)-1])").alias("idx1")).collect()
    println(multihot_feature)
    val multihot_feature_number = multihot_feature(0)(0).toString.toInt + 1
    val sample = spark.sql(
      s"""
         |select distinct uid,cast(click_adsrc_3 as Array<String>) click_adsrc_3,
         |cast(show_adsrc_3 as Array<String>) show_adsrc_3,
         |cast(adsrc_high_freq as Array<String>) adsrc_high_freq
         |from dl_cpc.dsp_adsrc_feature2 where dt='$oneday'
       """.stripMargin)

    sample.createOrReplaceTempView("sample_new")
    val sample_new = spark.sql(
      s"""
         |select * from sample_new
       """.stripMargin).select(hash("f25")($"uid").alias("uidhash"),
      $"click_adsrc_3",
      $"show_adsrc_3",
      $"adsrc_high_freq"
    )
    val result = original_sample.join(sample_new, Seq("uidhash"), "left_outer").
      select($"sample_idx", $"idx0", $"idx1", $"idx2", $"id_arr", $"label", $"dense",
        mkSparseFeature_m(multihot_feature_number)(array(
          hashSeq("ud136", "string")($"click_adsrc_3").alias("click_adsrc_3"),
          hashSeq("ud137", "string")($"show_adsrc_3").alias("show_adsrc_3"),
          hashSeq("ud138", "string")($"adsrc_high_freq").alias("adsrc_high_freq")
        )).alias("sparse")).select($"sample_idx", $"idx0", $"idx1", $"idx2", $"id_arr", $"label", $"dense",
      $"sparse".getField("_1").alias("idx0_new"),
      $"sparse".getField("_2").alias("idx1_new"),
      $"sparse".getField("_3").alias("idx2_new"),
      $"sparse".getField("_4").alias("id_arr_new")
    ).rdd.map {
      r =>
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
        (sample_idx, label, dense, idx0 ++ idx0_new, idx1 ++ idx1_new, idx2 ++ idx2_new, id_arr ++ id_arr_new)
    }.toDF("sample_idx", "label", "dense", "idx0", "idx1", "idx2", "id_arr")
    result
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
          val re = if (seq != null && seq.nonEmpty) for (i <- seq) yield Murmur3Hash.stringHash64(prefix + "#"  + i, 0)
          else Seq(Murmur3Hash.stringHash64(prefix + "#" , 0))
          re.slice(0, 1000)
      }
      case "string" => udf {
        seq: Seq[String] =>
          val re = if (seq != null && seq.nonEmpty) for (i <- seq) yield Murmur3Hash.stringHash64(prefix + "#"  + i, 0)
          else Seq(Murmur3Hash.stringHash64(prefix + "#" , 0))
          re.slice(0, 1000)
      }
    }
  }
}

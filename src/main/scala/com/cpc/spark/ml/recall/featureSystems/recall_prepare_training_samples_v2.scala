package com.cpc.spark.ml.recall.featureSystems

import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.common.Murmur3Hash
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.{udf, _}
import org.apache.spark.sql.{DataFrame, SparkSession}

object recall_prepare_training_samples_v2 {
  Logger.getRootLogger.setLevel(Level.WARN)

  //multi hot 特征默认hash code
  private val default_hash = for (i <- 1 to 37) yield Seq((i.toLong - 1, 0.toLong, Murmur3Hash.stringHash64("m" + i, 0)))

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("recall_prepare_training_samples")
      .enableHiveSupport()
      .getOrCreate()
    val featureName = args(0)
    val curday = args(1)
    val model_version = "adlist-v4"
    val cal1 = Calendar.getInstance()
    cal1.setTime(new SimpleDateFormat("yyyy-MM-dd").parse(s"$curday"))
    cal1.add(Calendar.DATE, -1)
    val oneday = new SimpleDateFormat("yyyy-MM-dd").format(cal1.getTime)

    cal1.add(Calendar.DATE, -1)
    val twoday = new SimpleDateFormat("yyyy-MM-dd").format(cal1.getTime)
    getSample(spark, model_version, featureName, "test", twoday, oneday).repartition(1000)
      .write
      .mode("overwrite")
      .format("tfrecords")
      .option("recordType", "Example")
      .save(s"hdfs://emr-cluster/user/cpc/sample/recall/featureSystem/offlineAuc/$featureName/$oneday")

    cal1.add(Calendar.DATE, -1)
    val threeday = new SimpleDateFormat("yyyy-MM-dd").format(cal1.getTime)
    getSample(spark, model_version, featureName, "train", threeday, twoday).repartition(1000)
      .write
      .mode("overwrite")
      .format("tfrecords")
      .option("recordType", "Example")
      .save(s"hdfs://emr-cluster/user/cpc/sample/recall/featureSystem/offlineAuc/$featureName/$twoday")

    cal1.add(Calendar.DATE, -1)
    val fourday = new SimpleDateFormat("yyyy-MM-dd").format(cal1.getTime)
    getSample(spark, model_version, featureName, "train", fourday, threeday).repartition(1000)
      .write
      .mode("overwrite")
      .format("tfrecords")
      .option("recordType", "Example")
      .save(s"hdfs://emr-cluster/user/cpc/sample/recall/featureSystem/offlineAuc/$featureName/$threeday")

    cal1.add(Calendar.DATE, -1)
    val fiveday = new SimpleDateFormat("yyyy-MM-dd").format(cal1.getTime)
    getSample(spark, model_version, featureName, "train", fiveday, fourday).repartition(1000)
      .write
      .mode("overwrite")
      .format("tfrecords")
      .option("recordType", "Example")
      .save(s"hdfs://emr-cluster/user/cpc/sample/recall/featureSystem/offlineAuc/$featureName/$fourday")

  }

  def getSample(spark: SparkSession, model_version: String, featureName: String, Type: String, date: String, date1: String): DataFrame = {
    import spark.implicits._
    var original_sample: DataFrame = null
    original_sample = spark.read.format("tfrecords").option("recordType", "Example").load(s"hdfs://emr-cluster/user/cpc/aiclk_dataflow/daily/$model_version/$date1/part*").
      select($"sample_idx", $"idx0", $"idx1", $"idx2", $"id_arr", $"label", $"dense", expr("dense[25]").alias("uidhash"))
    val multihot_feature = original_sample.limit(10).cache().select(expr("max(idx1[size(idx1)-1])").alias("idx1")).collect()
    val multihot_feature_number = multihot_feature(0)(0).toString.toInt + 1

    val sample = spark.sql(
      s"""
         |select * from (select *,
         |row_number() over(partition by uid order by hour desc) as row_num from dl_cpc.recall_rec_feature where day='$date') t1
         |where row_num=1
       """.stripMargin)
    val uid_memberid = spark.sql(
      s"""
         |select member_id as uid,device_code as uid_ad from gobblin.qukan_member_info_incre
         |where member_id > 0 and device_code is not null group by member_id,device_code
      """.stripMargin)

    sample.join(uid_memberid, Seq("uid"), "left_outer").createOrReplaceTempView("sample_new")
    val sample_new = spark.sql(
      s"""
         |select COALESCE(uid_ad, uid) as uid_new, * from sample_new
       """.stripMargin).select(hash("f25")($"uid_new").alias("uidhash"), $"slotid9",
      $"slotid10",
      $"slotid11",
      $"slotid12",
      $"slotid13",
      $"slotid14",
      $"slotid15",
      $"slotid16",
      $"slotid17",
      $"slotid18",
      $"slotid19",
      $"slotid20",
      $"slotid21",
      $"slotid143",
      $"slotid206",
      $"slotid207",
      $"slotid208",
      $"slotid209",
      $"slotid210",
      $"slotid211",
      $"slotid212",
      $"slotid213",
      $"slotid214",
      $"slotid215",
      $"slotid219",
      $"slotid220",
      $"slotid221",
      $"slotid222",
      $"slotid223",
      $"slotid224",
      $"slotid225"
    )
    val result = original_sample.join(sample_new, Seq("uidhash"), "left_outer").
      select($"sample_idx", $"idx0", $"idx1", $"idx2", $"id_arr", $"label", $"dense",
        mkSparseFeature_m(multihot_feature_number)(array(
      hashSeq("m9", "string")($"slotid9").alias("slotid9"),
      hashSeq("m10", "string")($"slotid10").alias("slotid10"),
      hashSeq("m11", "string")($"slotid11").alias("slotid11"),
      hashSeq("m12", "string")($"slotid12").alias("slotid12"),
      hashSeq("m13", "string")($"slotid13").alias("slotid13"),
      hashSeq("m14", "string")($"slotid14").alias("slotid14"),
      hashSeq("m15", "string")($"slotid15").alias("slotid15"),
      hashSeq("m16", "string")($"slotid16").alias("slotid16"),
      hashSeq("m17", "string")($"slotid17").alias("slotid17"),
      hashSeq("m18", "string")($"slotid18").alias("slotid18"),
      hashSeq("m19", "string")($"slotid19").alias("slotid19"),
      hashSeq("m20", "string")($"slotid20").alias("slotid20"),
      hashSeq("m21", "string")($"slotid21").alias("slotid21"),
      hashSeq("m143", "string")($"slotid143").alias("slotid143"),
      hashSeq("m206", "string")($"slotid206").alias("slotid206"),
      hashSeq("m207", "string")($"slotid207").alias("slotid207"),
      hashSeq("m208", "string")($"slotid208").alias("slotid208"),
      hashSeq("m209", "string")($"slotid209").alias("slotid209"),
      hashSeq("m210", "string")($"slotid210").alias("slotid210"),
      hashSeq("m211", "string")($"slotid211").alias("slotid211"),
      hashSeq("m212", "string")($"slotid212").alias("slotid212"),
      hashSeq("m213", "string")($"slotid213").alias("slotid213"),
      hashSeq("m214", "string")($"slotid214").alias("slotid214"),
      hashSeq("m215", "string")($"slotid215").alias("slotid215"),
      hashSeq("m219", "string")($"slotid219").alias("slotid219"),
      hashSeq("m220", "string")($"slotid220").alias("slotid220"),
      hashSeq("m221", "string")($"slotid221").alias("slotid221"),
      hashSeq("m222", "string")($"slotid222").alias("slotid222"),
      hashSeq("m223", "string")($"slotid223").alias("slotid223"),
      hashSeq("m224", "string")($"slotid224").alias("slotid224"),
      hashSeq("m225", "string")($"slotid225").alias("slotid225")
    )).alias("sparse")).select($"sample_idx", $"idx0", $"idx1", $"idx2", $"id_arr", $"label", $"dense",
      $"sparse".getField("_1").alias("idx0_new"),
      $"sparse".getField("_2").alias("idx1_new"),
      $"sparse".getField("_3").alias("idx2_new"),
      $"sparse".getField("_4").alias("id_arr_new")
    ).rdd.map{
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

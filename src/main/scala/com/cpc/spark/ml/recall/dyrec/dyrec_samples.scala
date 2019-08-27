package com.cpc.spark.ml.recall.dyrec

import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.common.Murmur3Hash
import com.cpc.spark.ml.dnn.Utils.CommonUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.{udf, _}
import org.apache.spark.sql.{DataFrame, SparkSession}

object dyrec_samples {
  Logger.getRootLogger.setLevel(Level.WARN)

  //multi hot 特征默认hash code
  private val default_hash = for (i <- 1 to 37) yield Seq((i.toLong - 1, 0.toLong, Murmur3Hash.stringHash64("m" + i, 0)))

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("dyrec_samples")
      .enableHiveSupport()
      .getOrCreate()
    val curday = args(0)
    val model_version = "cpc_tensorflow_example_v2_half"
    val cal1 = Calendar.getInstance()
    cal1.setTime(new SimpleDateFormat("yyyy-MM-dd").parse(s"$curday"))
    val today = args(0)
    val hour = args(1)
    val minute = args(2)
    print(today + " " + hour + " " + minute)
    cal1.add(Calendar.DATE, -1)
    val oneday = new SimpleDateFormat("yyyy-MM-dd").format(cal1.getTime)
    var Type = "2"
    if(minute == "30") {
      Type = "1"
    } else if (minute == "00") {
      Type = "0"
    }

    cal1.add(Calendar.DATE, -1)
    val twoday = new SimpleDateFormat("yyyy-MM-dd").format(cal1.getTime)
    getSample(spark, model_version, minute, today, oneday, hour).repartition(1000)
      .write
      .mode("overwrite")
      .format("tfrecords")
      .option("recordType", "Example")
      .save(s"hdfs://emr-cluster/user/cpc/aiclk_dataflow/realtime-${minute}/adlist-v4dyrec/$today/$hour/")
    val CountPathTmpName = s"hdfs://emr-cluster/user/cpc/aiclk_dataflow/daily/adlist-v4dyrec/tmp/"
    val CountPathName = s"hdfs://emr-cluster/user/cpc/aiclk_dataflow/realtime-${minute}/adlist-v4dyrec/$today/$hour/count"
    val count = spark.read.format("tfrecords").option("recordType", "Example").load(s"hdfs://emr-cluster/user/cpc/aiclk_dataflow/realtime-${minute}/adlist-v4dyrec/$today/$hour/part*").count()
    CommonUtils.writeCountToFile(spark, count, CountPathTmpName, CountPathName)
  }

  def getSample(spark: SparkSession, model_version: String, minute: String, today: String, oneday: String, hour: String): DataFrame = {
    import spark.implicits._
    var original_sample: DataFrame = null
    original_sample = spark.read.format("tfrecords").option("recordType", "Example").load(s"hdfs://emr-cluster/user/cpc/aiclk_dataflow/realtime-${minute}/adlist-v4-up/$today/$hour/part*").
      select($"sample_idx", $"idx0", $"idx1", $"idx2", $"id_arr", $"label", $"dense", expr("dense[25]").alias("uidhash"))
    val multihot_feature = original_sample.limit(10).cache().select(expr("max(idx1[size(idx1)-1])").alias("idx1")).collect()
    val multihot_feature_number = multihot_feature(0)(0).toString.toInt + 1
    val sample = spark.sql(
      s"""
         |select * from (select *,
         |row_number() over(partition by uid order by day, hour desc) as row_num from dl_cpc.recall_rec_feature where (day='$oneday' and hour>='$hour') or (day='$today' and hour<='$hour')) t1
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
      hashSeq("ud102", "string")($"slotid9").alias("slotid9"),
      hashSeq("ud103", "string")($"slotid10").alias("slotid10"),
      hashSeq("ud104", "string")($"slotid11").alias("slotid11"),
      hashSeq("ud105", "string")($"slotid12").alias("slotid12"),
      hashSeq("ud106", "string")($"slotid13").alias("slotid13"),
      hashSeq("ud107", "string")($"slotid14").alias("slotid14"),
      hashSeq("ud108", "string")($"slotid15").alias("slotid15"),
      hashSeq("ud109", "string")($"slotid16").alias("slotid16"),
      hashSeq("ud110", "string")($"slotid17").alias("slotid17"),
      hashSeq("ud111", "string")($"slotid18").alias("slotid18"),
      hashSeq("ud112", "string")($"slotid19").alias("slotid19"),
      hashSeq("ud113", "string")($"slotid20").alias("slotid20"),
      hashSeq("ud114", "string")($"slotid21").alias("slotid21"),
      hashSeq("ud115", "string")($"slotid143").alias("slotid143"),
      hashSeq("ud116", "string")($"slotid206").alias("slotid206"),
      hashSeq("ud117", "string")($"slotid207").alias("slotid207"),
      hashSeq("ud118", "string")($"slotid208").alias("slotid208"),
      hashSeq("ud119", "string")($"slotid209").alias("slotid209"),
      hashSeq("ud120", "string")($"slotid210").alias("slotid210"),
      hashSeq("ud121", "string")($"slotid211").alias("slotid211"),
      hashSeq("ud122", "string")($"slotid212").alias("slotid212"),
      hashSeq("ud123", "string")($"slotid213").alias("slotid213"),
      hashSeq("ud124", "string")($"slotid214").alias("slotid214"),
      hashSeq("ud125", "string")($"slotid215").alias("slotid215"),
      hashSeq("ud126", "string")($"slotid219").alias("slotid219"),
      hashSeq("ud127", "string")($"slotid220").alias("slotid220"),
      hashSeq("ud128", "string")($"slotid221").alias("slotid221"),
      hashSeq("ud129", "string")($"slotid222").alias("slotid222"),
      hashSeq("ud130", "string")($"slotid223").alias("slotid223"),
      hashSeq("ud131", "string")($"slotid224").alias("slotid224"),
      hashSeq("ud132", "string")($"slotid225").alias("slotid225")
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

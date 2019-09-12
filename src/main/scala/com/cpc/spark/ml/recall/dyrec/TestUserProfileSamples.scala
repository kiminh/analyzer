package com.cpc.spark.ml.recall.dyrec

import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.common.Murmur3Hash
import com.cpc.spark.ml.dnn.Utils.CommonUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.{array, expr, udf}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * created by xiongyao on 2019/9/4
  */
object TestUserProfileSamples {

  Logger.getRootLogger.setLevel(Level.WARN)

  //multi hot 特征默认hash code
  private val default_hash = for (i <- 1 to 37) yield Seq((i.toLong - 1, 0.toLong, Murmur3Hash.stringHash64("m" + i, 0)))

  def main(args: Array[String]): Unit = {
    var spark = SparkSession.builder().appName("userprofile_samples").enableHiveSupport().getOrCreate()
    val curday = args(0)
    var model_version = "cpc_tensorflow_example_half"
    val cal = Calendar.getInstance()
    cal.setTime(new SimpleDateFormat("yyyy-MM-dd").parse(s"$curday"))
    val today = args(0)
    val hour = args(1)
    val minute = args(2)
    print(today + " " + hour + " " + minute)
    cal.add(Calendar.DATE, -1)
    val oneday = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime)
    var Type = "2"
    if(minute == "15") {
      Type = "0"
    } else if (minute == "45") {
      Type = "1"
    }

    cal.add(Calendar.DATE, -1)
    val twoday = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime)

    getSample(spark, model_version, Type, today, oneday, hour).repartition(1000)

  }

  def getSample(spark: SparkSession, model_version: String, Type: String, today: String, oneday: String, hour: String): DataFrame = {
    import spark.implicits._
    var original_sample: DataFrame = null
    original_sample = spark.read.format("tfrecords").option("recordType", "Example").load(s"hdfs://emr-cluster2ns2/user/$model_version/$today/$hour/$Type/part*").
      select($"sample_idx", $"idx0", $"idx1", $"idx2", $"id_arr", $"label", $"dense", expr("dense[25]").alias("uidhash"))
    original_sample.show(10,false)
    val multihot_feature = original_sample.limit(10).cache().select(expr("max(idx1[size(idx1)-1])").alias("idx1")).collect()
    val multihot_feature_number = multihot_feature(0)(0).toString.toInt + 1

    println("multihot_feature_number",multihot_feature_number)

    val sample = spark.sql(
      s"""
         select
         |   t0.uid as uid,
         |   split(t1.feature_onehot1,',') as fea1,
         |   split(t1.feature_onehot2,',') as fea2,
         |   split(t1.feature_onehot3,',') as fea3,
         |   t1.feature_multihot1 as fea4,
         |   t1.feature_multihot2 as fea5,
         |   t1.feature_multihot3 as fea6,
         |   split(t2.feature_onehot1,',') as fea7,
         |   split(t2.feature_onehot2,',') as fea8,
         |   split(t2.feature_onehot3,',') as fea9,
         |   split(t3.feature_onehot1,',') as fea10,
         |   split(t3.feature_onehot2,',') as fea11,
         |   split(t3.feature_onehot3,',') as fea12
         |   from
         |     (
         |    select
         |      distinct uid
         |    from
         |      dl_cpc.recall_test_feature
         |    where
         |      dt = '$oneday'
         |      and feature_name in ( 'work_time','rank_coin_num','user_profile_features')
         |  ) t0
         |  left join
         |  (
         |    select
         |      *
         |    from
         |      dl_cpc.recall_test_feature
         |    where
         |      dt = '$oneday'
         |      and feature_name = 'user_profile_features'
         |  ) t1
         |  on t0.uid=t1.uid
         |  left join (
         |    select
         |      *
         |    from
         |      dl_cpc.recall_test_feature
         |    where
         |      dt = '$oneday'
         |      and feature_name = 'work_time'
         |  ) t2
         |  on t0.uid=t2.uid
         |  left join (
         |    select
         |      *
         |    from
         |      dl_cpc.recall_test_feature
         |    where
         |      dt = '$oneday'
         |      and feature_name = 'rank_coin_num'
         |  ) t3
         |  on t0.uid=t3.uid
       """.stripMargin)

    sample.show(10,false)
    sample.createOrReplaceTempView("sample_new")

    val sample_new = spark.sql(
      s"""
         |select * from sample_new
       """.stripMargin).select(hash("f25")($"uid").alias("uidhash"),
      $"fea1",
      $"fea2",
      $"fea3",
      $"fea4",
      $"fea5",
      $"fea6",
      $"fea7",
      $"fea8",
      $"fea9",
      $"fea10",
      $"fea11",
      $"fea12"
    )
    println("sample_new:")
    sample_new.show(10,false)

    val df = original_sample.join(sample_new, Seq("uidhash"), "left_outer").
      select($"sample_idx", $"idx0", $"idx1", $"idx2", $"id_arr", $"label", $"dense",
        mkSparseFeature_m(multihot_feature_number)(array(
          hashSeq("ud139", "string")($"fea1").alias("fea1"),
          hashSeq("ud140", "string")($"fea2").alias("fea2"),
          hashSeq("ud141", "string")($"fea3").alias("fea3"),
          hashSeq("ud142", "string")($"fea4").alias("fea4"),
          hashSeq("ud143", "string")($"fea5").alias("fea5"),
          hashSeq("ud144", "string")($"fea6").alias("fea6"),
          hashSeq("ud136", "string")($"fea7").alias("fea7"),
          hashSeq("ud137", "string")($"fea8").alias("fea8"),
          hashSeq("ud138", "string")($"fea9").alias("fea9"),
          hashSeq("ud133", "string")($"fea10").alias("fea10"),
          hashSeq("ud134", "string")($"fea11").alias("fea11"),
          hashSeq("ud135", "string")($"fea12").alias("fea12")
        )).alias("sparse")).select($"sample_idx", $"idx0", $"idx1", $"idx2", $"id_arr", $"label", $"dense",
      $"sparse".getField("_1").alias("idx0_new"),
      $"sparse".getField("_2").alias("idx1_new"),
      $"sparse".getField("_3").alias("idx2_new"),
      $"sparse".getField("_4").alias("id_arr_new")
    )
    println("join result")
    df.show(10,false)

    val resultDF = df.rdd.map{
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
    }
    println("merge result")
    resultDF.toDF("sample_idx", "label", "dense", "idx0", "idx1", "idx2", "id_arr").show(10,false)
    val result = resultDF.toDF("sample_idx", "label", "dense", "idx0", "idx1", "idx2", "id_arr")
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

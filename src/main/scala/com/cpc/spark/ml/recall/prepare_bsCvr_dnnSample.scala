package com.cpc.spark.ml.recall

import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.common.Murmur3Hash
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{array, split, udf}
import org.apache.spark.storage.StorageLevel

object prepare_bsCvr_dnnSample {
  Logger.getRootLogger.setLevel(Level.WARN)

  //multi hot 特征默认hash code
  private val default_hash = for (i <- 1 to 37) yield Seq((i - 1, 0, Murmur3Hash.stringHash64("m" + i, 0)))

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("dnn sample")
      .enableHiveSupport()
      .getOrCreate()
    val date = args(0)
    //    val hour = args(1)

    val train = getSample(spark, date).persist(StorageLevel.MEMORY_AND_DISK)

    val n = train.count()
    println("训练数据：total = %d, 正比例 = %.4f".format(n, train.where("label=array(1,0)").count.toDouble / n))

    val sampleDay = getDay(date, 1)

    val Array(traindata, testdata) = train.randomSplit(Array(0.95, 0.05), 1)

    traindata.repartition(1000)
      .write
      .mode("overwrite")
      .format("tfrecords")
      .option("recordType", "Example")
      .save(s"/user/cpc/sample/recall/dnn_recall_cvr_v1/dnntrain-$sampleDay")
    //train.take(10).foreach(println)

    testdata.repartition(100)
      .write
      .mode("overwrite")
      .format("tfrecords")
      .option("recordType", "Example")
      .save(s"/user/cpc/sample/recall/dnn_recall_cvr_v1/dnntest-$sampleDay")

    train.unpersist()
  }

  def getSample(spark: SparkSession, date: String): DataFrame = {
    import spark.implicits._
    val day = getDay(date, 1)
    val dayFeature = getDay(date, 2)

    val behavior_data = spark.read.parquet(s"/user/cpc/features/adBehaviorFeature/$dayFeature")

    val uidRequest = spark.read.parquet("/user/cpc/features/timeDistributionFeature").
      select($"uid", hashSeq("m26", "string")($"request").alias("m26"))

    val profileData = spark.read.parquet("/user/cpc/qtt-lookalike-sample/v1").
      select($"did".alias("uid"), hashSeq("m1", "string")($"apps._1").alias("m1"),
         hashSeq("m24", "string")($"words").alias("m24"),
         hashSeq("m25", "string")($"terms").alias("m25")
      )

    val sql =
      s"""
         |select
         |  if(iscvr>0, array(1,0), array(0,1)) as label,
         |  media_type, media_appsid as mediaid,
         |  ext['channel'].int_value as channel,
         |  ext['client_type'].string_value as sdk_type,
         |  adslot_type, adslotid,
         |  adtype, interaction, bid, ideaid, unitid, planid, userid,
         |  ext_int['is_new_ad'] as is_new_ad, ext['adclass'].int_value as adclass, ext['charge_type'] as charge_type,
         |  ext_int['siteid'] as site_id,
         |  os, network, ext['phone_price'].int_value as phone_price,
         |  ext['brand_title'].string_value as brand,
         |  province, city, ext['city_level'].int_value as city_level,
         |  uid, age, sex, ext_string['dtu_id'] as dtu_id,
         |  a.hour
         |from
         |  (select *
         |from dl_cpc.cpc_union_log where `date` = '$day'
         |  and isclick = 1 and ideaid > 0
         |  and media_appsid in ("80000001", "80000002", "80000006", "800000062", "80000064", "80000066","80000141")
         |  and uid not like "%.%"
         |  and uid not like "%000000%"
         |) a
         |inner join
         |(select searchid, label2 as iscvr from dl_cpc.ml_cvr_feature_v1
         |  WHERE `date` = '$day'
         |) b on a.searchid = b.searchid
      """.stripMargin
    println("--------------------------------")
    println(sql)
    println("--------------------------------")

    spark.sql(sql)
      .join(profileData, Seq("uid"), "leftouter")
      .join(uidRequest, Seq("uid"), "leftouter")
      .join(behavior_data, Seq("uid"), "leftouter")
      .select($"label",

        //hash("f1")($"media_type").alias("f1"),
        //hash("f2")($"mediaid").alias("f2"),
        //hash("f3")($"channel").alias("f3"),
        //hash("f4")($"sdk_type").alias("f4"),
        hash("f1")($"adslot_type").alias("f1"),
        //hash("f6")($"adslotid").alias("f6"),
        hash("f2")($"sex").alias("f2"),
        //hash("f8")($"dtu_id").alias("f8"),
        hash("f3")($"planid").alias("f3"),
        //hash("f10")($"interaction").alias("f10"),
        //hash("f11")($"bid").alias("f11"),
        //hash("f4")($"ideaid").alias("f4"),
        hash("f4")($"unitid").alias("f4"),
        //hash("f6")($"planid").alias("f6"),
        hash("f5")($"userid").alias("f5"),
        //hash("f16")($"is_new_ad").alias("f16"),
        hash("f6")($"charge_type").alias("f6"),
        hash("f7")($"os").alias("f7"),
        hash("f8")($"phone_price").alias("f8"),
        //hash("f20")($"network").alias("f20"),
        hash("f9")($"brand").alias("f9"),
        hash("f10")($"province").alias("f10"),
        hash("f11")($"city").alias("f11"),
        hash("f12")($"city_level").alias("f12"),
        hash("f13")($"uid").alias("f13"),
        hash("f14")($"age").alias("f14"),
        //hash("f15")($"age").alias("f15"),
        //hash("f28")($"hour").alias("f28"),

        array($"m1", $"m2", $"m3", $"m4", $"m5", $"m6", $"m7", $"m8", $"m9", $"m10",
          $"m11", $"m12", $"m13", $"m14", $"m15",$"m16", $"m17", $"m18", $"m19", $"m20",$"m21", $"m22",$"m23", $"m24",$"m25",$"m26")
          .alias("raw_sparse")
      )

      .select(array($"f1", $"f2", $"f3", $"f4", $"f5", $"f6", $"f7", $"f8", $"f9",
        $"f10", $"f11", $"f12", $"f13", $"f14").alias("dense"),
        //mkSparseFeature($"apps", $"ideaids").alias("sparse"), $"label"
        //mkSparseFeature1($"m1").alias("sparse"), $"label"
        mkSparseFeature_m($"raw_sparse").alias("sparse"),
        $"label"
      )

      .select(
        $"label",
        $"dense",
        $"sparse".getField("_1").alias("idx0"),
        $"sparse".getField("_2").alias("idx1"),
        $"sparse".getField("_3").alias("idx2"),
        $"sparse".getField("_4").alias("id_arr")
      )

      .rdd.zipWithUniqueId()
      .map { x =>
        (x._2, x._1.getAs[Seq[Int]]("label"), x._1.getAs[Seq[Long]]("dense"),
          x._1.getAs[Seq[Int]]("idx0"), x._1.getAs[Seq[Int]]("idx1"),
          x._1.getAs[Seq[Int]]("idx2"), x._1.getAs[Seq[Long]]("id_arr"))
      }
      .toDF("sample_idx", "label", "dense", "idx0", "idx1", "idx2", "id_arr")
  }

  /**
    * 获取时间序列
    *
    * @param startdate : 日期
    * @param day1      ：日期之前day1天作为开始日期
    * @param day2      ：日期序列数量
    * @return
    */
  def getDays(startdate: String, day1: Int = 0, day2: Int): String = {
    val format = new SimpleDateFormat("yyyy-MM-dd")
    val cal = Calendar.getInstance()
    cal.setTime(format.parse(startdate))
    cal.add(Calendar.DATE, -day1)
    var re = Seq(format.format(cal.getTime))
    for (_ <- 1 until day2) {
      cal.add(Calendar.DATE, -1)
      re = re :+ format.format(cal.getTime)
    }
    re.mkString("','")
  }

  /**
    * 获取时间
    *
    * @param startdate ：开始日期
    * @param day       ：开始日期之前day天
    * @return
    */
  def getDay(startdate: String, day: Int): String = {
    val format = new SimpleDateFormat("yyyy-MM-dd")
    val cal = Calendar.getInstance()
    cal.setTime(format.parse(startdate))
    cal.add(Calendar.DATE, -day)
    format.format(cal.getTime)
  }

  def getUidApp(spark: SparkSession, date: String): DataFrame = {
    import spark.implicits._
    spark.sql(
      s"""
         |select * from dl_cpc.cpc_user_installed_apps where `load_date` = date_add('$date', -1)
      """.stripMargin).rdd
      .map(x => (x.getAs[String]("uid"), x.getAs[Seq[String]]("pkgs")))
      .reduceByKey(_ ++ _)
      .map(x => (x._1, x._2.distinct))
      .toDF("uid", "pkgs")
  }

  /**
    * 获取hash code
    *
    * @param prefix ：前缀
    * @return
    */
  private def hash(prefix: String) = udf {
    num: String =>
      if (num != null) Murmur3Hash.stringHash64(prefix + num, 0) else Murmur3Hash.stringHash64(prefix, 0)
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

  private def mkSparseFeature_m = udf {
    features: Seq[Seq[Long]] =>
      var i = 0
      var re = Seq[(Int, Int, Long)]()
      for (feature <- features) {
        re = re ++
          (if (feature != null) feature.zipWithIndex.map(x => (i, x._2, x._1)) else default_hash(i))
        i = i + 1
      }
      val c = re.map(x => (0, x._1, x._2, x._3))
      (c.map(_._1), c.map(_._2), c.map(_._3), c.map(_._4))
  }

}

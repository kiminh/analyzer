package com.cpc.spark.ml.dnn.trash

import com.cpc.spark.common.Murmur3Hash
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * 用于dnn线上服务从  原始id -> hash id -> 预测值 的校验工作
  * created time : 2018/10/11 11:09
  *
  * @author zhj
  * @version 1.0
  *
  */
@deprecated
object DNNSampleForTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("dnn sample")
      .enableHiveSupport()
      .getOrCreate()

    val date = args(0)
    val tdate = args(1)

    val train = getSample(spark, date).persist()
    val n = train.count()
    println("训练数据：total = %d, 正比例 = %.4f".format(n, train.where("label=array(1,0)").count.toDouble / n))

    train.repartition(100)
      .write
      .mode("overwrite")
      .format("tfrecords")
      .option("recordType", "Example")
      .save("/user/cpc/zhj/tmp/dnntrain-" + date)
    println("train size", train.count())

    val test = getSample(spark, tdate, train = false).randomSplit(Array(0.97, 0.03), 123L)(1)
    val tn = test.count
    println("测试数据：total = %d, 正比例 = %.4f".format(tn, test.where("label=array(1,0)").count.toDouble / tn))

    test.repartition(100)
      .write
      .mode("overwrite")
      .format("tfrecords")
      .option("recordType", "Example")
      .save("/user/cpc/zhj/tmp/dnntest-" + tdate)
    test.take(10).foreach(println)
  }

  def getSample(spark: SparkSession, date: String, train: Boolean = true): DataFrame = {
    import spark.implicits._

    val userAppIdx = getUidApp(spark, date)

    val onehot_features = Seq("media_type", "mediaid", "channel", "sdk_type", "adslot_type",
      "adslotid", "sex", "dtu_id", "adtype", "interaction", "bid", "ideaid",
      "unitid", "planid", "userid", "is_new_ad", "adclass", "site_id", "os",
      "network", "phone_price", "brand", "province", "city", "city_level", "uid", "age")

    val multihot_features = Seq("pkgs")

    val sql =
      s"""
         |select if(isclick>0, array(1,0), array(0,1)) as label,
         |  media_type, media_appsid as mediaid,
         |  ext['channel'].int_value as channel,
         |  ext['client_type'].string_value as sdk_type,
         |
         |  adslot_type, adslotid,
         |
         |  adtype, interaction, bid, ideaid, unitid, planid, userid,
         |  ext_int['is_new_ad'] as is_new_ad, ext['adclass'].int_value as adclass,
         |  ext_int['siteid'] as site_id,
         |
         |  os, network, ext['phone_price'].int_value as phone_price,
         |  ext['brand_title'].string_value as brand,
         |
         |  province, city, ext['city_level'].int_value as city_level,
         |
         |  uid, age, sex, ext_string['dtu_id'] as dtu_id,
         |
         |  row_number() over(order by age) as rn
         |
         |from dl_cpc.cpc_union_log where `date`='$date' and hour=10
         |  and isshow = 1 and ideaid > 0 and adslot_type = 1
         |  and media_appsid in ("80000001", "80000002")
         |  and uid not like "%.%"
         |  and uid not like "%000000%"
         |
      """.stripMargin
    println(sql)

    val re = spark.sql(sql)
      .join(userAppIdx, Seq("uid"), "leftouter")

    val path = if (train) s"/user/cpc/zhj/tmp/sampletrain-$date" else
      s"/user/cpc/zhj/tmp/sampletest- $date"

    re.select(
      expr(s"array(${onehot_features.mkString(",")}) as raw_dense"),
      expr(s"array(${multihot_features.mkString(",")}) as raw_sparse"),
      $"rn".alias("sample_idx")
    ).write.mode("overwrite").parquet(path)

    re.select($"label",

      $"rn".alias("sample_idx"),

      hash("f1")($"media_type").alias("f1"),
      hash("f2")($"mediaid").alias("f2"),
      hash("f3")($"channel").alias("f3"),
      hash("f4")($"sdk_type").alias("f4"),
      hash("f5")($"adslot_type").alias("f5"),
      hash("f6")($"adslotid").alias("f6"),
      hash("f7")($"sex").alias("f7"),
      hash("f8")($"dtu_id").alias("f8"),
      hash("f9")($"adtype").alias("f9"),
      hash("f10")($"interaction").alias("f10"),
      hash("f11")($"bid").alias("f11"),
      hash("f12")($"ideaid").alias("f12"),
      hash("f13")($"unitid").alias("f13"),
      hash("f14")($"planid").alias("f14"),
      hash("f15")($"userid").alias("f15"),
      hash("f16")($"is_new_ad").alias("f16"),
      hash("f17")($"adclass").alias("f17"),
      hash("f18")($"site_id").alias("f18"),
      hash("f19")($"os").alias("f19"),
      hash("f20")($"network").alias("f20"),
      hash("f21")($"phone_price").alias("f21"),
      hash("f22")($"brand").alias("f22"),
      hash("f23")($"province").alias("f23"),
      hash("f24")($"city").alias("f24"),
      hash("f25")($"city_level").alias("f25"),
      hash("f26")($"uid").alias("f26"),
      hash("f27")($"age").alias("f27"),

      hashSeq("m1", "string")($"pkgs").alias("m1"))

      .select(array($"f1", $"f2", $"f3", $"f4", $"f5", $"f6", $"f7", $"f8", $"f9",
        $"f10", $"f11", $"f12", $"f13", $"f14", $"f15", $"f16", $"f17", $"f18", $"f19",
        $"f20", $"f21", $"f22", $"f23", $"f24", $"f25", $"f26", $"f27").alias("dense"),
        //mkSparseFeature($"apps", $"ideaids").alias("sparse"), $"label"
        mkSparseFeature1($"m1").alias("sparse"), $"label",
        $"sample_idx"
      )

      .select(
        $"sample_idx",
        $"label",
        $"dense",
        $"sparse".getField("_1").alias("idx0"),
        $"sparse".getField("_2").alias("idx1"),
        $"sparse".getField("_3").alias("idx2"),
        $"sparse".getField("_4").alias("id_arr")
      )
  }


  def getUidApp(spark: SparkSession, date: String): DataFrame = {
    import spark.implicits._
    spark.sql(
      """
        |select * from dl_cpc.cpc_user_installed_apps where `load_date` = "%s"
      """.stripMargin.format(date)).rdd
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

  private val mkSparseFeature = udf {
    (apps: Seq[Long], ideaids: Seq[Long]) =>
      val a = apps.zipWithIndex.map(x => (0, x._2, x._1))
      val b = ideaids.zipWithIndex.map(x => (1, x._2, x._1))
      val c = (a ++ b).map(x => (0, x._1, x._2, x._3))
      (c.map(_._1), c.map(_._2), c.map(_._3), c.map(_._4))
  }

  private val mkSparseFeature1 = udf {
    apps: Seq[Long] =>
      val c = apps.zipWithIndex.map(x => (0, 0, x._2, x._1))
      (c.map(_._1), c.map(_._2), c.map(_._3), c.map(_._4))
  }
}

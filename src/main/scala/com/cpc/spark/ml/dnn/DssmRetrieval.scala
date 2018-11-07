package com.cpc.spark.ml.dnn

import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.common.Murmur3Hash
import com.cpc.spark.ml.dnn.DssmRetrieval.hash
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.{array, udf}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * 基于DnnCtrV3, 把特征拆分成用户特征和广告特征
  *
  * created time : 2018/11/06 15：55
  *
  * @author huazhenhao
  * @version 1.0
  *
  */
object DssmRetrieval {

  Logger.getRootLogger.setLevel(Level.WARN)

  //multi hot 特征默认hash code
  private val default_hash = for (i <- 1 to 37) yield Seq((i - 1, 0, Murmur3Hash.stringHash64("m" + i, 0)))

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("dssm")
      .enableHiveSupport()
      .getOrCreate()

    val date = args(0)
    val sampleRate = args(1).toDouble

    val train = getSample(spark, date, sampleRate)

    val n = train.count()
    println("训练数据：total = %d, 正比例 = %.4f".format(n, train.where("label=array(1,0)").count.toDouble / n))

    train.repartition(100)
      .write
      .mode("overwrite")
      .format("tfrecords")
      .option("recordType", "Example")
      .save("/user/cpc/hzh/dssm/train-v0/" + date)
  }

  def getSample(spark: SparkSession, date: String, sampleRate: Double): DataFrame = {
    import spark.implicits._

    val userAppIdx = getUidApp(spark, date)
      .select($"uid", hashSeq("um1", "string")($"pkgs").alias("um1"))

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
         |  split(ext['materialid'].string_value, ',') as material_ids,
         |
         |  hour
         |
         |from dl_cpc.cpc_union_log where `date` = '$date'
         |  and isshow = 1 and ideaid > 0 and adslot_type = 1
         |  and media_appsid in ("80000001", "80000002")
         |  and uid not like "%.%"
         |  and uid not like "%000000%"
         |  and uid > 0
      """.stripMargin
    println("--------------------------------")
    println(sql)
    println("--------------------------------")

    //    val behavior_data = spark.read.parquet("/user/cpc/zhj/behaviorV3")

    val re =
      spark.sql(sql).sample(false, sampleRate).withColumn("am1", hashSeq("am1", "string")($"material_ids"))
        .join(userAppIdx, Seq("uid"), "leftouter")
    //        .join(behavior_data, Seq("uid"), "leftouter")

    re.select($"label",
      // user index
      hash("u1")($"uid").alias("u1"),

      // user feature
      hash("u2")($"os").alias("u2"),
      hash("u3")($"network").alias("u3"),
      hash("u4")($"phone_price").alias("u4"),
      hash("u5")($"brand").alias("u5"),
      hash("u6")($"province").alias("u6"),
      hash("u7")($"city").alias("u7"),
      hash("u8")($"city_level").alias("u8"),
      hash("u9")($"age").alias("u9"),
      hash("u10")($"sex").alias("u10"),

      // user multi-hot
      array($"um1").alias("u_sparse_raw"),

      // ad index
      hash("a1")($"ideaid").alias("a1"),

      // ad feature
      hash("a2")($"unitid").alias("a2"),
      hash("a3")($"adtype").alias("a3"),
      hash("a4")($"interaction").alias("a4"),
      hash("a5")($"bid").alias("a5"),
      hash("a6")($"planid").alias("a6"),
      hash("a7")($"userid").alias("a7"),
      hash("a8")($"adclass").alias("a8"),
      hash("a9")($"site_id").alias("a9"),

      // ad multi-hot
      array($"am1").alias("ad_sparse_raw")

    )

      .select(
        array($"u1", $"u2", $"u3", $"u4", $"u5", $"u6", $"u7", $"u8", $"u9", $"u10").alias("u_dense"),
        mkSparseFeature_m($"u_sparse_raw").alias("u_sparse"),
        array($"a1", $"a2", $"a3", $"a4", $"a5", $"a6", $"a7", $"a8", $"a9").alias("ad_dense"),
        mkSparseFeature_m($"ad_sparse_raw").alias("ad_sparse"),
        $"label"
      )
      .select(
        $"label",
        $"u_dense",
        $"u_sparse".getField("_1").alias("u_idx0"),
        $"u_sparse".getField("_2").alias("u_idx1"),
        $"u_sparse".getField("_3").alias("u_idx2"),
        $"u_sparse".getField("_4").alias("u_id_arr"),
        $"ad_dense",
        $"ad_sparse".getField("_1").alias("ad_idx0"),
        $"ad_sparse".getField("_2").alias("ad_idx1"),
        $"ad_sparse".getField("_3").alias("ad_idx2"),
        $"ad_sparse".getField("_4").alias("ad_id_arr")
      )
      .rdd.zipWithUniqueId()
      .map { x =>
        (x._2,
          x._1.getAs[Seq[Int]]("label"),
          x._1.getAs[Seq[Long]]("u_dense"),
          x._1.getAs[Seq[Int]]("u_idx0"),
          x._1.getAs[Seq[Int]]("u_idx1"),
          x._1.getAs[Seq[Int]]("u_idx2"),
          x._1.getAs[Seq[Long]]("u_id_arr"),
          x._1.getAs[Seq[Long]]("ad_dense"),
          x._1.getAs[Seq[Int]]("ad_idx0"),
          x._1.getAs[Seq[Int]]("ad_idx1"),
          x._1.getAs[Seq[Int]]("ad_idx2"),
          x._1.getAs[Seq[Long]]("ad_id_arr")
        )
      }
      .toDF("sample_idx", "label",
        "u_dense", "u_idx0", "u_idx1", "u_idx2", "u_id_arr",
        "ad_dense", "ad_idx0", "ad_idx1", "ad_idx2", "ad_id_arr")
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

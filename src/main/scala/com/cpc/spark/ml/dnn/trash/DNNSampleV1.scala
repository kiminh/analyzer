package com.cpc.spark.ml.dnn.trash

import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.common.Murmur3Hash
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

@deprecated
object DNNSampleV1 {
  Logger.getRootLogger.setLevel(Level.WARN)

  private var trainLog = Seq[String]()

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("dnn sample")
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._
    val date = args(0)
    val hour = args(1)

    val default_hash_uid = Murmur3Hash.stringHash64("f26", 0)

    val rawtrain = getSample(spark, date, hour).withColumn("uid", $"dense" (25)).persist()

    rawtrain.printSchema()

    val uid = rawtrain.select("uid")
      .groupBy("uid").count()

    val train = rawtrain.join(uid, Seq("uid"), "left")
      .select($"sample_idx", $"label",
        getNewDense(25, default_hash_uid)($"dense", $"count" < 2).alias("dense"),
        $"idx0", $"idx1", $"idx2", $"id_arr")

    val n = train.count()
    println("训练数据：total = %d, 正比例 = %.4f".format(n, train.where("label=array(1,0)").count.toDouble / n))

    train.repartition(100)
      .write
      .mode("overwrite")
      .format("tfrecords")
      .option("recordType", "Example")
      .save(s"/user/cpc/zhj/daily/dnntrain-$date-$hour")
    println("train size", train.count())
    train.take(10).foreach(println)
    rawtrain.unpersist()
  }

  def getSample(spark: SparkSession, date: String, hour: String): DataFrame = {
    import spark.implicits._

    val userAppIdx = getUidApp(spark, date)
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
         |  uid, age, sex, ext_string['dtu_id'] as dtu_id
         |
         |from dl_cpc.cpc_union_log where `date` = '$date'
         |  and isshow = 1 and ideaid > 0 and adslot_type = 1
         |  and media_appsid in ("80000001", "80000002")
         |  and uid not like "%.%"
         |  and uid not like "%000000%"
         |
      """.stripMargin
    println(sql)

    spark.sql(sql)
      .join(userAppIdx, Seq("uid"), "leftouter")
      .repartition(1000)
      .select($"label",

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
        mkSparseFeature1($"m1").alias("sparse"), $"label"
      )

      .select(
        $"label",
        $"dense",
        $"sparse".getField("_1").alias("idx0"),
        $"sparse".getField("_2").alias("idx1"),
        $"sparse".getField("_3").alias("idx2"),
        $"sparse".getField("_4").alias("id_arr")
      )

      .rdd.zipWithIndex()
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
    for (i <- 1 until day2) {
      cal.add(Calendar.DATE, -1)
      re = re :+ format.format(cal.getTime)
    }
    re.mkString("','")
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

  /**
    * 更具指定条件使用默认值更换dense特征中的值
    *
    * @param p :位置 0 ~ length-1
    * @param d :默认值
    * @return
    */
  private def getNewDense(p: Int, d: Long) = udf {
    (dense: Seq[Long], f: Boolean) =>
      if (f) (dense.slice(0, p) :+ d) ++ dense.slice(p + 1, 1000) else dense
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





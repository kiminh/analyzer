package com.cpc.spark.ml.dnn

import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.common.Murmur3Hash
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable

object prepareAgeModelSample_v2 {
  Logger.getRootLogger.setLevel(Level.WARN)

  //multi hot 特征默认hash code
  private val default_hash = for (i <- 1 to 37) yield Seq((i - 1, 0, Murmur3Hash.stringHash64("m" + i, 0)))
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("prepare age model sample")
      .enableHiveSupport()
      .getOrCreate()

    val sampleForAge = getSample(spark).persist(StorageLevel.MEMORY_AND_DISK)
/**
    val cal1 = Calendar.getInstance()
    cal1.add(Calendar.DATE, -1)
    val tardate = new SimpleDateFormat("yyyyMMdd").format(cal1.getTime)

    val stmt =
      """
        |select distinct uid from dl_cpc.cpc_union_log where `date` = "%s" and media_appsid in ("80000001", "80000002")
      """.stripMargin.format(tardate)

    val uv = spark.sql(stmt).rdd.map {
      r =>
        val did = r.getAs[String](0)
        did
    }.distinct().toDF("uid")

    val psampleForage = uv.join(sampleForAge, Seq("uid"))

    psampleForage.repartition(100)
      .write
      .mode("overwrite")
      .format("tfrecords")
      .option("recordType", "Example")
      .save(s"/user/cpc/dnn/age/dnnpredict")
*/
    val train = sampleForAge.filter("label is not null")

    val n = train.count().toDouble
    println("训练数据：total = %.4f, age1 = %.4f, age2 = %.4f, age3 = %.4f, age4 = %.4f,".
      format(n, train.where("label=array(1,0,0,0)").count.toDouble / n,
        train.where("label=array(0,1,0,0)").count.toDouble / n,
        train.where("label=array(0,0,1,0)").count.toDouble / n,
        train.where("label=array(0,0,0,1)").count.toDouble / n))

    val Array(traindata, testdata) = train.randomSplit(Array(0.95, 0.05), 1)

    traindata.repartition(100)
      .write
      .mode("overwrite")
      .format("tfrecords")
      .option("recordType", "Example")
      .save(s"/user/cpc/dnn/age/dnntrain")

    testdata.repartition(100)
      .write
      .mode("overwrite")
      .format("tfrecords")
      .option("recordType", "Example")
      .save(s"/user/cpc/dnn/age/dnntest")
    train.unpersist()
  }
  def getSample(spark: SparkSession): DataFrame = {
    import spark.implicits._

    val calCur = Calendar.getInstance()
    val dateCur = new SimpleDateFormat("yyyyMMdd").format(calCur.getTime)
    val day = new SimpleDateFormat("yyyy-MM-dd").format(calCur.getTime)

    val profileData = spark.read.parquet("/user/cpc/qtt-lookalike-sample/v1").
      select($"did".alias("uid"), $"apps._1".alias("pkgs"), $"words", $"terms", $"brand",
      split($"province._1", "province")(1).alias("province"), split($"city._1", "city")(1).alias("city"),
      split($"isp._1", "isp")(1).alias("isp"), split($"os._1", "os")(1).alias("os"),
      split($"screen_w._1", "screen_w")(1).alias("screen_w"), split($"screen_h._1", "screen_h")(1).alias("screen_h"),
      split($"sex._1", "sex")(1).alias("sex"), split($"antispam_score._1", "antispam_score")(1).alias("antispam_score")
    )

    val zfb = spark.read.parquet("/user/cpc/qtt-zfb/10").map {
      r =>
        val did = r.getAs[String]("did")
        val birth = r.getAs[String]("birth")
        var label = Array(0, 0, 0, 0)
        try{
        if (birth != "") {
          if ((dateCur.toInt - birth.toInt) / 10000 < 18) {
            label = Array(1, 0, 0, 0)
          } else if((dateCur.toInt - birth.toInt) / 10000 < 23) {
            label = Array(0, 1, 0, 0)
          } else if((dateCur.toInt - birth.toInt) / 10000 < 40) {
            label = Array(0, 0, 1, 0)
          } else {
            label = Array(0, 0, 0, 1)
          }
        } else {
          label = Array(0, 0, 0, 0)
          }
        }
        catch {
          case ex: Exception => println(birth)
        }
        if (label != Array(0, 0, 0, 0)) {
          (did, label)
        } else {
          null
        }}.filter(_ != null).toDF("uid", "label")

    //获取在app的请求时间分布

    val uidRequest = spark.read.parquet("/user/cpc/features/timeDistributionFeature")
    val uidRequestDense = spark.read.parquet("/user/cpc/features/timeDistributionDenseFeature")

    val uidActiveApp = spark.read.parquet("/user/cpc/userInstalledApp/{%s}".format(getDays(day, 1, 30))).rdd
      .map(x => (x.getAs[String]("uid"),x.getAs[mutable.WrappedArray[String]]("used_pkgs")))
      .reduceByKey(_ ++ _)
      .map(x => (x._1,x._2.distinct)).toDF("uid", "used_pkgs")

    val sample = profileData.join(zfb, Seq("uid"), "leftouter").
      join(uidRequest, Seq("uid"), "leftouter").
      join(uidRequestDense, Seq("uid"), "leftouter").
      join(uidActiveApp, Seq("uid"), "leftouter").repartition(800)

    sample.select($"uid", $"label", hashSeq("m1", "string")($"pkgs").alias("m1"),
      hashSeq("m2", "string")($"request").alias("m2"),
      hashSeq("m3", "string")($"words").alias("m3"),
      hashSeq("m4", "string")($"terms").alias("m4"),
      hashSeq("m5", "string")($"used_pkgs").alias("m5"),
      hash("f1")($"brand").alias("f1"),
      hash("f2")($"province").alias("f2"),
      hash("f3")($"city").alias("f3"),
      hash("f4")($"isp").alias("f4"),
      hash("f5")($"os").alias("f5"),
      hash("f6")($"screen_w").alias("f6"),
      hash("f7")($"screen_h").alias("f7"),
      hash("f8")($"sex").alias("f8"),
      hash("f9")($"antispam_score").alias("f9"),
      hash("f10")($"requestTimeNum00").alias("f10"),
      hash("f11")($"requestTimeNum01").alias("f11"),
      hash("f12")($"requestTimeNum02").alias("f12"),
      hash("f13")($"requestTimeNum03").alias("f13"),
      hash("f14")($"requestTimeNum04").alias("f14"),
      hash("f15")($"requestTimeNum05").alias("f15"),
      hash("f16")($"requestTimeNum06").alias("f16"),
      hash("f17")($"requestTimeNum07").alias("f17"),
      hash("f18")($"requestTimeNum08").alias("f18"),
      hash("f19")($"requestTimeNum09").alias("f19"),
      hash("f20")($"requestTimeNum10").alias("f20"),
      hash("f21")($"requestTimeNum11").alias("f21"),
      hash("f22")($"requestTimeNum12").alias("f22"),
      hash("f23")($"requestTimeNum13").alias("f23"),
      hash("f24")($"requestTimeNum14").alias("f24"),
      hash("f25")($"requestTimeNum15").alias("f25"),
      hash("f26")($"requestTimeNum16").alias("f26"),
      hash("f27")($"requestTimeNum17").alias("f27"),
      hash("f28")($"requestTimeNum18").alias("f28"),
      hash("f29")($"requestTimeNum19").alias("f29"),
      hash("f30")($"requestTimeNum20").alias("f30"),
      hash("f31")($"requestTimeNum21").alias("f31"),
      hash("f32")($"requestTimeNum22").alias("f32"),
      hash("f33")($"requestTimeNum23").alias("f33")
    ).
      select($"uid", $"label",
        array($"f1", $"f2", $"f3", $"f4", $"f5", $"f6", $"f7", $"f8", $"f9", $"f10", $"f11", $"f12", $"f13",
          $"f14", $"f15", $"f16", $"f17", $"f18", $"f19", $"f20", $"f21", $"f22", $"f23", $"f24", $"f25", $"f26", $"f27",
          $"f28", $"f29", $"f30", $"f31", $"f32", $"f33").alias("dense"),
        array($"m1", $"m2", $"m3", $"m4", $"m5").alias("raw_sparse")
      ).select($"dense",
        mkSparseFeature_m($"raw_sparse").alias("sparse"),
        $"label", $"uid"
    ).select(
      $"uid", $"label",
        $"dense",
        $"sparse".getField("_1").alias("idx0"),
        $"sparse".getField("_2").alias("idx1"),
        $"sparse".getField("_3").alias("idx2"),
        $"sparse".getField("_4").alias("id_arr")
      ).rdd.zipWithUniqueId().map { x =>
        (x._2, x._1.getAs[String]("uid"), x._1.getAs[Seq[Int]]("label"), x._1.getAs[Seq[Long]]("dense"),
          x._1.getAs[Seq[Int]]("idx0"), x._1.getAs[Seq[Int]]("idx1"),
          x._1.getAs[Seq[Int]]("idx2"), x._1.getAs[Seq[Long]]("id_arr"))
      }.toDF("sample_idx", "uid", "label", "dense", "idx0", "idx1", "idx2", "id_arr")

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
    re.mkString(",")
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

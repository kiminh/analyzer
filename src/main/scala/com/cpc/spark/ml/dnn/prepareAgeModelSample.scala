package com.cpc.spark.ml.dnn

import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.common.Murmur3Hash
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel

object prepareAgeModelSample {
  Logger.getRootLogger.setLevel(Level.WARN)

  //multi hot 特征默认hash code
  private val default_hash = for (i <- 1 to 37) yield Seq((i - 1, 0, Murmur3Hash.stringHash64("m" + i, 0)))
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("prepare age model sample")
      .enableHiveSupport()
      .getOrCreate()

    val train = getSample(spark).persist(StorageLevel.MEMORY_AND_DISK)
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

    testdata.sample(withReplacement = false, 0.1).repartition(100)
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

    val data = spark.read.parquet("/user/cpc/qtt-lookalike-sample/v1")

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

        if (label != Array(0, 0, 0, 0)) {
          (did, label)
        } else {
          null
        }}.filter(_ != null).toDF("uid", "label")

    //获取前7天的广告数据，请求，展现，点击
    val cal1 = Calendar.getInstance()
    val f = new SimpleDateFormat("yyyy-MM-dd")
    var dates = Seq[String]()
    for (i <- 0 until 7) {
      val date = f.format(cal1.getTime)
      cal1.add(Calendar.DATE, -1)
      dates = dates :+ date
    }

    val uidRequest = spark.read.parquet(s"/home/cpc/zhj/statdata/addata/{${dates.mkString(",")}}").rdd.map {
      x =>
        ((x.getAs[String]("date"), x.getAs[String]("hour"), x.getAs[String]("uid")), x.getAs[Long]("request_num"))
    }.reduceByKey(_ + _, 500).map(x => ((x._1._2, x._1._3), (x._2, 1))).reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2)).map { //(hour,uid),(request_num,date_num)
      x =>
        val avgRequestNum = x._2._1 / x._2._2
        if(avgRequestNum<5){
          (x._1._2, Seq(x._1._1 + 1))
        } else if(avgRequestNum<15){
          (x._1._2, Seq("requestTimeNum" + x._1._1 + 2))
        } else if(avgRequestNum<25){
          (x._1._2, Seq("requestTimeNum" + x._1._1 + 3))
        } else if(avgRequestNum<35){
          (x._1._2, Seq("requestTimeNum" + x._1._1 + 4))
        } else if(avgRequestNum<45){
          (x._1._2, Seq("requestTimeNum" + x._1._1 + 5))
        } else if(avgRequestNum<55){
          (x._1._2, Seq("requestTimeNum" + x._1._1 + 6))
        } else {(x._1._2, Seq("requestTimeNum" + x._1._1 + 7))}
    }.reduceByKey(_ ++ _).toDF("uid", "request")

    val sample = zfb.join(profileData, Seq("uid")).join(uidRequest, Seq("uid"), "leftouter").repartition(800)

    sample.select($"uid", $"label", hashSeq("m1", "string")($"pkgs").alias("m1"),
      hashSeq("m2", "string")($"request").alias("m2"),
      hashSeq("m3", "string")($"words").alias("m3"),
      hashSeq("m4", "string")($"terms").alias("m4"),
      hash("f1")($"brand").alias("f1"),
      hash("f2")($"province").alias("f2"),
      hash("f3")($"city").alias("f3"),
      hash("f4")($"isp").alias("f4"),
      hash("f5")($"os").alias("f5"),
      hash("f6")($"screen_w").alias("f6"),
      hash("f7")($"screen_h").alias("f7"),
      hash("f8")($"sex").alias("f8"),
      hash("f9")($"antispam_score").alias("f9")).
      select($"label",
        array($"f1", $"f2", $"f3", $"f4", $"f5", $"f6", $"f7", $"f8", $"f9").alias("dense"),
        array($"m1", $"m2", $"m3", $"m4").alias("raw_sparse")
      ).select($"dense",
        mkSparseFeature_m($"raw_sparse").alias("sparse"),
        $"label"
      ).select(
        $"label",
        $"dense",
        $"sparse".getField("_1").alias("idx0"),
        $"sparse".getField("_2").alias("idx1"),
        $"sparse".getField("_3").alias("idx2"),
        $"sparse".getField("_4").alias("id_arr")
      ).rdd.zipWithUniqueId().map { x =>
        (x._2, x._1.getAs[Seq[Int]]("label"), x._1.getAs[Seq[Long]]("dense"),
          x._1.getAs[Seq[Int]]("idx0"), x._1.getAs[Seq[Int]]("idx1"),
          x._1.getAs[Seq[Int]]("idx2"), x._1.getAs[Seq[Long]]("id_arr"))
      }.toDF("sample_idx", "label", "dense", "idx0", "idx1", "idx2", "id_arr")

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

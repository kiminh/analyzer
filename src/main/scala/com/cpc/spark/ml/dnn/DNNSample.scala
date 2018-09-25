package com.cpc.spark.ml.dnn

import java.io.FileOutputStream
import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import mlmodel.mlmodel
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.collection.mutable
import scala.collection.mutable.WrappedArray
import scala.util.Random
import com.cpc.spark.common.Murmur3Hash

object DNNSample {

  private var trainLog = Seq[String]()

  def main(args: Array[String]): Unit = {
    Logger.getRootLogger.setLevel(Level.WARN)

    val spark = SparkSession.builder()
      .appName("dnn sample")
      .enableHiveSupport()
      .getOrCreate()
    import spark.implicits._

    val date = new SimpleDateFormat("yyyy-MM-dd").format(new Date().getTime)

    //按分区取数据
    val ctrPathSep = getPathSeq(args(0).toInt)

    val userAppIdx = getUidApp(spark, ctrPathSep)

    val ulog = getData(spark,"ctrdata_v1",ctrPathSep)
      .filter {x =>
        val ideaid = x.getAs[Int]("ideaid")
        val slottype = x.getAs[Int]("adslot_type")
        val mediaid = x.getAs[String]("media_appsid").toInt
        ideaid > 0 && slottype == 1 && Seq(80000001, 80000002).contains(mediaid)
      }
      //.randomSplit(Array(0.1, 0.9), new Date().getTime)(0)
      .join(userAppIdx, Seq("uid"))
      .rdd
      .map{row =>
        val ret = getVectorParser(row)
        val raw = ret._1
        val apps = ret._2
        var label = Seq(0, 1)
        if (row.getAs[Int]("label") > 0) {
          label = Seq(1, 0)
        }

        var hashed = Seq[Long]()
        for (i <- raw.indices) {
          hashed = hashed :+ Murmur3Hash.stringHash("%s:%d".format(fnames(i), raw(i)))
        }

        val sparse = Sparse()
        sparse.idx0 :+= 0L

        var appHashed = Seq[Long]()
        for (i <- apps.indices) {
          appHashed = appHashed :+ Murmur3Hash.stringHash("app:%s".format(apps(i)))
        }

        sparse.idx1 :+= 0L
        sparse.idx2 ++= appHashed

        (label, hashed, sparse)
      }
      .zipWithUniqueId()
      .map(x => (x._2, x._1._1, x._1._2, x._1._3))
      .toDF("sample_idx", "label", "dense", "sparse")
      .repartition(1000)

    val clickiNum = ulog.filter{
      x =>
        val label = x.getAs[Seq[Int]]("label")
        label(0) == 1
    }.count()
    println(ulog.count(), clickiNum)

    val Array(train, test) = ulog.randomSplit(Array(0.97, 0.03))
    val resampled = train.filter{
      x =>
        val label = x.getAs[Seq[Int]]("label")
        label(0) == 1 || Random.nextInt(1000) < 100
    }

    resampled.select("sample_idx", "label", "id", "app")
      .coalesce(100)
      .write
      .mode("overwrite")
      .format("tfrecords")
      .option("recordType", "Example")
      .save("/user/cpc/dw/dnntrain-" + date)
    println("train size", resampled.count())

    test.select("sample_idx", "label", "id", "app")
      .coalesce(100)
      .write
      .mode("overwrite")
      .format("tfrecords")
      .option("recordType", "Example")
      .save("/user/cpc/dw/dnntest-" + date)
    test.take(10).foreach(println)
    println("test size", test.count())
  }

  def getPathSeq(days: Int): mutable.Map[String,Seq[String]] ={
    var date = ""
    var hour = ""
    val cal = Calendar.getInstance()
    cal.add(Calendar.HOUR, -(days * 24 + 2))
    val pathSep = mutable.Map[String,Seq[String]]()

    for (n <- 1 to days * 24) {
      date = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime)
      hour = new SimpleDateFormat("HH").format(cal.getTime)
      pathSep.update(date,(pathSep.getOrElse(date,Seq[String]()) :+ hour))
      cal.add(Calendar.HOUR, 1)
    }

    pathSep
  }

  def getUidApp(spark: SparkSession, pathSep: mutable.Map[String,Seq[String]]): DataFrame ={
    val inpath = "/user/cpc/userInstalledApp/{%s}".format(pathSep.keys.mkString(","))
    println(inpath)

    import spark.implicits._
    spark.read.parquet(inpath).rdd
      .map(x => (x.getAs[String]("uid"),x.getAs[WrappedArray[String]]("pkgs")))
      .reduceByKey(_ ++ _)
      .map(x => (x._1,x._2.distinct))
      .toDF("uid","pkgs")
  }


  def getData(spark: SparkSession, dataVersion: String, pathSep: mutable.Map[String,Seq[String]]): DataFrame = {

    var path = Seq[String]()
    pathSep.map{
      x =>
        path = path :+ "/user/cpc/lrmodel/%s/%s/{%s}".format(dataVersion, x._1, x._2.mkString(","))
    }

    path.foreach{
      x =>
        println(x)
    }

    spark.read.parquet(path:_*)
  }

  val fnames = Seq(
    "hour", "sex", "age", "os", "net", "pl", "adtype", "city", "mediaid",
    "adslotid", "adclass", "planid", "unitid", "ideaid"
  )

  def getVectorParser(x: Row): (Seq[Int], Seq[String]) = {
    val cal = Calendar.getInstance()
    cal.setTimeInMillis(x.getAs[Int]("timestamp") * 1000L)
    val week = cal.get(Calendar.DAY_OF_WEEK)   //1 to 7
    val hour = cal.get(Calendar.HOUR_OF_DAY)
    var raw = Seq[Int]()

    raw = raw :+ hour

    val sex = x.getAs[Int]("sex")
    raw = raw :+ sex

    //age
    val age = x.getAs[Int]("age")
    raw = raw :+ age

    //os 96 - 97 (2)
    val os = x.getAs[Int]("os")
    raw = raw :+ os

    //net
    val net = x.getAs[Int]("network")
    raw = raw :+ net

    val pl = x.getAs[Int]("phone_level")
    raw = raw :+ pl

    val at = x.getAs[Int]("adtype")
    raw = raw :+ at

    val cityid = x.getAs[Int]("city")
    raw = raw :+ cityid

    val mediaid = x.getAs[String]("media_appsid").toInt
    raw = raw :+ mediaid

    val slotid = x.getAs[String]("adslotid").toInt
    raw = raw :+ slotid

    val ac = x.getAs[Int]("adclass")
    raw = raw :+ ac

    val planid = x.getAs[Int]("planid")
    raw = raw :+ planid

    val unitid = x.getAs[Int]("unitid")
    raw = raw :+ unitid

    val ideaid = x.getAs[Int]("ideaid")
    raw = raw :+ ideaid

    val apps = x.getAs[Seq[String]]("apps")

    if (apps.length > 0) {
      (raw, apps.slice(0, 500))
    } else {
      (raw, Seq(""))
    }
  }

  case class Sparse(
                   var idx0: Seq[Long] = Seq(),
                   var idx1: Seq[Long] = Seq(),
                   var idx2: Seq[Long] = Seq()
                   )
}





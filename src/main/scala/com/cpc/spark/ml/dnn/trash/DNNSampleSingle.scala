package com.cpc.spark.ml.dnn.trash

import java.io.FileOutputStream
import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import mlmodel.mlmodel
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.collection.mutable
import scala.collection.mutable.WrappedArray
import scala.util.Random
@deprecated
object DNNSampleSingle {

  private var trainLog = Seq[String]()

  def main(args: Array[String]): Unit = {
    Logger.getRootLogger.setLevel(Level.WARN)

    val spark = SparkSession.builder()
      .appName("dnn sample single")
      .enableHiveSupport()
      .getOrCreate()
    import spark.implicits._

    val date = new SimpleDateFormat("yyyy-MM-dd").format(new Date().getTime)

    //按分区取数据
    val ctrPathSep = getPathSeq(args(0).toInt)
    initFeatureDict(spark, ctrPathSep)
    val userAppIdx = getUidApp(spark, ctrPathSep)
    println("max id", maxIndex)

    val BcDict = spark.sparkContext.broadcast(dict)
    val ulog = getData(spark,"ctrdata_v1",ctrPathSep)
      .filter {x =>
        val ideaid = x.getAs[Int]("ideaid")
        val slottype = x.getAs[Int]("adslot_type")
        val mediaid = x.getAs[String]("media_appsid").toInt
        ideaid > 0 && slottype == 1 && Seq(80000001, 80000002).contains(mediaid)
      }
      .join(userAppIdx, Seq("uid"))
      .rdd
      .map{row =>
        dict = BcDict.value
        val ret = getVectorParser2(row)
        val raw = ret._1
        val apps = ret._2
        val ids = ret._3
        val appids = ret._4
        var label = Seq(0, 1)
        if (row.getAs[Int]("label") > 0) {
          label = Seq(1, 0)
        }
        (label, ids, appids, raw, apps)
      }
      .zipWithUniqueId()
      .map(x => (x._2, x._1._1, x._1._2, x._1._3, x._1._4, x._1._5))
      .toDF("sample_idx", "label", "id", "app", "raw", "pkg")
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
    savePbPack("dnn-rawid", "/home/cpc/dw/bin/dict.pb", dict.toMap)

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
    val uidApp = spark.read.parquet(inpath).rdd
      .map(x => (x.getAs[String]("uid"),x.getAs[WrappedArray[String]]("pkgs")))
      .reduceByKey(_ ++ _)
      .map(x => (x._1,x._2.distinct))
      .toDF("uid","pkgs").rdd.cache()

    val ids = getTopApp(uidApp, 5000)
    dictStr.update("appid",ids)

    val userAppIdx = getUserAppIdx(spark, uidApp, ids)

    userAppIdx
  }

  //安装列表中top k的App
  def getTopApp(uidApp : RDD[Row], k : Int): Map[String,Int] ={
    val ids = mutable.Map[String,Int]()
    uidApp
      .flatMap(x => x.getAs[WrappedArray[String]]("pkgs").map((_,1)))
      .reduceByKey(_ + _)
      .sortBy(_._2, false)
      .toLocalIterator
      .take(k)
      .foreach{
        id =>
          maxIndex += 1
          ids.update(id._1,maxIndex)
      }
    ids.toMap
  }

  //用户安装列表对应的App idx
  def getUserAppIdx(spark: SparkSession, uidApp : RDD[Row], ids : Map[String,Int]): DataFrame ={
    import spark.implicits._
    uidApp.map{
      x =>
        val k = x.getAs[String]("uid")
        val apps = x.getAs[Seq[String]]("pkgs")
        val v = apps.map(p => (ids.getOrElse(p,0))).filter(_ > 0)
        (k, apps, v)
    }.toDF("uid", "apps", "appIdx")
  }


  var dict = mutable.Map[String, Map[Int, Int]]()
  val dictNames = Seq(
    "mediaid",
    "planid",
    "unitid",
    "ideaid",
    "slotid",
    "adclass",
    "cityid"
  )
  var dictStr = mutable.Map[String, Map[String, Int]]()

  var maxIndex = 200

  def initFeatureDict(spark: SparkSession, pathSep: mutable.Map[String,Seq[String]]): Unit = {
    trainLog :+= "\n------dict size------"
    for (name <- dictNames) {
      val pathTpl = "/user/cpc/lrmodel/feature_ids_v1/%s/{%s}"
      val ids = mutable.Map[Int, Int]()
      println(pathTpl.format(name, pathSep.keys.mkString(",")))
      spark.read
        .parquet(pathTpl.format(name, pathSep.keys.mkString(",")))
        .rdd
        .map(x => x.getInt(0))
        .distinct()
        .sortBy(x => x)
        .toLocalIterator
        .foreach {
          id =>
            maxIndex += 1
            ids.update(id, maxIndex)
        }
      dict.update(name, ids.toMap)
      println("dict", name, ids.size)
    }
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


  def getVectorParser2(x: Row): (Seq[Int], Seq[String], Seq[Int], Seq[Int]) = {
    val cal = Calendar.getInstance()
    cal.setTimeInMillis(x.getAs[Int]("timestamp") * 1000L)
    val week = cal.get(Calendar.DAY_OF_WEEK)   //1 to 7
    val hour = cal.get(Calendar.HOUR_OF_DAY)
    var raw = Seq[Int]()
    var idx = Seq[Int]()
    var i = 1


    raw = raw :+ hour
    idx = idx :+ hour + i
    i += 25

    //sex
    val sex = x.getAs[Int]("sex")
    raw = raw :+ sex
    idx = idx :+ sex + i
    i += 5

    //age
    val age = x.getAs[Int]("age")
    raw = raw :+ age
    idx = idx :+ age + i
    i += 10

    //os 96 - 97 (2)
    val os = x.getAs[Int]("os")
    raw = raw :+ os
    idx = idx :+ os + i
    i += 10

    //net
    val net = x.getAs[Int]("network")
    raw = raw :+ net
    idx = idx :+ net + i
    i += 10

    val pl = x.getAs[Int]("phone_level")
    raw = raw :+ pl
    idx = idx :+ pl + i
    i += 10

    val at = x.getAs[Int]("adtype")
    raw = raw :+ at
    idx = idx :+ at + i
    i += 50

    //idx = idx :+ x.getAs[Int]("adslot_type") + i
    //i += 10

    val cityid = x.getAs[Int]("city")
    raw = raw :+ cityid
    idx = idx :+ dict("cityid").getOrElse(cityid, 0)

    val mediaid = x.getAs[String]("media_appsid").toInt
    raw = raw :+ mediaid
    idx = idx :+ dict("mediaid").getOrElse(mediaid, 0)

    val slotid = x.getAs[String]("adslotid").toInt
    raw = raw :+ slotid
    idx = idx :+ dict("slotid").getOrElse(slotid, 0)

    val ac = x.getAs[Int]("adclass")
    raw = raw :+ ac
    idx = idx :+ dict("adclass").getOrElse(ac, 0)

    val planid = x.getAs[Int]("planid")
    raw = raw :+ planid
    idx = idx :+ dict("planid").getOrElse(planid, 0)

    val unitid = x.getAs[Int]("unitid")
    raw = raw :+ unitid
    idx = idx :+ dict("unitid").getOrElse(unitid, 0)

    val ideaid = x.getAs[Int]("ideaid")
    raw = raw :+ ideaid
    idx = idx :+ dict("ideaid").getOrElse(ideaid, 0)

    val apps = x.getAs[Seq[String]]("apps")
    val appids = x.getAs[Seq[Int]]("appIdx")

    if (appids.length > 0) {
      (raw, apps, idx, appids.slice(0, 500))
    } else {
      (raw, apps, idx, Seq(0))
    }
  }

  def savePbPack(parser: String, path: String, dict: Map[String, Map[Int, Int]]): Unit = {
    val dictpb = mlmodel.Dict(
      planid = dict("planid"),
      unitid = dict("unitid"),
      ideaid = dict("ideaid"),
      slotid = dict("slotid"),
      adclass = dict("adclass"),
      cityid = dict("cityid"),
      mediaid = dict("mediaid"),
      appid = dictStr("appid")
    )
    val pack = mlmodel.Pack(
      dict = Option(dictpb)
    )
    pack.writeTo(new FileOutputStream(path))
  }
}





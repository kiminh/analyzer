package com.cpc.spark.ml.dnn

import java.io.FileOutputStream
import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import com.cpc.spark.ml.common.{Utils => MUtils}
import com.cpc.spark.ml.train.LRIRModel
import mlmodel.mlmodel
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.collection.mutable
import scala.collection.mutable.WrappedArray
import scala.util.Random

object DNNSampleSingle {

  private val days = 7
  private val daysCvr = 20
  private var trainLog = Seq[String]()
  private val model = new LRIRModel


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
    val cvrPathSep = getPathSeq(args(1).toInt)

    initFeatureDict(spark, ctrPathSep)
    val userAppIdx = getUidApp(spark, ctrPathSep)

    val BcDict = spark.sparkContext.broadcast(dict)
    val ulog = getData(spark,"ctrdata_v1",ctrPathSep).rdd
      .filter {x =>
        val ideaid = x.getAs[Int]("ideaid")
        val slottype = x.getAs[Int]("adslot_type")
        val mediaid = x.getAs[String]("media_appsid").toInt
        ideaid > 0 && slottype == 1 && Seq(80000001, 80000002).contains(mediaid)
      }
      .randomSplit(Array(0.02, 0.98), new Date().getTime)(0)
      .map{row =>
        dict = BcDict.value
        val vec = getVectorParser2(row)
        var label = Seq(0, 1)
        if (row.getAs[Int]("label") > 0) {
          label = Seq(1, 0)
        }
        (label, vec)
      }
      .zipWithUniqueId()
      .map(x => (x._2, x._1._1, x._1._2))
      .toDF("sample_idx", "label", "id")
      .repartition(1000)

    val Array(train, test) = ulog.randomSplit(Array(0.95, 0.05))

    train.filter{
        x =>
        val label = x.getAs[Seq[Int]]("label")
        label(0) == 1 || Random.nextInt(1000) < 120
      }
      .write
      .mode("overwrite")
      .format("tfrecords")
      .option("recordType", "Example")
      .save("/user/cpc/dw/dnntrain-" + date)

    test.write
      .mode("overwrite")
      .format("tfrecords")
      .option("recordType", "Example")
      .save("/user/cpc/dw/dnntest-" + date)

    savePbPack("dnnp1", "/home/cpc/dw/bin/dict.pb", dict.toMap)
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

    val ids = getTopApp(uidApp, 1000)
    dictStr.update("appid",ids)

    val userAppIdx = getUserAppIdx(spark, uidApp, ids)

    userAppIdx
  }

  //安装列表中top k的App
  def getTopApp(uidApp : RDD[Row], k : Int): Map[String,Int] ={
    val ids = mutable.Map[String,Int]()
    maxIndex = 0
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
        val v = x.getAs[WrappedArray[String]]("pkgs").map(p => (ids.getOrElse(p,0))).filter(_ > 0)
        (k,v)
    }.toDF("uid","appIdx")
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

  var maxIndex = 0

  def initFeatureDict(spark: SparkSession, pathSep: mutable.Map[String,Seq[String]]): Unit = {
    trainLog :+= "\n------dict size------"
    maxIndex = 0
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


  def getVectorParser2(x: Row): Seq[Int] = {
    val cal = Calendar.getInstance()
    cal.setTimeInMillis(x.getAs[Int]("timestamp") * 1000L)
    val week = cal.get(Calendar.DAY_OF_WEEK)   //1 to 7
    val hour = cal.get(Calendar.HOUR_OF_DAY)
    var els = Seq[Int]()
    var i = 0


    els = els :+ hour + i
    i += 24

    //sex
    els = els :+ x.getAs[Int]("sex") + i
    i += 9

    //age
    els = els :+ x.getAs[Int]("age") + i
    i += 100

    //os 96 - 97 (2)
    els = els :+ x.getAs[Int]("os") + i
    i += 10


    //net
    els = els :+ x.getAs[Int]("network") + i
    i += 10

    els = els :+ dict("cityid").getOrElse(x.getAs[Int]("city"), 0) + i
    i += dict("cityid").size + 1

    //media id
    els = els :+ dict("mediaid").getOrElse(x.getAs[String]("media_appsid").toInt, 0) + i
    i += dict("mediaid").size + 1

    //ad slot id
    els = els :+ dict("slotid").getOrElse(x.getAs[String]("adslotid").toInt, 0) + i
    i += dict("slotid").size + 1

    //0 to 4
    els = els :+ x.getAs[Int]("phone_level") + i
    i += 10

    //ad class
    val adcls = dict("adclass").getOrElse(x.getAs[Int]("adclass"), 0)
    els = els :+ adcls + i
    i += dict("adclass").size + 1

    //adtype
    els = els :+ x.getAs[Int]("adtype") + i
    i += 10

    //adslot_type
    els = els :+ x.getAs[Int]("adslot_type") + i
    i += 10

    //planid
    els = els :+ dict("planid").getOrElse(x.getAs[Int]("planid"), 0) + i
    i += dict("planid").size + 1

    //unitid
    els = els :+ dict("unitid").getOrElse(x.getAs[Int]("unitid"), 0) + i
    i += dict("unitid").size + 1

    //ideaid
    els = els :+ dict("ideaid").getOrElse(x.getAs[Int]("ideaid"), 0) + i
    i += dict("ideaid").size + 1

    els
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





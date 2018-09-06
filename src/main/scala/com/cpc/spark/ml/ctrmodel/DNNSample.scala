package com.cpc.spark.ml.ctrmodel

import java.io.FileInputStream
import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import com.cpc.spark.common.Utils
import com.cpc.spark.ml.common.{Utils => MUtils}
import com.cpc.spark.ml.train.LRIRModel
import com.typesafe.config.ConfigFactory
import lrmodel.lrmodel.Pack
import mlserver.mlserver._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.classification.LogisticRegressionModel
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.collection.mutable
import scala.collection.mutable.WrappedArray
import scala.util.Random

/**
  * Created by zhaolei on 22/12/2017.
  */
object DNNSample {

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

    //按分区取数据
    val ctrPathSep = getPathSeq(args(0).toInt)
    val cvrPathSep = getPathSeq(args(1).toInt)

    //initFeatureDict(spark, ctrPathSep)
    //val userAppIdx = getUidApp(spark, ctrPathSep).cache()

    val ulog = getData(spark,"ctrdata_v1",ctrPathSep).rdd
      .filter(_.getAs[Int]("ideaid") > 0)
      .randomSplit(Array(0.1, 0.9), new Date().getTime)(0)
      .map{row =>
        val vec = getVectorParser1(row)
        var label = Seq(0, 1)
        if (row.getAs[Int]("label") > 0) {
          label = Seq(1, 0)
        }
        (label, vec)
      }
      .toDF("label", "id")
    println(ulog.count())

    ulog.write
      .mode("overwrite")
      .format("tfrecords")
      .option("recordType", "Example")
      .save("/user/cpc/dw/dnnsample")
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


  def getVectorParser1(x: Row): Seq[Long] = {
    val cal = Calendar.getInstance()
    cal.setTimeInMillis(x.getAs[Int]("timestamp") * 1000L)
    val week = cal.get(Calendar.DAY_OF_WEEK)   //1 to 7
    val hour = cal.get(Calendar.HOUR_OF_DAY)
    var els = Seq[Int]()

    els :+= hour
    els :+= x.getAs[Int]("sex")
    els :+= x.getAs[Int]("age")
    els :+= x.getAs[Int]("os")
    els :+= x.getAs[Int]("network")
    els :+= x.getAs[Int]("city")
    els :+= x.getAs[String]("media_appsid").toInt
    els :+= x.getAs[String]("adslotid").toInt
    els :+= x.getAs[Int]("phone_level")
    els :+= x.getAs[Int]("adclass")
    els :+= x.getAs[Int]("adtype")
    els :+= x.getAs[Int]("adslot_type")
    els :+= x.getAs[Int]("planid")
    els :+= x.getAs[Int]("unitid")
    els :+= x.getAs[Int]("ideaid")

    var hashed = Seq[Long]()
    for (i <- feature_names.indices) {
      hashed :+= (feature_names(i) + els(i)).hashCode.toLong
    }
    hashed
  }

  val feature_names = Seq(
    "sex", "age", "os", "network", "cityid",
    "mediaid", "slotid", "phone_level", "adclass",
    "adtype", "adslot_type", "planid", "unitid", "ideaid")

  def convertSample(): Unit = {

  }
}





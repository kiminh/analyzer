package com.cpc.spark.ml.xgboost

import java.io.{FileInputStream, FileOutputStream}
import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import com.cpc.spark.ml.common.{Utils => MUtils}
import com.redis.RedisClient
import com.redis.serialization.Parse.Implicits._
import com.typesafe.config.ConfigFactory
import io.grpc.ManagedChannelBuilder
import mlmodel.mlmodel._
import mlserver.mlserver._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.linalg.{Vector => MVec, Vectors => MVecs}
import org.apache.spark.mllib.classification.{LogisticRegressionModel, LogisticRegressionWithLBFGS}
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.optimization.L1Updater
import org.apache.spark.mllib.regression.{IsotonicRegression, IsotonicRegressionModel, LabeledPoint}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import userprofile.Userprofile.UserProfile

import scala.collection.mutable
import scala.collection.mutable.WrappedArray
import scala.util.Random

/**
  * Created by zhaolei on 22/12/2017.
  */
object XgboostLrTrain {

  //  private val minBinSize = 10000d
  private var binNum = 1000d
  private var binsLog = Seq[String]()
  private var irBinNum = 0
  private var irError = 0d
  private var isUpdateModel = false
  private var xgbTestResults: RDD[(Double, Double)] = null
  private var trainLog = Seq[String]()
  private var irmodel: IsotonicRegressionModel = _
  private var lrmodel: LogisticRegressionModel = _
  private var auPRC = 0d
  private var auROC = 0d

  def modelClear(): Unit = {
    binsLog = Seq[String]()
    irBinNum = 0
    irError = 0
    xgbTestResults = null
    irmodel = null
    isUpdateModel = false
  }

  def main(args: Array[String]): Unit = {
    trainLog :+= args.mkString(" ")
    println(args.mkString(" "))
    val type1 = args(0)
    val upload = args(1).toInt   //upload to mlserver
    println(s"type=$type1")
    println(s"upload=$upload")


    Logger.getRootLogger.setLevel(Level.WARN)
    val spark = SparkSession.builder()
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.kryoserializer.buffer.max", "2047MB")
      .appName("xgboost ->lr train type:%s".format(type1))
      .enableHiveSupport()
      .getOrCreate()

    val qttListTestLeaf: RDD[LabeledPoint] = spark.sparkContext
      .textFile(s"/user/cpc/qtt-portrait-ctr-model/sample/djq_ctr_sample_test_leaf_${type1}", 50)
      .map { x => {
        val array = x.split("\t")
        val label = array(0).toDouble
        val vector1 = array(1).split("\\s+").map(x => {
          val array = x.split(":")
          (array(0).toInt, array(1).toDouble)
        })
        val vec = Vectors.sparse(1000000, vector1)
        LabeledPoint(label, vec)
      }
      }

    val qttListTrainLeaf: RDD[LabeledPoint] = spark.sparkContext
      .textFile(s"/user/cpc/qtt-portrait-ctr-model/sample/djq_ctr_sample_train_leaf_${type1}", 50)
      .map { x => {
        val array = x.split("\t")
        val label = array(0).toDouble
        val vector1 = array(1).split("\\s+").map(x => {
          val array = x.split(":")
          (array(0).toInt, array(1).toDouble)
        })
        val vec = Vectors.sparse(1000000, vector1)
        LabeledPoint(label, vec)
      }
      }

    //val Array(train, test) = qttListLeaf.randomSplit(Array(0.8, 0.2))
    val train = qttListTrainLeaf
    val test = qttListTestLeaf

    val lbfgs = new LogisticRegressionWithLBFGS().setNumClasses(2)
    lbfgs.optimizer.setUpdater(new L1Updater())
    lbfgs.optimizer.setNumIterations(200)
    lbfgs.optimizer.setConvergenceTol(1e-8)

    import spark.implicits._
    val BcDict = spark.sparkContext.broadcast(dict)
//    val rate = 0.1
//    val sampleTrain = train.filter((_.label > 0 || Random.nextInt(1000) < rate))
    val sampleTrain = train
    val sampleTest = test


    val lr = lbfgs.run(sampleTrain)
    val filetime = new SimpleDateFormat("yyyy-MM-dd-HH-mm").format(new Date().getTime)
    lr.save(spark.sparkContext, "/user/cpc/xgboost_lr_model/" + filetime)
    lr.clearThreshold()
    xgbTestResults = sampleTest.map { r => (lr.predict(r.features), r.label) }
    printXGBTestLog(xgbTestResults)
    val metrics = new BinaryClassificationMetrics(xgbTestResults)
    auPRC = metrics.areaUnderPR
    auROC = metrics.areaUnderROC
    trainLog :+= "auPRC=%.6f auROC=%.6f".format(auPRC, auROC)
    println(s"test: auPrc=$auPRC, auc=$auROC")
    lrmodel = lr

    // train metrics
    val xgbTrainResults = sampleTrain.map { r => (lr.predict(r.features), r.label) }
    val metricsTrain = new BinaryClassificationMetrics(xgbTrainResults)
    println(s"train: auPrc=${metricsTrain.areaUnderPR()}, auc=${metricsTrain.areaUnderROC()}")


    runIr(spark, binNum.toInt, 0.95)

    val BcWeights = spark.sparkContext.broadcast(lrmodel.weights)

    var type2 = "list"
    if (type1 == 1) {
      type2 = "list"
    } else if (type1 == 2) {
      type2 = "content"
    } else if (type1 == 3) {
      type2 = "interact"
    } else {
      type2 = "unknown"
    }

    val fname = s"ctr-portrait9-xglr-qtt-$type2.mlm"
    val filename = s"/home/cpc/djq/xgboost_lr/$fname"
    saveLrPbPack(filename , "xglr", type1.toInt)
    println(filetime, filename)

    if (upload > 0) {
      val conf = ConfigFactory.load()
      println(MUtils.updateMlcppOnlineData(filename, s"/home/work/mlcpp/data/$fname", conf))
    }
  }

  def getPathSeq(days: Int): mutable.Map[String,Seq[String]] ={
    var date = ""
    var hour = ""
    val cal = Calendar.getInstance()
    cal.add(Calendar.HOUR, -(days * 24 + 1))
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
      .map(x => (x.getAs[String]("uid"),x.getAs[mutable.WrappedArray[String]]("pkgs")))
      .reduceByKey(_ ++ _)
      .map(x => (x._1,x._2.distinct))
      .toDF("uid","pkgs").rdd

    val ids = getTopApp(uidApp, 500)
    dictStr.update("appid",ids)
    getUserAppIdx(spark, uidApp, ids)
  }

  //安装列表中top k的App
  def getTopApp(uidApp : RDD[Row], k : Int): Map[String,Int] ={
    var idx = 0
    val ids = mutable.Map[String,Int]()
    uidApp
      .flatMap(x => x.getAs[mutable.WrappedArray[String]]("pkgs").map((_,1)))
      .reduceByKey(_ + _)
      .sortBy(_._2,false)
      .toLocalIterator
      .take(k)
      .foreach{
        id =>
          ids.update(id._1,idx)
          idx += 1
      }
    ids.toMap
  }

  def isMorning(): Boolean = {
    new SimpleDateFormat("HH").format(new Date().getTime) < "08"
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

  def getLimitedData(spark: SparkSession, limitedNum: Double, ulog: DataFrame): DataFrame = {
    var rate = 1d
    val num = ulog.count().toDouble

    if (num > limitedNum) {
      rate = limitedNum / num
    }

    ulog.randomSplit(Array(rate, 1 - rate), new Date().getTime)(0)
  }

  private var dict = mutable.Map[String, Map[Int, Int]]()
  val dictNames = Seq(
    "mediaid",
    "planid",
    "unitid",
    "ideaid",
    "slotid",
    "adclass",
    "cityid"
  )
  private var dictStr = mutable.Map[String, Map[String, Int]]()

  def initFeatureDict(spark: SparkSession, pathSep: mutable.Map[String,Seq[String]]): Unit = {

    trainLog :+= "\n------dict size------"
    for (name <- dictNames) {
      val pathTpl = "/user/cpc/lrmodel/feature_ids_v1/%s/{%s}"
      var n = 0
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
            n += 1
            ids.update(id, n)
        }
      dict.update(name, ids.toMap)
      println("dict", name, ids.size)
      trainLog :+= "%s=%d".format(name, ids.size)
    }
  }


  def getData(spark: SparkSession, dataVersion: String, pathSep: mutable.Map[String,Seq[String]]): DataFrame = {
    trainLog :+= "\n-------get ulog data------"

    var path = Seq[String]()
    pathSep.foreach{
      x =>
        path = path :+ "/user/cpc/lrmodel/%s/%s/{%s}".format(dataVersion, x._1, x._2.mkString(","))
    }

    path.foreach{
      x =>
        trainLog :+= x
        println(x)
    }

    spark.read.parquet(path:_*)
  }

  def getCtrVectorParser1(x: Row): Vector = {
    val cal = Calendar.getInstance()
    cal.setTimeInMillis(x.getAs[Int]("timestamp") * 1000L)
    val week = cal.get(Calendar.DAY_OF_WEEK)   //1 to 7
    val hour = cal.get(Calendar.HOUR_OF_DAY)
    var els = Seq[(Int, Double)]()
    var i = 0

    //1
    els = els :+ (week + i - 1, 1d)
    i += 7

    //2
    els = els :+ (hour + i, 1d)
    i += 24

    //3
    els = els :+ (x.getAs[Int]("sex") + i, 1d)
    i += 5

    //4
    els = els :+ (x.getAs[Int]("age") + i, 1d)
    i += 20

    //5
    els = els :+ (x.getAs[Int]("os") + i, 1d)
    i += 10

    //6
    els = els :+ (x.getAs[Int]("isp") + i, 1d)
    i += 20

    //7
    els = els :+ (x.getAs[Int]("network") + i, 1d)
    i += 10

    //8
    els = els :+ (dict("cityid").getOrElse(x.getAs[Int]("city"), 0) + i, 1d)
    i += dict("cityid").size + 1

    //9
    els = els :+ (dict("slotid").getOrElse(x.getAs[String]("adslotid").toInt, 0) + i, 1d)
    i += dict("slotid").size + 1

    //10
    els = els :+ (x.getAs[Int]("phone_level") + i, 1d)
    i += 10

    //11
    var pnum = x.getAs[Int]("pagenum")
    if (pnum < 0) {
      pnum = 0
    } else if (pnum > 49) {
      pnum = 49
    }
    els = els :+ (pnum + i, 1d)
    i += 50

    //12
    var bid = 0
    try {
      bid = x.getAs[String]("bookid").toInt
    } catch {
      case e: Exception =>
    }
    if (bid < 0) {
      bid = 0
    } else if (bid > 49) {
      bid = 49
    }
    els = els :+ (bid + i, 1d)
    i += 50

    //13
    val adcls = dict("adclass").getOrElse(x.getAs[Int]("adclass"), 0)
    els = els :+ (adcls + i, 1d)
    i += dict("adclass").size + 1

    //14
    els = els :+ (x.getAs[Int]("adtype") + i, 1d)
    i += 20

    //15
    els = els :+ (x.getAs[Int]("adslot_type") + i, 1d)
    i += 10

    //16
    els = els :+ (dict("planid").getOrElse(x.getAs[Int]("planid"), 0) + i, 1d)
    i += dict("planid").size + 1

    //17
    els = els :+ (dict("unitid").getOrElse(x.getAs[Int]("unitid"), 0) + i, 1d)
    i += dict("unitid").size + 1

    //18
    els = els :+ (dict("ideaid").getOrElse(x.getAs[Int]("ideaid"), 0) + i, 1d)
    i += dict("ideaid").size + 1

    //19
    var uran_idx = x.getAs[Int]("user_req_ad_num")
    if (uran_idx < 0) {
      uran_idx = 0
    } else if (uran_idx > 19) {
      uran_idx = 19
    }
    els = els :+ (uran_idx + i, 1d)
    i += 20

    //20
    var urn_idx = x.getAs[Int]("user_req_num")
    if (urn_idx < 0) {
      urn_idx = 0
    } else if (urn_idx < 10) {
      //origin value
    } else if (urn_idx < 20) {
      urn_idx = 10
    } else if (urn_idx < 50) {
      urn_idx = 11
    } else if (urn_idx < 100) {
      urn_idx = 12
    } else {
      urn_idx = 13
    }
    els = els :+ (urn_idx + i, 1d)
    i += 20

    //21
    val appIdx = x.getAs[mutable.WrappedArray[Int]]("appIdx")
    if (appIdx != null){
      appIdx.sortBy(p => p)
        .foreach {
          p =>
            els = els :+ (p + i, 1d)
        }
    }
    i += 500

    try {
      Vectors.sparse(i, els)
    } catch {
      case e: Exception =>
        throw new Exception(els.toString + " " + i.toString + " " + e.getMessage)
        null
    }
  }

  def getCtrVectorParser2(x: Row): Vector = {
    val cal = Calendar.getInstance()
    cal.setTimeInMillis(x.getAs[Int]("timestamp") * 1000L)
    val week = cal.get(Calendar.DAY_OF_WEEK)   //1 to 7
    val hour = cal.get(Calendar.HOUR_OF_DAY)
    var els = Seq[(Int, Double)]()
    var i = 0

    //1
    els = els :+ (week + i - 1, 1d)
    i += 7

    //2
    els = els :+ (hour + i, 1d)
    i += 24

    //3
    els = els :+ (x.getAs[Int]("sex") + i, 1d)
    i += 5

    //4
    els = els :+ (x.getAs[Int]("age") + i, 1d)
    i += 20

    //5
    els = els :+ (x.getAs[Int]("os") + i, 1d)
    i += 10

    //6
    els = els :+ (x.getAs[Int]("isp") + i, 1d)
    i += 20

    //7
    els = els :+ (x.getAs[Int]("network") + i, 1d)
    i += 10

    //8
    els = els :+ (dict("cityid").getOrElse(x.getAs[Int]("city"), 0) + i, 1d)
    i += dict("cityid").size + 1

    //9
    els = els :+ (dict("slotid").getOrElse(x.getAs[String]("adslotid").toInt, 0) + i, 1d)
    i += dict("slotid").size + 1

    //10
    els = els :+ (x.getAs[Int]("phone_level") + i, 1d)
    i += 10

    //11
    var pnum = x.getAs[Int]("pagenum")
    if (pnum < 0) {
      pnum = 0
    } else if (pnum > 49) {
      pnum = 49
    }
    els = els :+ (pnum + i, 1d)
    i += 50

    //12
    var bid = 0
    try {
      bid = x.getAs[String]("bookid").toInt
    } catch {
      case e: Exception =>
    }
    if (bid < 0) {
      bid = 0
    } else if (bid > 49) {
      bid = 49
    }
    els = els :+ (bid + i, 1d)
    i += 50

    //13
    val adcls = dict("adclass").getOrElse(x.getAs[Int]("adclass"), 0)
    els = els :+ (adcls + i, 1d)
    i += dict("adclass").size + 1

    //14
    els = els :+ (x.getAs[Int]("adtype") + i, 1d)
    i += 20

    //15
    els = els :+ (x.getAs[Int]("adslot_type") + i, 1d)
    i += 10

    //16
    els = els :+ (dict("planid").getOrElse(x.getAs[Int]("planid"), 0) + i, 1d)
    i += dict("planid").size + 1

    //17
    els = els :+ (dict("unitid").getOrElse(x.getAs[Int]("unitid"), 0) + i, 1d)
    i += dict("unitid").size + 1

    //18
    els = els :+ (dict("ideaid").getOrElse(x.getAs[Int]("ideaid"), 0) + i, 1d)
    i += dict("ideaid").size + 1

    //19
    var uran_idx = x.getAs[Int]("user_req_ad_num")
    if (uran_idx < 0) {
      uran_idx = 0
    } else if (uran_idx > 19) {
      uran_idx = 19
    }
    els = els :+ (uran_idx + i, 1d)
    i += 20

    //20
    var urn_idx = x.getAs[Int]("user_req_num")
    if (urn_idx < 0) {
      urn_idx = 0
    } else if (urn_idx < 10) {
      //origin value
    } else if (urn_idx < 20) {
      urn_idx = 10
    } else if (urn_idx < 50) {
      urn_idx = 11
    } else if (urn_idx < 100) {
      urn_idx = 12
    } else {
      urn_idx = 13
    }
    els = els :+ (urn_idx + i, 1d)
    i += 20

    //21
    var user_click_unit = x.getAs[Int]("user_long_click_count")
    if (user_click_unit < 0) {
      user_click_unit = 0
    } else if (user_click_unit > 9) {
      user_click_unit = 9
    }
    els = els :+ (user_click_unit + i, 1d)
    i += 10

    //22
    val appIdx = x.getAs[mutable.WrappedArray[Int]]("appIdx")
    if (appIdx != null){
      appIdx.sortBy(p => p)
        .foreach {
          p =>
            els = els :+ (p + i, 1d)
        }
    }
    i += 500

    try {
      Vectors.sparse(i, els)
    } catch {
      case e: Exception =>
        throw new Exception(els.toString + " " + i.toString + " " + e.getMessage)
        null
    }
  }

  def transformFeature1(weights: Array[Double], x: Row): MVec = {
    val cal = Calendar.getInstance()
    cal.setTimeInMillis(x.getAs[Int]("timestamp") * 1000L)
    val week = cal.get(Calendar.DAY_OF_WEEK)   //1 to 7
    val hour = cal.get(Calendar.HOUR_OF_DAY)
    var els = Seq[Double]()
    var i = 0

    //1
    els :+= weights(week + i - 1)
    i += 7

    //2
    els :+= weights(hour + i)
    i += 24

    //3
    els :+= weights(x.getAs[Int]("sex") + i)
    i += 5

    //4
    els :+= weights(x.getAs[Int]("age") + i)
    i += 20

    //5
    els :+= weights(x.getAs[Int]("os") + i)
    i += 10

    //6
    els :+= weights(x.getAs[Int]("isp") + i)
    i += 20

    //7
    els :+= weights(x.getAs[Int]("network") + i)
    i += 10

    //8
    els :+= weights(dict("cityid").getOrElse(x.getAs[Int]("city"), 0) + i)
    i += dict("cityid").size + 1

    //9
    els :+= weights(dict("slotid").getOrElse(x.getAs[String]("adslotid").toInt, 0) + i)
    i += dict("slotid").size + 1

    //10
    els :+= weights(x.getAs[Int]("phone_level") + i)
    i += 10

    //11
    var pnum = x.getAs[Int]("pagenum")
    if (pnum < 0) {
      pnum = 0
    } else if (pnum > 49) {
      pnum = 49
    }
    els :+= weights(pnum + i)
    i += 50

    //12
    var bid = 0
    try {
      bid = x.getAs[String]("bookid").toInt
    } catch {
      case e: Exception =>
    }
    if (bid < 0) {
      bid = 0
    } else if (bid > 49) {
      bid = 49
    }
    els :+= weights(bid + i)
    i += 50

    //13
    val adcls = dict("adclass").getOrElse(x.getAs[Int]("adclass"), 0)
    els :+= weights(adcls + i)
    i += dict("adclass").size + 1

    //14
    els :+= weights(x.getAs[Int]("adtype") + i)
    i += 20

    //15
    els :+= weights(x.getAs[Int]("adslot_type") + i)
    i += 10

    //16
    els :+= weights(dict("planid").getOrElse(x.getAs[Int]("planid"), 0) + i)
    i += dict("planid").size + 1

    //17
    els :+= weights(dict("unitid").getOrElse(x.getAs[Int]("unitid"), 0) + i)
    i += dict("unitid").size + 1

    //18
    els :+= weights(dict("ideaid").getOrElse(x.getAs[Int]("ideaid"), 0) + i)
    i += dict("ideaid").size + 1

    //19
    var uran_idx = x.getAs[Int]("user_req_ad_num")
    if (uran_idx < 0) {
      uran_idx = 0
    } else if (uran_idx > 19) {
      uran_idx = 19
    }
    els :+= weights(uran_idx + i)
    i += 20

    //20
    var urn_idx = x.getAs[Int]("user_req_num")
    if (urn_idx < 0) {
      urn_idx = 0
    } else if (urn_idx < 10) {
      //origin value
    } else if (urn_idx < 20) {
      urn_idx = 10
    } else if (urn_idx < 50) {
      urn_idx = 11
    } else if (urn_idx < 100) {
      urn_idx = 12
    } else {
      urn_idx = 13
    }
    els :+= weights(urn_idx + i)
    i += 20

    //21
    var appw = 0d
    val appIdx = x.getAs[mutable.WrappedArray[Int]]("appIdx")
    if (appIdx != null){
      appIdx.sortBy(p => p)
        .foreach {
          id =>
            appw += weights(id + i)
        }
    }
    els :+= appw
    i += 1

    try {
      MVecs.dense(els.toArray)
    } catch {
      case e: Exception =>
        throw new Exception(els.toString + " " + i.toString + " " + e.getMessage)
        null
    }
  }

  def transformFeature2(weights: Array[Double], x: Row): MVec = {
    val cal = Calendar.getInstance()
    cal.setTimeInMillis(x.getAs[Int]("timestamp") * 1000L)
    val week = cal.get(Calendar.DAY_OF_WEEK)   //1 to 7
    val hour = cal.get(Calendar.HOUR_OF_DAY)
    var els = Seq[Double]()
    var i = 0

    //1
    els :+= weights(week + i - 1)
    i += 7

    //2
    els :+= weights(hour + i)
    i += 24

    //3
    els :+= weights(x.getAs[Int]("sex") + i)
    i += 5

    //4
    els :+= weights(x.getAs[Int]("age") + i)
    i += 20

    //5
    els :+= weights(x.getAs[Int]("os") + i)
    i += 10

    //6
    els :+= weights(x.getAs[Int]("isp") + i)
    i += 20

    //7
    els :+= weights(x.getAs[Int]("network") + i)
    i += 10

    //8
    els :+= weights(dict("cityid").getOrElse(x.getAs[Int]("city"), 0) + i)
    i += dict("cityid").size + 1

    //9
    els :+= weights(dict("slotid").getOrElse(x.getAs[String]("adslotid").toInt, 0) + i)
    i += dict("slotid").size + 1

    //10
    els :+= weights(x.getAs[Int]("phone_level") + i)
    i += 10

    //11
    var pnum = x.getAs[Int]("pagenum")
    if (pnum < 0) {
      pnum = 0
    } else if (pnum > 49) {
      pnum = 49
    }
    els :+= weights(pnum + i)
    i += 50

    //12
    var bid = 0
    try {
      bid = x.getAs[String]("bookid").toInt
    } catch {
      case e: Exception =>
    }
    if (bid < 0) {
      bid = 0
    } else if (bid > 49) {
      bid = 49
    }
    els :+= weights(bid + i)
    i += 50

    //13
    val adcls = dict("adclass").getOrElse(x.getAs[Int]("adclass"), 0)
    els :+= weights(adcls + i)
    i += dict("adclass").size + 1

    //14
    els :+= weights(x.getAs[Int]("adtype") + i)
    i += 20

    //15
    els :+= weights(x.getAs[Int]("adslot_type") + i)
    i += 10

    //16
    els :+= weights(dict("planid").getOrElse(x.getAs[Int]("planid"), 0) + i)
    i += dict("planid").size + 1

    //17
    els :+= weights(dict("unitid").getOrElse(x.getAs[Int]("unitid"), 0) + i)
    i += dict("unitid").size + 1

    //18
    els :+= weights(dict("ideaid").getOrElse(x.getAs[Int]("ideaid"), 0) + i)
    i += dict("ideaid").size + 1

    //19
    var uran_idx = x.getAs[Int]("user_req_ad_num")
    if (uran_idx < 0) {
      uran_idx = 0
    } else if (uran_idx > 19) {
      uran_idx = 19
    }
    els :+= weights(uran_idx + i)
    i += 20

    //20
    var urn_idx = x.getAs[Int]("user_req_num")
    if (urn_idx < 0) {
      urn_idx = 0
    } else if (urn_idx < 10) {
      //origin value
    } else if (urn_idx < 20) {
      urn_idx = 10
    } else if (urn_idx < 50) {
      urn_idx = 11
    } else if (urn_idx < 100) {
      urn_idx = 12
    } else {
      urn_idx = 13
    }
    els :+= weights(urn_idx + i)
    i += 20

    //21
    var user_click_unit = x.getAs[Int]("user_long_click_count")
    if (user_click_unit < 0) {
      user_click_unit = 0
    } else if (user_click_unit > 9) {
      user_click_unit = 9
    }
    els :+= weights(user_click_unit + i)
    i += 10

    //22
    var appw = 0d
    val appIdx = x.getAs[mutable.WrappedArray[Int]]("appIdx")
    if (appIdx != null){
      appIdx.sortBy(p => p)
        .foreach {
          id =>
            appw += weights(id + i)
        }
    }
    els :+= appw
    i += 1

    try {
      MVecs.dense(els.toArray)
    } catch {
      case e: Exception =>
        throw new Exception(els.toString + " " + i.toString + " " + e.getMessage)
        null
    }
  }

  def runIr(spark: SparkSession, binNum: Int, rate: Double): Double = {
    irBinNum = binNum
    val sample = xgbTestResults.randomSplit(Array(rate, 1 - rate), seed = new Date().getTime)
    val bins = binData(sample(0), irBinNum)
    val sc = spark.sparkContext

    // 真实值y轴，预测均值x轴   样本量就是桶的个数
    val ir = new IsotonicRegression().setIsotonic(true).run(sc.parallelize(bins.map(x => (x._1, x._3, 1d))))
    val sum = sample(1) //在测试数据上计算误差
      .map(x => (x._2, ir.predict(x._1))) //(click, calibrate ctr)
      .reduce((x, y) => (x._1 + y._1, x._2 + y._2))

    irError = (sum._2 - sum._1) / sum._1 //误差比
    irmodel = ir
    irError
  }

  private def binData(sample: RDD[(Double, Double)], binNum: Int): Seq[(Double, Double, Double, Double)] = {
    val binSize = sample.count().toInt / binNum  //每个桶的容量
    var bins = Seq[(Double, Double, Double, Double)]()
    var click = 0d  //正例数
    var pv = 0d  //总数或展示数
    var pSum = 0d  //预测值的累加
    var pMin = 1d  // 最小的预测值
    var pMax = 0d  // 最大的预测值
    var n = 0  //控制打印
    sample.sortByKey()  //(p, label)按照XGB预测值升序排序
      .toLocalIterator
      .foreach {
        x =>
          pSum = pSum + x._1
          if (x._1 < pMin) {
            pMin = x._1
          }
          if (x._1 > pMax) {
            pMax = x._1
          }
          if (x._2 > 0.01) {
            click = click + 1
          }
          pv = pv + 1
          if (pv >= binSize) {  //如果超过通的容量，就换下一个桶
            val ctr = click / pv    //  点击/展示
            bins = bins :+ (ctr, pMin, pSum / pv, pMax)  // 真实值，最小值，预测均值，最大值
            n = n + 1
            if (n > binNum - 20) {
              val logStr = "bin %d: %.6f(%d/%d) %.6f %.6f %.6f".format(
                n, ctr, click.toInt, pv.toInt, pMin, pSum / pv, pMax) //桶号：真实ctr（点击/展示），最小值，预测均值，最大值

              binsLog = binsLog :+ logStr
              println(logStr)
            }

            click = 0d
            pv = 0d
            pSum = 0d
            pMin = 1d
            pMax = 0d
          }
      }
    bins
  }

  def savePbPack(path: String, parser: String): Unit = {
    val weights = mutable.Map[Int, Double]()
    lrmodel.weights.toSparse.foreachActive {
      case (i, d) =>
        weights.update(i, d)
    }
    val lr = LRModel(
      parser = parser,
      featureNum = lrmodel.numFeatures,
      auPRC = auPRC,
      auROC = auROC,
      weights = weights.toMap
    )
    /*
    val ir = IRModel(
      boundaries = irmodel.boundaries.toSeq,
      predictions = irmodel.predictions.toSeq,
      meanSquareError = irError * irError
    )
    */
    val dictpb = Dict(
      planid = dict("planid"),
      unitid = dict("unitid"),
      ideaid = dict("ideaid"),
      slotid = dict("slotid"),
      adclass = dict("adclass"),
      cityid = dict("cityid"),
      mediaid = dict("mediaid"),
      appid = dictStr("appid")
    )

    val pack = Pack(
      createTime = new Date().getTime,
      lr = Option(lr),
      //ir = Option(ir),
      dict = Option(dictpb),
      strategy = Strategy.StrategyXgboostLR
    )

    pack.writeTo(new FileOutputStream(path))
  }

  def saveLrPbPack(path: String, parser: String, type1: Int): Unit = {
    val weights = mutable.Map[Int, Double]()
    lrmodel.weights.toSparse.foreachActive {
      case (i, d) =>
        weights.update(i, d)
    }
    val lr = LRModel(
      parser = parser,
      featureNum = lrmodel.numFeatures,
      auPRC = auPRC,
      auROC = auROC,
      weights = weights.toMap
    )
    val ir = IRModel(
      boundaries = irmodel.boundaries.toSeq,
      predictions = irmodel.predictions.toSeq,
      meanSquareError = irError * irError
    )
    println("ir boundaries:")
    println(irmodel.boundaries.toSeq)
    println("ir predictions:")
    println(irmodel.predictions.toSeq)
    val dictpb = Dict(
//      planid = dict("planid"),
//      unitid = dict("unitid"),
//      ideaid = dict("ideaid"),
//      slotid = dict("slotid"),
//      adclass = dict("adclass"),
//      cityid = dict("cityid"),
//      mediaid = dict("mediaid"),
//      appid = dictStr("appid")
    )

    var type2 = "list"
    if (type1 == 1) {
      type2 = "list"
    } else if (type1 == 2) {
      type2 = "content"
    } else if (type1 == 3) {
      type2 = "interact"
    } else {
      type2 = "unknown"
    }
    println(s"type2=$type2")

    val pack = Pack(
      name = s"qtt-$type2-ctr-portrait11",
      createTime = new Date().getTime,
      lr = Option(lr),
      ir = Option(ir),
      dict = Option(dictpb),
      strategy = Strategy.StrategyXgboostLR,
      gbmfile = s"data/ctr-portrait9-qtt-$type2.gbm",
      gbmTreeLimit = 200,
      gbmTreeDepth = 10
    )

    pack.writeTo(new FileOutputStream(path))
  }

  def printXGBTestLog(lrTestResults: RDD[(Double, Double)]): Unit = {
    val testSum = lrTestResults.count()
    if (testSum < 0) {
      throw new Exception("must run lr test first or test results is empty")
    }
    var test0 = 0 //反例数
    var test1 = 0 //正例数
    lrTestResults
      .map {
        x =>
          var label = 0
          if (x._2 > 0.01) {
            label = 1
          }
          (label, 1)
      }
      .reduceByKey((x, y) => x + y)
      .toLocalIterator
      .foreach {
        x =>
          if (x._1 == 1) {
            test1 = x._2
          } else {
            test0 = x._2
          }
      }

    var log = "predict distribution %s %d(1) %d(0)\n".format(testSum, test1, test0)
    lrTestResults  //(p, label)
      .map {
      x =>
        val v = (x._1 * 100).toInt / 5
        ((v, x._2.toInt), 1)
    }  //  ((预测值,lable),1)
      .reduceByKey((x, y) => x + y)  //  ((预测值,lable),num)
      .map {
      x =>
        val key = x._1._1
        val label = x._1._2
        if (label == 0) {
          (key, (x._2, 0))  //  (预测值,(num1,0))
        } else {
          (key, (0, x._2))  //  (预测值,(0,num2))
        }
    }
      .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))  //  (预测值,(num1反,num2正))
      .sortByKey(false)
      .toLocalIterator
      .foreach {
        x =>
          val sum = x._2
          val pre = x._1.toDouble * 0.05
          if (pre>0.2) {
            //isUpdateModel = true
          }
          log = log + "%.2f %d %.4f %.4f %d %.4f %.4f %.4f\n".format(
            pre, //预测值
            sum._2, //正例数
            sum._2.toDouble / test1.toDouble, //该准确率下的正例数/总正例数
            sum._2.toDouble / testSum.toDouble, //该准确率下的正例数/总数
            sum._1,  //反例数
            sum._1.toDouble / test0.toDouble, //该准确率下的反例数/总反例数
            sum._1.toDouble / testSum.toDouble, //该准确率下的反例数/总数
            sum._2.toDouble / (sum._1 + sum._2).toDouble)  // 真实值
      }

    println(log)
    trainLog :+= log
  }

  def evaluate(spark: SparkSession, dataType: String, mlmfile: String): Unit = {
    val cal = Calendar.getInstance()
    cal.add(Calendar.HOUR, -4)
    val date = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime)
    val part = new SimpleDateFormat("yyyy-MM-dd/HH").format(cal.getTime)

    val mlm = Pack.parseFrom(new FileInputStream(mlmfile))
    val dictData = mlm.dict.get
    dict.update("mediaid", dictData.mediaid)
    dict.update("planid", dictData.planid)
    dict.update("ideaid", dictData.ideaid)
    dict.update("unitid", dictData.unitid)
    dict.update("slotid", dictData.slotid)
    dict.update("adclass", dictData.adclass)
    dict.update("cityid", dictData.cityid)

    import spark.implicits._
    val uidApp = spark.read.parquet("/user/cpc/userInstalledApp/%s/*".format(date)).rdd
      .map(x => (x.getAs[String]("uid"),x.getAs[mutable.WrappedArray[String]]("pkgs")))
      .reduceByKey(_ ++ _)
      .map(x => (x._1,x._2.distinct))
      .toDF("uid","pkgs").rdd
    val appids = dictData.appid
    val userAppids = getUserAppIdx(spark, uidApp, appids)

    var qtt = spark.read.parquet("/user/cpc/lrmodel/ctrdata_v1/%s/*".format(date)).coalesce(200)
    if (dataType == "qtt-list") {
      qtt = qtt.filter{
        x =>
          Seq("80000001", "80000002").contains(x.getAs[String]("media_appsid")) &&
            x.getAs[Int]("adslot_type") == 1 && x.getAs[Int]("ideaid") > 0
      }
    } else if (dataType == "qtt-content") {
      qtt = qtt.filter{
        x =>
          Seq("80000001", "80000002").contains(x.getAs[String]("media_appsid")) &&
            x.getAs[Int]("adslot_type") == 2 && x.getAs[Int]("ideaid") > 0
      }
    } else if (dataType == "qtt-all") {
      qtt = qtt.filter{
        x =>
          Seq("80000001", "80000002").contains(x.getAs[String]("media_appsid")) &&
            Seq(1, 2).contains(x.getAs[Int]("adslot_type")) && x.getAs[Int]("ideaid") > 0
      }
    } else {
      qtt = qtt.filter{ x => x.getAs[Int]("ideaid") > 0 }
    }
    qtt = getLimitedData(spark, 1e7, qtt).join(userAppids, Seq("uid"), "leftouter").cache()

    val lr = LogisticRegressionModel.load(spark.sparkContext, "/user/cpc/xgboost_lr/2018-04-09-14-51")
    val BcDict = spark.sparkContext.broadcast(dict)
    val BcWeights = spark.sparkContext.broadcast(lr.weights)
    val lrtest = qtt.rdd
      .mapPartitions {
        p =>
          dict = BcDict.value
          p.map {
            u =>
              val vec = getCtrVectorParser1(u)
              LabeledPoint(u.getAs[Int]("label").toDouble, vec)
          }
      }

    println(lrtest.first())
    lr.clearThreshold()
    val lrresults = lrtest.map { r => (lr.predict(r.features), r.label) }
    printXGBTestLog(lrresults)
    val metrics = new BinaryClassificationMetrics(lrresults)
    val auPRC = metrics.areaUnderPR
    val auROC = metrics.areaUnderROC
    println(auPRC, auROC)

    val xgbtest = qtt
      .mapPartitions {
        p =>
          dict = BcDict.value
          val weights = BcWeights.value.toArray
          p.map {
            r =>
              val vec = transformFeature1(weights, r)
              (r.getAs[Int]("label"), vec)
          }
      }
      .toDF("label", "features")
    println(xgbtest.first())
    xgbtest
      .map {
        x =>
          val label = x.getAs[Int]("label")
          val vec = x.getAs[MVec]("features")
          var svm = label.toString
          vec.foreachActive {
            (i, v) =>
              svm = svm + " %d:%.20f".format(i + 1, v)
          }
          svm
      }
      .write.mode(SaveMode.Overwrite).text("/user/cpc/xgboost_eval_svm_v1")
  }

  def testServer(spark: SparkSession, dataType: String, mlmfile: String): Unit = {
    val cal = Calendar.getInstance()
    cal.add(Calendar.HOUR, -4)
    val date = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime)
    val part = new SimpleDateFormat("yyyy-MM-dd/HH").format(cal.getTime)

    val mlm = Pack.parseFrom(new FileInputStream(mlmfile))
    val dictData = mlm.dict.get
    dict.update("mediaid", dictData.mediaid)
    dict.update("planid", dictData.planid)
    dict.update("ideaid", dictData.ideaid)
    dict.update("unitid", dictData.unitid)
    dict.update("slotid", dictData.slotid)
    dict.update("adclass", dictData.adclass)
    dict.update("cityid", dictData.cityid)

    import spark.implicits._
    val uidApp = spark.read.parquet("/user/cpc/userInstalledApp/%s/*".format(date)).rdd
      .map(x => (x.getAs[String]("uid"),x.getAs[mutable.WrappedArray[String]]("pkgs")))
      .reduceByKey(_ ++ _)
      .map(x => (x._1,x._2.distinct))
      .toDF("uid","pkgs").rdd
    val appids = dictData.appid
    val userAppids = getUserAppIdx(spark, uidApp, appids)

    var qtt = spark.read.parquet("/user/cpc/lrmodel/ctrdata_v1/%s/*".format(date)).coalesce(200)
    if (dataType == "qtt-list") {
      qtt = qtt.filter{
        x =>
          Seq("80000001", "80000002").contains(x.getAs[String]("media_appsid")) &&
            x.getAs[Int]("adslot_type") == 1 && x.getAs[Int]("ideaid") > 0
      }
    } else if (dataType == "qtt-content") {
      qtt = qtt.filter{
        x =>
          Seq("80000001", "80000002").contains(x.getAs[String]("media_appsid")) &&
            x.getAs[Int]("adslot_type") == 2 && x.getAs[Int]("ideaid") > 0
      }
    } else if (dataType == "qtt-all") {
      qtt = qtt.filter{
        x =>
          Seq("80000001", "80000002").contains(x.getAs[String]("media_appsid")) &&
            Seq(1, 2).contains(x.getAs[Int]("adslot_type")) && x.getAs[Int]("ideaid") > 0
      }
    } else {
      qtt = qtt.filter{ x => x.getAs[Int]("ideaid") > 0 }
    }
    val n = 20
    qtt = qtt.limit(n).join(userAppids, Seq("uid"), "leftouter").cache()

    val lrm = mlm.lr.get
    val wvec = Vectors.sparse(lrm.featureNum, lrm.weights.toSeq)
    val BcDict = spark.sparkContext.broadcast(dict)
    val BcWeights = spark.sparkContext.broadcast(wvec)
    val lrtest = qtt.rdd
      .mapPartitions {
        p =>
          dict = BcDict.value
          p.map {
            u =>
              val vec = getCtrVectorParser1(u)
              LabeledPoint(u.getAs[Int]("label").toDouble, vec)
          }
      }

    lrtest.take(n).foreach(println)

    val xgbtest = qtt
      .mapPartitions {
        p =>
          dict = BcDict.value
          val weights = BcWeights.value.toArray
          p.map {
            r =>
              val vec = transformFeature1(weights, r)
              (r.getAs[Int]("label"), vec)
          }
      }
      .toDF("label", "features")
    xgbtest.take(n).foreach(println)
    xgbtest
      .map {
        x =>
          val label = x.getAs[Int]("label")
          val vec = x.getAs[MVec]("features")
          var svm = label.toString
          vec.foreachActive {
            (i, v) =>
              svm = svm + " %d:%.20f".format(i + 1, v)
          }
          svm
      }
      .write.mode(SaveMode.Overwrite).text("/user/cpc/xgboost_stub_svm_v1")

    val conf = ConfigFactory.load()
    val channel = ManagedChannelBuilder
      .forAddress("cpc-dumper02", 9092)
      .usePlaintext(true)
      .build
    val redis = new RedisClient(conf.getString("redis.host"), conf.getInt("redis.port"))
    qtt.rdd.toLocalIterator
      .foreach {
        r =>
          var seq = Seq[String]()
          val uid = r.getAs[String]("uid") + "_UPDATA"

          val buffer = redis.get[Array[Byte]](uid).getOrElse(null)
          if (buffer != null) {
            val installpkg = UserProfile.parseFrom(buffer).getInstallpkgList
            for( i <- 0 until installpkg.size){
              val name = installpkg.get(i).getPackagename
              seq = seq :+ name
            }
          }

          val (ad, m, slot, u, loc, n, d, t) = unionLogToObject(r, seq)
          var ads = Seq[AdInfo]()
          for (i <- 1 to 1) {
            ads = ads :+ ad
          }

          val req = Request(
            version = "test",
            ads = ads,
            media = Option(m),
            user = Option(u),
            loc = Option(loc),
            network = Option(n),
            device = Option(d),
            time = r.getAs[Int]("timestamp"),
            cvrVersion = "cvr_test",
            adSlot = Option(slot)
          )

          val blockingStub = PredictorGrpc.blockingStub(channel)
          val reply = blockingStub.predict(req)

          println("ideaid=%d scala=%.8f ml=%.8f".format(ad.ideaid, 0d, reply.results(0).value))
      }
  }

  def unionLogToObject(x: Row, seq: Seq[String]): (AdInfo, Media, AdSlot, User, Location, Network, Device, Long) = {
    val ad = AdInfo(
      ideaid = x.getAs[Int]("ideaid"),
      unitid = x.getAs[Int]("unitid"),
      planid = x.getAs[Int]("planid"),
      adtype = x.getAs[Int]("adtype"),
      _class = x.getAs[Int]("adclass"),
      showCount = x.getAs[Int]("user_req_ad_num"),
      clickCount = x.getAs[Int]("user_click_unit_num")
    )
    val m = Media(
      mediaAppsid = x.getAs[String]("media_appsid").toInt
    )
    val slot = AdSlot(
      adslotid = x.getAs[String]("adslotid").toInt,
      adslotType = x.getAs[Int]("adslot_type"),
      pageNum = x.getAs[Int]("pagenum"),
      bookId = x.getAs[String]("bookid")
    )
    val u = User(
      sex = x.getAs[Int]("sex"),
      age = x.getAs[Int]("age"),
      installpkg = seq,
      reqCount = x.getAs[Int]("user_req_num"),
      clickCount = x.getAs[Int]("user_click_num")
    )
    val n = Network(
      network = x.getAs[Int]("network"),
      isp = x.getAs[Int]("isp")
    )
    val loc = Location(
      city = x.getAs[Int]("city")
    )
    val d = Device(
      os = x.getAs[Int]("os"),
      phoneLevel = x.getAs[Int]("phone_level")
    )
    (ad, m, slot, u, loc, n, d, x.getAs[Int]("timestamp") * 1000L)
  }


}


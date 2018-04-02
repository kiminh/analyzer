package com.cpc.spark.ml.xgboost

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import com.cpc.spark.common.Utils
import com.cpc.spark.ml.common.{Utils => MUtils}
import com.cpc.spark.ml.train.LRIRModel
import com.typesafe.config.ConfigFactory
import mlserver.mlserver._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}

import scala.collection.mutable
import scala.collection.mutable.WrappedArray
import scala.util.Random

/**
  * Created by zhaolei on 22/12/2017.
  */
object LRTransform {

  private val days = 7
  private val daysCvr = 20
  private var trainLog = Seq[String]()
  private val model = new LRIRModel


  def main(args: Array[String]): Unit = {
    Logger.getRootLogger.setLevel(Level.WARN)
    val spark: SparkSession = model.initSpark("cpc lr model")

    //按分区取数据
    val ctrPathSep = getPathSeq(args(0).toInt)
    val cvrPathSep = getPathSeq(args(1).toInt)

    initFeatureDict(spark, ctrPathSep)

    val userAppIdx = getUidApp(spark, ctrPathSep)

    val ulog = getData(spark,"ctrdata_v1",ctrPathSep)
      .filter(_.getAs[Int]("ideaid") > 0)

    trainLog :+= "ulog nums = %d".format(ulog.rdd.count)
    trainLog :+= "ulog NumPartitions = %d".format(ulog.rdd.getNumPartitions)

    //qtt-all-parser3-hourly
    model.clearResult()
    val qttAll = ulog.filter(x => Seq("80000001", "80000002").contains(x.getAs[String]("media_appsid")) &&
      Seq(1, 2).contains(x.getAs[Int]("adslot_type")))
    train(spark, "ctrparser2", "qtt-all-ctrparser2-hourly", qttAll, "", 4e8)


    ulog.unpersist()
    userAppIdx.unpersist()
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
      .toDF("uid","pkgs").rdd

    val ids = getTopApp(uidApp, 1000)
    dictStr.update("appid",ids)

    val userAppIdx = getUserAppIdx(spark, uidApp, ids)
      .repartition(1000)

    uidApp.unpersist()

    userAppIdx
  }

  //安装列表中top k的App
  def getTopApp(uidApp : RDD[Row], k : Int): Map[String,Int] ={
    var idx = 0
    val ids = mutable.Map[String,Int]()
    uidApp
      .flatMap(x => x.getAs[WrappedArray[String]]("pkgs").map((_,1)))
      .reduceByKey(_ + _)
      .sortBy(_._2,false)
      .toLocalIterator
      .take(k)
      .foreach{
        id =>
          idx += 1
          ids.update(id._1,idx)
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

  //用户安装列表特征合并到原有特征
  def getLeftJoinData(data: DataFrame, userAppIdx: DataFrame): DataFrame ={
    data.join(userAppIdx,Seq("uid"),"leftouter")
  }

  def train(spark: SparkSession, parser: String, name: String, ulog: DataFrame, destfile: String, n: Double): Unit = {
    trainLog :+= "\n------train log--------"
    trainLog :+= "name = %s".format(name)
    trainLog :+= "parser = %s".format(parser)
    trainLog :+= "destfile = %s".format(destfile)

    val num = ulog.count().toDouble
    println("sample num", num)
    trainLog :+= "total size %.0f".format(num)

    //最多n条训练数据
    var trainRate = 0.9
    if (num * trainRate > n) {
      trainRate = n / num
    }

    //最多1000w条测试数据
    var testRate = 0.09
    if (num * testRate > 1e7) {
      testRate = 1e7 / num
    }

    var Array(train, test, tmp) = ulog
      .randomSplit(Array(trainRate, testRate, 1 - trainRate - testRate), new Date().getTime)
    ulog.unpersist()

    val tnum = train.count().toDouble
    val pnum = train.filter(_.getAs[Int]("label") > 0).count().toDouble
    val nnum = tnum - pnum

    //保证训练数据正负比例 1:9
    val rate = (pnum * 9 / nnum * 1000).toInt
    println("total positive negative", tnum, pnum, nnum, rate)
    trainLog :+= "train size total=%.0f positive=%.0f negative=%.0f scaleRate=%d/1000".format(tnum, pnum, nnum, rate)

    train = getLimitedData(2e7, train.filter(x => x.getAs[Int]("label") > 0 || Random.nextInt(1000) < rate))
    val sampleTrain = formatSample(spark, parser, train)
    val sampleTest = formatSample(spark, parser, test)

    model.run(sampleTrain, 200, 1e-8)
    model.test(sampleTest)
    model.printLrTestLog()

    val lr = model.getLRmodel()
    Utils.deleteHdfs("/user/cpc/xgboost_lr")
    model.saveHdfs("/user/cpc/xgboost_lr")
    val BcDict = spark.sparkContext.broadcast(dict)
    val BcWeights = spark.sparkContext.broadcast(lr.weights)

    import spark.implicits._

    train
      .mapPartitions {
        p =>
          dict = BcDict.value
          val weights = BcWeights.value.toArray
          p.map {
            r =>

              var svm = r.getAs[Int]("label").toString
              val vec = transformFeature(weights, r)
              vec.foreachActive {
                (i, v) =>
                  svm = svm + " %d:%f".format(i + 1, v)
              }
              svm
          }
      }
      .write.mode(SaveMode.Overwrite).text("/user/cpc/xgboost_train_svm_v2")

    test
      .mapPartitions {
        p =>
          dict = BcDict.value
          val weights = BcWeights.value.toArray
          p.map {
            r =>

              var svm = r.getAs[Int]("label").toString
              val vec = transformFeature(weights, r)
              vec.foreachActive {
                (i, v) =>
                  svm = svm + " %d:%f".format(i + 1, v)
              }
              svm
          }
      }
      .write.mode(SaveMode.Overwrite).text("/user/cpc/xgboost_test_svm_v2")

    println("done")
    System.exit(1)

    trainLog :+= model.getLrTestLog()


    val testNum = sampleTest.count().toDouble * 0.9
    val minBinSize = 1000d
    var binNum = 1000d
    if (testNum < minBinSize * binNum) {
      binNum = testNum / minBinSize
    }

    model.runIr(binNum.toInt, 0.95)
    trainLog :+= model.binsLog.mkString("\n")

    val date = new SimpleDateFormat("yyyy-MM-dd-HH-mm").format(new Date().getTime)
    val lrfilepath = "/data/cpc/anal/model/lrmodel-%s-%s.lrm".format(name, date)
    model.saveHdfs("/user/cpc/lrmodel/lrmodeldata/%s".format(date))
    model.saveIrHdfs("/user/cpc/lrmodel/irmodeldata/%s".format(date))
    model.savePbPack(parser, lrfilepath, dict.toMap, dictStr.toMap)

    trainLog :+= "protobuf pack %s".format(lrfilepath)

    trainLog :+= "\n-------update server data------"
    if (destfile.length > 0) {
      trainLog :+= MUtils.updateOnlineData(lrfilepath, destfile, ConfigFactory.load())
    }
  }

  def getLimitedData(limitedNum: Double, ulog: DataFrame): DataFrame = {
    var rate = 1d
    val num = ulog.count().toDouble

    if (num > limitedNum) {
      rate = limitedNum / num
    }

    ulog.randomSplit(Array(rate, 1 - rate), new Date().getTime)(0)
  }

  def formatSample(spark: SparkSession, parser: String, ulog: DataFrame): RDD[LabeledPoint] = {
    val BcDict = spark.sparkContext.broadcast(dict)

    ulog.rdd
      .mapPartitions {
        p =>
          dict = BcDict.value
          p.map {
            u =>
              val vec = parser match {
                case "ctrparser2" =>
                  getCtrVectorParser2(u)
              }
              LabeledPoint(u.getAs[Int]("label").toDouble, vec)
          }
      }
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
    pathSep.map{
      x =>
        path = path :+ "/user/cpc/lrmodel/%s/%s/{%s}".format(dataVersion, x._1, x._2.mkString(","))
    }

    path.foreach{
      x =>
        trainLog :+= x
        println(x)
    }

    spark.read.parquet(path:_*).repartition(1000)
  }

  /*
  def parseFeature(row: Row): Vector = {
    val (ad, m, slot, u, loc, n, d, t) = unionLogToObject(row)
    var svm = ""
    getVector(ad, m, slot, u, loc, n, d, t)
  }
  */

  def unionLogToObject(x: Row, seq: Seq[String]): (AdInfo, Media, AdSlot, User, Location, Network, Device, Long) = {
    val ad = AdInfo(
      ideaid = x.getAs[Int]("ideaid"),
      unitid = x.getAs[Int]("unitid"),
      planid = x.getAs[Int]("planid"),
      adtype = x.getAs[Int]("adtype"),
      _class = x.getAs[Int]("adclass"),
      showCount = x.getAs[Int]("user_req_ad_num")
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
      reqCount = x.getAs[Int]("user_req_num")
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

  def getCtrVectorParser2(x: Row): Vector = {
    val cal = Calendar.getInstance()
    cal.setTimeInMillis(x.getAs[Int]("timestamp") * 1000L)
    val week = cal.get(Calendar.DAY_OF_WEEK)   //1 to 7
    val hour = cal.get(Calendar.HOUR_OF_DAY)
    var els = Seq[(Int, Double)]()
    var i = 0

    els = els :+ (week + i - 1, 1d)
    i += 7

    //(24)
    els = els :+ (hour + i, 1d)
    i += 24

    //sex
    els = els :+ (x.getAs[Int]("sex") + i, 1d)
    i += 9

    //age
    els = els :+ (x.getAs[Int]("age") + i, 1d)
    i += 100

    //os 96 - 97 (2)
    els = els :+ (x.getAs[Int]("os") + i, 1d)
    i += 10

    //isp
    els = els :+ (x.getAs[Int]("isp") + i, 1d)
    i += 20

    //net
    els = els :+ (x.getAs[Int]("network") + i, 1d)
    i += 10

    els = els :+ (dict("cityid").getOrElse(x.getAs[Int]("city"), 0) + i, 1d)
    i += dict("cityid").size + 1

    //ad slot id
    els = els :+ (dict("slotid").getOrElse(x.getAs[String]("adslotid").toInt, 0) + i, 1d)
    i += dict("slotid").size + 1

    //0 to 4
    els = els :+ (x.getAs[Int]("phone_level") + i, 1d)
    i += 10

    //pagenum
    var pnum = x.getAs[Int]("pagenum")
    if (pnum < 0) {
      pnum = 0
    } else if (pnum > 49) {
      pnum = 49
    }
    els = els :+ (pnum + i, 1d)
    i += 50

    //bookid
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

    //ad class
    val adcls = dict("adclass").getOrElse(x.getAs[Int]("adclass"), 0)
    els = els :+ (adcls + i, 1d)
    i += dict("adclass").size + 1

    //adtype
    els = els :+ (x.getAs[Int]("adtype") + i, 1d)
    i += 10

    //adslot_type
    els = els :+ (x.getAs[Int]("adslot_type") + i, 1d)
    i += 10

    //planid
    els = els :+ (dict("planid").getOrElse(x.getAs[Int]("planid"), 0) + i, 1d)
    i += dict("planid").size + 1

    //unitid
    els = els :+ (dict("unitid").getOrElse(x.getAs[Int]("unitid"), 0) + i, 1d)
    i += dict("unitid").size + 1

    //ideaid
    els = els :+ (dict("ideaid").getOrElse(x.getAs[Int]("ideaid"), 0) + i, 1d)
    i += dict("ideaid").size + 1

    //user_req_ad_num
    var uran_idx = x.getAs[Int]("user_req_ad_num")
    if (uran_idx < 0) {
      uran_idx = 0
    } else if (uran_idx > 19) {
      uran_idx = 19
    }
    els = els :+ (uran_idx + i, 1d)
    i += 20

    //user_req_num
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

    var user_click = x.getAs[Int]("user_click_num")
    if (user_click < 0) {
      user_click = 0
    } else if (user_click > 19) {
      user_click = 19
    }
    els = els :+ (user_click + i, 1d)
    i += 20

    var user_click_unit = x.getAs[Int]("user_click_unit_num")
    if (user_click_unit < 0) {
      user_click_unit = 0
    } else if (user_click_unit > 9) {
      user_click_unit = 9
    }
    els = els :+ (user_click + i, 1d)
    i += 10


    try {
      Vectors.sparse(i, els)
    } catch {
      case e: Exception =>
        throw new Exception(els.toString + " " + i.toString + " " + e.getMessage)
        null
    }
  }

  def transformFeature(weights: Array[Double], x: Row): Vector = {
    val cal = Calendar.getInstance()
    cal.setTimeInMillis(x.getAs[Int]("timestamp") * 1000L)
    val week = cal.get(Calendar.DAY_OF_WEEK)   //1 to 7
    val hour = cal.get(Calendar.HOUR_OF_DAY)
    var els = Seq[Double]()
    var i = 0

    els :+= weights(week + i - 1)
    i += 7

    els :+= weights(hour + i)
    i += 24

    els :+= weights(x.getAs[Int]("sex") + i)
    i += 9

    els :+= weights(x.getAs[Int]("age") + i)
    i += 100

    els :+= weights(x.getAs[Int]("os") + i)
    i += 10

    els :+= weights(x.getAs[Int]("isp") + i)
    i += 20

    els :+= weights(x.getAs[Int]("network") + i)
    i += 10

    els :+= weights(dict("cityid").getOrElse(x.getAs[Int]("city"), 0) + i)
    i += dict("cityid").size + 1

    els :+= weights(dict("slotid").getOrElse(x.getAs[String]("adslotid").toInt, 0) + i)
    i += dict("slotid").size + 1

    els :+= weights(x.getAs[Int]("phone_level") + i)
    i += 10

    //pagenum
    var pnum = x.getAs[Int]("pagenum")
    if (pnum < 0) {
      pnum = 0
    } else if (pnum > 49) {
      pnum = 49
    }
    els :+= weights(pnum + i)
    i += 50

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

    val adcls = dict("adclass").getOrElse(x.getAs[Int]("adclass"), 0)
    els :+= weights(adcls + i)
    i += dict("adclass").size + 1

    els :+= weights(x.getAs[Int]("adtype") + i)
    i += 10

    els :+= weights(x.getAs[Int]("adslot_type") + i)
    i += 10

    els :+= weights(dict("planid").getOrElse(x.getAs[Int]("planid"), 0) + i)
    i += dict("planid").size + 1

    els :+= weights(dict("unitid").getOrElse(x.getAs[Int]("unitid"), 0) + i)
    i += dict("unitid").size + 1

    els :+= weights(dict("ideaid").getOrElse(x.getAs[Int]("ideaid"), 0) + i)
    i += dict("ideaid").size + 1

    //user_req_ad_num
    var uran_idx = x.getAs[Int]("user_req_ad_num")
    if (uran_idx < 0) {
      uran_idx = 0
    } else if (uran_idx > 19) {
      uran_idx = 19
    }
    els :+= weights(uran_idx + i)
    i += 20

    //user_req_num
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

    var user_click = x.getAs[Int]("user_click_num")
    if (user_click < 0) {
      user_click = 0
    } else if (user_click > 19) {
      user_click = 19
    }
    els :+ weights(user_click + i)
    i += 20

    var user_click_unit = x.getAs[Int]("user_click_unit_num")
    if (user_click_unit < 0) {
      user_click_unit = 0
    } else if (user_click_unit > 9) {
      user_click_unit = 9
    }
    els :+= weights(user_click + i)
    i += 10

    try {
      Vectors.dense(els.toArray)
    } catch {
      case e: Exception =>
        throw new Exception(els.toString + " " + i.toString + " " + e.getMessage)
        null
    }
  }

}


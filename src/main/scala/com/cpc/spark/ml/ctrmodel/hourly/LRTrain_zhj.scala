
package com.cpc.spark.ml.ctrmodel.hourly

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
object LRTrain_zhj {

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

    val userAppIdx = getUidApp(spark, ctrPathSep).cache()

    //按分区取数据
    var cvrUlog = getData(spark, "cvrdata_v2", cvrPathSep).cache()

    //去掉长尾广告id
    var minIdeaNum = 50
    var ideaids = cvrUlog.select("ideaid")
      .groupBy("ideaid")
      .count()
      .where("count > %d".format(minIdeaNum))

    var sample = cvrUlog.join(ideaids, Seq("ideaid")).cache()
    println(cvrUlog.count(), sample.count())
    cvrUlog.unpersist()

    var cvrUserAppIdx = getUidApp(spark, cvrPathSep)
    initFeatureDict(spark, cvrPathSep)

    var cvrQttAll = sample.where("media_appsid in (\"80000001\", \"80000002\") and adslot_type in (1, 2)")
      .join(cvrUserAppIdx, Seq("uid"), "leftouter")
      .cache()
    sample.unpersist()

    //cvr-qtt-all-parser3-hourly
    model.clearResult()
    train(spark, "parser3", "cvr-qtt-all-parser3-hourly", cvrQttAll, "cvr-qtt-all-parser3-hourly.lrm", 1e8)

    cvrQttAll.unpersist()

    userAppIdx.unpersist()
  }


  def getPathSeq(days: Int): mutable.Map[String, Seq[String]] = {
    var date = ""
    var hour = ""
    val cal = Calendar.getInstance()
    cal.add(Calendar.HOUR, -(days * 24 + 2))
    val pathSep = mutable.Map[String, Seq[String]]()

    for (n <- 1 to days * 24) {
      date = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime)
      hour = new SimpleDateFormat("HH").format(cal.getTime)
      pathSep.update(date, (pathSep.getOrElse(date, Seq[String]()) :+ hour))
      cal.add(Calendar.HOUR, 1)
    }

    pathSep
  }


  def getUidApp(spark: SparkSession, pathSep: mutable.Map[String, Seq[String]]): DataFrame = {
    val inpath = "/user/cpc/userInstalledApp/{%s}".format(pathSep.keys.mkString(","))
    println(inpath)

    import spark.implicits._
    val uidApp = spark.read.parquet(inpath).rdd
      .map(x => (x.getAs[String]("uid"), x.getAs[WrappedArray[String]]("pkgs")))
      .reduceByKey(_ ++ _)
      .map(x => (x._1, x._2.distinct))
      .toDF("uid", "pkgs").rdd.cache()

    val ids = getTopApp(uidApp, 1000)
    dictStr.update("appid", ids)

    val userAppIdx = getUserAppIdx(spark, uidApp, ids)
      .repartition(1000)

    uidApp.unpersist()

    userAppIdx
  }

  //安装列表中top k的App
  def getTopApp(uidApp: RDD[Row], k: Int): Map[String, Int] = {
    var idx = 0
    val ids = mutable.Map[String, Int]()
    uidApp
      .flatMap(x => x.getAs[WrappedArray[String]]("pkgs").map((_, 1)))
      .reduceByKey(_ + _)
      .sortBy(_._2, false)
      .toLocalIterator
      .take(k)
      .foreach {
        id =>
          idx += 1
          ids.update(id._1, idx)
      }
    ids.toMap
  }

  def isMorning(): Boolean = {
    new SimpleDateFormat("HH").format(new Date().getTime) <= "17"
  }

  //用户安装列表对应的App idx
  def getUserAppIdx(spark: SparkSession, uidApp: RDD[Row], ids: Map[String, Int]): DataFrame = {
    import spark.implicits._
    uidApp.map {
      x =>
        val k = x.getAs[String]("uid")
        val v = x.getAs[WrappedArray[String]]("pkgs").map(p => (ids.getOrElse(p, 0))).filter(_ > 0)
        (k, v)
    }.toDF("uid", "appIdx")
  }

  //用户安装列表特征合并到原有特征
  def getLeftJoinData(data: DataFrame, userAppIdx: DataFrame): DataFrame = {
    data.join(userAppIdx, Seq("uid"), "leftouter")
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

    val Array(train, test, tmp) = ulog
      .randomSplit(Array(trainRate, testRate, 1 - trainRate - testRate), new Date().getTime)
    ulog.unpersist()


    val tnum = train.count().toDouble
    val pnum = train.filter(_.getAs[Int]("label") > 0).count().toDouble
    val nnum = tnum - pnum

    //保证训练数据正负比例 1:9
    val rate = (pnum * 9 / nnum * 1000).toInt
    println("total positive negative", tnum, pnum, nnum, rate)
    trainLog :+= "train size total=%.0f positive=%.0f negative=%.0f scaleRate=%d/1000".format(tnum, pnum, nnum, rate)

    val sampleTrain = formatSample(spark, parser, train.filter(x => x.getAs[Int]("label") > 0 || Random.nextInt(1000) < rate))
    val sampleTest = formatSample(spark, parser, test)

    println(sampleTrain.take(5).foreach(x => println(x.features)))
    model.run(sampleTrain, 200, 1e-8)
    model.test(sampleTest)

    model.printLrTestLog()
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
    val mlfilepath = "/data/cpc/anal/model/lrmodel-%s-%s.mlm".format(name, date)
    model.saveHdfs("/user/cpc/lrmodel/lrmodeldata/%s".format(date))
    model.saveIrHdfs("/user/cpc/lrmodel/irmodeldata/%s".format(date))
    model.savePbPack(parser, lrfilepath, dict.toMap, dictStr.toMap)
    model.savePbPack2(parser, mlfilepath, dict.toMap, dictStr.toMap)

    trainLog :+= "protobuf pack %s".format(lrfilepath)

    trainLog :+= "\n-------update server data------"
    if (destfile.length > 0) {
      trainLog :+= MUtils.updateOnlineData(lrfilepath, destfile, ConfigFactory.load())
      MUtils.updateMlcppOnlineData(mlfilepath, "/home/work/mlcpp/data/" + destfile, ConfigFactory.load())
    }
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
                case "parser1" =>
                  getVectorParser1(u)
                case "parser2" =>
                  getVectorParser2(u)
                case "parser3" =>
                  getVectorParser3(u)
                case "ctrparser2" =>
                  getCtrVectorParser2(u)
                case "ctrparser3" =>
                  getCtrVectorParser3(u)
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

  def initFeatureDict(spark: SparkSession, pathSep: mutable.Map[String, Seq[String]]): Unit = {

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


  def getData(spark: SparkSession, dataVersion: String, pathSep: mutable.Map[String, Seq[String]]): DataFrame = {
    trainLog :+= "\n-------get ulog data------"

    var path = Seq[String]()
    pathSep.map {
      x =>
        path = path :+ "/user/cpc/lrmodel/%s/%s/{%s}".format(dataVersion, x._1, x._2.mkString(","))
    }

    path.foreach {
      x =>
        trainLog :+= x
        println(x)
    }

    spark.read.parquet(path: _*).coalesce(600)
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


  def getVectorParser1(x: Row): Vector = {

    val cal = Calendar.getInstance()
    cal.setTimeInMillis(x.getAs[Int]("timestamp") * 1000L)
    val week = cal.get(Calendar.DAY_OF_WEEK) //1 to 7
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
    //els = els :+ (x.getAs[Int]("isp") + i, 1d)
    //i += 20

    //net
    els = els :+ (x.getAs[Int]("network") + i, 1d)
    i += 10

    els = els :+ (dict("cityid").getOrElse(x.getAs[Int]("city"), 0) + i, 1d)
    i += dict("cityid").size + 1

    //media id
    els = els :+ (dict("mediaid").getOrElse(x.getAs[String]("media_appsid").toInt, 0) + i, 1d)
    i += dict("mediaid").size + 1

    //ad slot id
    els = els :+ (dict("slotid").getOrElse(x.getAs[String]("adslotid").toInt, 0) + i, 1d)
    i += dict("slotid").size + 1

    //0 to 4
    els = els :+ (x.getAs[Int]("phone_level") + i, 1d)
    i += 10

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

    try {
      Vectors.sparse(i, els)
    } catch {
      case e: Exception =>
        throw new Exception(els.toString + " " + i.toString + " " + e.getMessage)
        null
    }
  }

  def getVectorParser2(x: Row): Vector = {

    val cal = Calendar.getInstance()
    cal.setTimeInMillis(x.getAs[Int]("timestamp") * 1000L)
    val week = cal.get(Calendar.DAY_OF_WEEK) //1 to 7
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

    //media id
    els = els :+ (dict("mediaid").getOrElse(x.getAs[String]("media_appsid").toInt, 0) + i, 1d)
    i += dict("mediaid").size + 1

    //ad slot id
    els = els :+ (dict("slotid").getOrElse(x.getAs[String]("adslotid").toInt, 0) + i, 1d)
    i += dict("slotid").size + 1

    //0 to 4
    els = els :+ (x.getAs[Int]("phone_level") + i, 1d)
    i += 10

    //pagenum
    var pnum = x.getAs[Int]("pagenum")
    if (pnum < 0 || pnum > 50) {
      pnum = 0
    }
    els = els :+ (pnum + i, 1d)
    i += 100

    //bookid
    var bid = 0
    try {
      bid = x.getAs[String]("bookid").toInt
    } catch {
      case e: Exception =>
    }
    if (bid < 0 || bid > 50) {
      bid = 0
    }
    els = els :+ (bid + i, 1d)
    i += 100

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
    var uran_idx = 0
    val uran = x.getAs[Int]("user_req_ad_num")
    if (uran >= 1 && uran <= 10) {
      uran_idx = uran
    }
    if (uran > 10) {
      uran_idx = 11
    }
    els = els :+ (uran_idx + i, 1d)
    i += 12 + 1

    //user_req_num
    var urn_idx = 0
    val urn = x.getAs[Int]("user_req_num")
    if (urn >= 1 && urn <= 10) {
      urn_idx = 1
    } else if (urn > 10 && urn <= 100) {
      urn_idx = 2
    } else if (urn > 100 && urn <= 1000) {
      urn_idx = 3
    } else if (urn > 1000) {
      urn_idx = 4
    }
    els = els :+ (urn_idx + i, 1d)
    i += 5 + 1

    try {
      Vectors.sparse(i, els)
    } catch {
      case e: Exception =>
        throw new Exception(els.toString + " " + i.toString + " " + e.getMessage)
        null
    }
  }


  def getVectorParser3(x: Row): Vector = {

    val cal = Calendar.getInstance()
    cal.setTimeInMillis(x.getAs[Int]("timestamp") * 1000L)
    val week = cal.get(Calendar.DAY_OF_WEEK) //1 to 7
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
    i += 9

    //4
    els = els :+ (x.getAs[Int]("age") + i, 1d)
    i += 100

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
    els = els :+ (dict("mediaid").getOrElse(x.getAs[String]("media_appsid").toInt, 0) + i, 1d)
    i += dict("mediaid").size + 1

    //10
    els = els :+ (dict("slotid").getOrElse(x.getAs[String]("adslotid").toInt, 0) + i, 1d)
    i += dict("slotid").size + 1

    //11
    els = els :+ (x.getAs[Int]("phone_level") + i, 1d)
    i += 10

    //12
    var pnum = x.getAs[Int]("pagenum")
    if (pnum < 0 || pnum > 50) {
      pnum = 0
    }
    els = els :+ (pnum + i, 1d)
    i += 100

    //13
    var bid = 0
    try {
      bid = x.getAs[String]("bookid").toInt
    } catch {
      case e: Exception =>
    }
    if (bid < 0 || bid > 50) {
      bid = 0
    }
    els = els :+ (bid + i, 1d)
    i += 100

    //14
    val adcls = dict("adclass").getOrElse(x.getAs[Int]("adclass"), 0)
    els = els :+ (adcls + i, 1d)
    i += dict("adclass").size + 1

    //15
    els = els :+ (x.getAs[Int]("adtype") + i, 1d)
    i += 10

    //16
    els = els :+ (x.getAs[Int]("adslot_type") + i, 1d)
    i += 10

    //17
    els = els :+ (dict("planid").getOrElse(x.getAs[Int]("planid"), 0) + i, 1d)
    i += dict("planid").size + 1

    //18
    els = els :+ (dict("unitid").getOrElse(x.getAs[Int]("unitid"), 0) + i, 1d)
    i += dict("unitid").size + 1

    //19
    els = els :+ (dict("ideaid").getOrElse(x.getAs[Int]("ideaid"), 0) + i, 1d)
    i += dict("ideaid").size + 1

    //20
    var uran_idx = 0
    val uran = x.getAs[Int]("user_req_ad_num")
    if (uran >= 1 && uran <= 10) {
      uran_idx = uran
    }
    if (uran > 10) {
      uran_idx = 11
    }
    els = els :+ (uran_idx + i, 1d)
    i += 12 + 1

    //21
    var urn_idx = 0
    val urn = x.getAs[Int]("user_req_num")
    if (urn >= 1 && urn <= 10) {
      urn_idx = 1
    } else if (urn > 10 && urn <= 100) {
      urn_idx = 2
    } else if (urn > 100 && urn <= 1000) {
      urn_idx = 3
    } else if (urn > 1000) {
      urn_idx = 4
    }
    els = els :+ (urn_idx + i, 1d)
    i += 5 + 1

    //22
    if (x.getAs[Int]("sex") > 0 && x.getAs[Int]("age") > 0) {
      els = els :+ (6 * (x.getAs[Int]("sex") - 1) + x.getAs[Int]("age") + i, 1d)
    }
    i += 2 * 6 + 1

    //23
    val appIdx = x.getAs[WrappedArray[Int]]("appIdx")
    if (appIdx != null) {
      val inxList = appIdx.map(p => (p + i, 1d))
      els = els ++ inxList
    }
    i += 1000 + 1

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
    val week = cal.get(Calendar.DAY_OF_WEEK) //1 to 7
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

    //media id
    els = els :+ (dict("mediaid").getOrElse(x.getAs[String]("media_appsid").toInt, 0) + i, 1d)
    i += dict("mediaid").size + 1

    //ad slot id
    els = els :+ (dict("slotid").getOrElse(x.getAs[String]("adslotid").toInt, 0) + i, 1d)
    i += dict("slotid").size + 1

    //0 to 4
    els = els :+ (x.getAs[Int]("phone_level") + i, 1d)
    i += 10

    //pagenum
    var pnum = x.getAs[Int]("pagenum")
    if (pnum < 0 || pnum > 50) {
      pnum = 0
    }
    els = els :+ (pnum + i, 1d)
    i += 100

    //bookid
    var bid = 0
    try {
      bid = x.getAs[String]("bookid").toInt
    } catch {
      case e: Exception =>
    }
    if (bid < 0 || bid > 50) {
      bid = 0
    }
    els = els :+ (bid + i, 1d)
    i += 100

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
    var uran_idx = 0
    val uran = x.getAs[Int]("user_req_ad_num")
    if (uran >= 1 && uran <= 10) {
      uran_idx = uran
    }
    if (uran > 10) {
      uran_idx = 11
    }
    els = els :+ (uran_idx + i, 1d)
    i += 12 + 1

    //user_req_num
    var urn_idx = 0
    val urn = x.getAs[Int]("user_req_num")
    if (urn >= 1 && urn <= 10) {
      urn_idx = 1
    } else if (urn > 10 && urn <= 100) {
      urn_idx = 2
    } else if (urn > 100 && urn <= 1000) {
      urn_idx = 3
    } else if (urn > 1000) {
      urn_idx = 4
    }
    els = els :+ (urn_idx + i, 1d)
    i += 5 + 1

    var user_click = x.getAs[Int]("user_click_num")
    if (user_click >= 5) {
      user_click = 5
    }
    els = els :+ (user_click + i, 1d)
    i += 6

    var user_click_unit = x.getAs[Int]("user_click_unit_num")
    if (user_click_unit >= 5) {
      user_click_unit = 5
    }
    els = els :+ (user_click + i, 1d)
    i += 6


    try {
      Vectors.sparse(i, els)
    } catch {
      case e: Exception =>
        throw new Exception(els.toString + " " + i.toString + " " + e.getMessage)
        null
    }
  }

  def getCtrVectorParser3(x: Row): Vector = {

    val cal = Calendar.getInstance()
    cal.setTimeInMillis(x.getAs[Int]("timestamp") * 1000L)
    val week = cal.get(Calendar.DAY_OF_WEEK) //1 to 7
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

    //media id
    els = els :+ (dict("mediaid").getOrElse(x.getAs[String]("media_appsid").toInt, 0) + i, 1d)
    i += dict("mediaid").size + 1

    //ad slot id
    els = els :+ (dict("slotid").getOrElse(x.getAs[String]("adslotid").toInt, 0) + i, 1d)
    i += dict("slotid").size + 1

    //0 to 4
    els = els :+ (x.getAs[Int]("phone_level") + i, 1d)
    i += 10

    //pagenum
    var pnum = x.getAs[Int]("pagenum")
    if (pnum < 0 || pnum > 50) {
      pnum = 0
    }
    els = els :+ (pnum + i, 1d)
    i += 100

    //bookid
    var bid = 0
    try {
      bid = x.getAs[String]("bookid").toInt
    } catch {
      case e: Exception =>
    }
    if (bid < 0 || bid > 50) {
      bid = 0
    }
    els = els :+ (bid + i, 1d)
    i += 100

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
    var uran_idx = 0
    val uran = x.getAs[Int]("user_req_ad_num")
    if (uran >= 1 && uran <= 10) {
      uran_idx = uran
    }
    if (uran > 10) {
      uran_idx = 11
    }
    els = els :+ (uran_idx + i, 1d)
    i += 12 + 1

    //user_req_num
    var urn_idx = 0
    val urn = x.getAs[Int]("user_req_num")
    if (urn >= 1 && urn <= 10) {
      urn_idx = 1
    } else if (urn > 10 && urn <= 100) {
      urn_idx = 2
    } else if (urn > 100 && urn <= 1000) {
      urn_idx = 3
    } else if (urn > 1000) {
      urn_idx = 4
    }
    els = els :+ (urn_idx + i, 1d)
    i += 5 + 1

    var user_click = x.getAs[Int]("user_click_num")
    if (user_click >= 5) {
      user_click = 5
    }
    els = els :+ (user_click + i, 1d)
    i += 6

    var user_click_unit = x.getAs[Int]("user_click_unit_num")
    if (user_click_unit >= 5) {
      user_click_unit = 5
    }
    els = els :+ (user_click + i, 1d)
    i += 6

    //sex - age
    if (x.getAs[Int]("sex") > 0 && x.getAs[Int]("age") > 0) {
      els = els :+ (6 * (x.getAs[Int]("sex") - 1) + x.getAs[Int]("age") + i, 1d)
    }
    i += 2 * 6 + 1

    //user installed app
    val appIdx = x.getAs[WrappedArray[Int]]("appIdx")
    if (appIdx != null) {
      val inxList = appIdx.map(p => (p + i, 1d))
      els = els ++ inxList
    }
    i += 1000 + 1

    try {
      Vectors.sparse(i, els)
    } catch {
      case e: Exception =>
        throw new Exception(els.toString + " " + i.toString + " " + e.getMessage)
        null
    }
  }

  def evaluate(spark: SparkSession, dataType: String, mlmfile: String): Unit = {
    val cal = Calendar.getInstance()
    cal.add(Calendar.HOUR, -4)
    val date = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime)
    val part = new SimpleDateFormat("yyyy-MM-dd/HH").format(cal.getTime)

    val mlm = Pack.parseFrom(new FileInputStream(mlmfile))
    val dictData = mlm
    dict.update("mediaid", dictData.mediaid)
    dict.update("planid", dictData.planid)
    dict.update("ideaid", dictData.ideaid)
    dict.update("unitid", dictData.unitid)
    dict.update("slotid", dictData.slotid)
    dict.update("adclass", dictData.adclass)
    dict.update("cityid", dictData.cityid)

    import spark.implicits._
    val uidApp = spark.read.parquet("/user/cpc/userInstalledApp/%s/*".format(date)).rdd
      .map(x => (x.getAs[String]("uid"), x.getAs[mutable.WrappedArray[String]]("pkgs")))
      .reduceByKey(_ ++ _)
      .map(x => (x._1, x._2.distinct))
      .toDF("uid", "pkgs").rdd
    val appids = dictData.appid
    val userAppids = getUserAppIdx(spark, uidApp, appids)

    var qtt = spark.read.parquet("/user/cpc/lrmodel/ctrdata_v1/%s/*".format(date)).coalesce(200)
    if (dataType == "qtt-list") {
      qtt = qtt.filter {
        x =>
          Seq("80000001", "80000002").contains(x.getAs[String]("media_appsid")) &&
            x.getAs[Int]("adslot_type") == 1 && x.getAs[Int]("ideaid") > 0
      }
    } else if (dataType == "qtt-content") {
      qtt = qtt.filter {
        x =>
          Seq("80000001", "80000002").contains(x.getAs[String]("media_appsid")) &&
            x.getAs[Int]("adslot_type") == 2 && x.getAs[Int]("ideaid") > 0
      }
    } else if (dataType == "qtt-all") {
      qtt = qtt.filter {
        x =>
          Seq("80000001", "80000002").contains(x.getAs[String]("media_appsid")) &&
            Seq(1, 2).contains(x.getAs[Int]("adslot_type")) && x.getAs[Int]("ideaid") > 0
      }
    } else {
      qtt = qtt.filter { x => x.getAs[Int]("ideaid") > 0 }
    }
    qtt = getLimitedData(spark, 1e7, qtt).join(userAppids, Seq("uid"), "leftouter").cache()

    val lr = LogisticRegressionModel.load(spark.sparkContext, "/user/cpc/lrmodel/lrmodeldata/2018-04-09-03-37")
    val BcDict = spark.sparkContext.broadcast(dict)
    val BcWeights = spark.sparkContext.broadcast(lr.weights)
    val lrtest = qtt.rdd
      .mapPartitions {
        p =>
          dict = BcDict.value
          p.map {
            u =>
              val vec = getCtrVectorParser3(u)
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

  }

  def getLimitedData(spark: SparkSession, limitedNum: Double, ulog: DataFrame): DataFrame = {
    import spark.implicits._
    var rate = 1d
    val num = ulog.count().toDouble

    if (num > limitedNum) {
      rate = limitedNum / num
    }

    ulog.randomSplit(Array(rate, 1 - rate), new Date().getTime)(0)
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
    lrTestResults //(p, label)
      .map {
      x =>
        val v = (x._1 * 100).toInt / 5
        ((v, x._2.toInt), 1)
    } //  ((预测值,lable),1)
      .reduceByKey((x, y) => x + y) //  ((预测值,lable),num)
      .map {
      x =>
        val key = x._1._1
        val label = x._1._2
        if (label == 0) {
          (key, (x._2, 0)) //  (预测值,(num1,0))
        } else {
          (key, (0, x._2)) //  (预测值,(0,num2))
        }
    }
      .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2)) //  (预测值,(num1反,num2正))
      .sortByKey(false)
      .toLocalIterator
      .foreach {
        x =>
          val sum = x._2
          val pre = x._1.toDouble * 0.05
          if (pre > 0.2) {
            //isUpdateModel = true
          }
          log = log + "%.2f %d %.4f %.4f %d %.4f %.4f %.4f\n".format(
            pre, //预测值
            sum._2, //正例数
            sum._2.toDouble / test1.toDouble, //该准确率下的正例数/总正例数
            sum._2.toDouble / testSum.toDouble, //该准确率下的正例数/总数
            sum._1, //反例数
            sum._1.toDouble / test0.toDouble, //该准确率下的反例数/总反例数
            sum._1.toDouble / testSum.toDouble, //该准确率下的反例数/总数
            sum._2.toDouble / (sum._1 + sum._2).toDouble) // 真实值
      }

    println(log)
    trainLog :+= log
  }
}

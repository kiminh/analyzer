package com.cpc.spark.ml.ctrmodel.v1

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import com.cpc.spark.common.Utils
import com.cpc.spark.ml.common.{Utils => MUtils}
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import com.cpc.spark.ml.train.LRIRModel
import com.typesafe.config.ConfigFactory
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SaveMode, SparkSession}

import scala.collection.mutable
import mlserver.mlserver._
import org.apache.spark.mllib.regression.LabeledPoint

import scala.util.Random

/**
  * Created by roydong on 15/12/2017.
  */
object LRTrain {

  private var days = 7
  private var dayBefore = 7
  private var trainLog = Seq[String]()

  private var spark: SparkSession = _
  private var model: LRIRModel = _

  def main(args: Array[String]): Unit = {
    if (args.length > 0) {
      dayBefore = args(0).toInt
      days = args(1).toInt
    }
    model = new LRIRModel
    spark = model.initSpark("ctr lr model")
    initFeatureDict()
    val ulog = getData().cache()

    //qtt-all
    val qtt = ulog.filter(x => x.getString(7) == "80000001" || x.getString(7) == "80000002")
    train("parser1", "qtt-all", qtt, "qtt-all.lrm")

    //qtt-list
    model.clearResult()
    val qttlist = ulog.filter(x => (x.getString(7) == "80000001" || x.getString(7) == "80000002") && x.getInt(16) == 1)
    train("parser1", "qtt-list", qttlist, "qtt-list.lrm")

    //qtt-content
    model.clearResult()
    val qttcontent = ulog.filter(x => (x.getString(7) == "80000001" || x.getString(7) == "80000002") && x.getInt(16) == 2)
    train("parser1", "qtt-content", qttcontent, "qtt-content.lrm")

    //external-list
    model.clearResult()
    val externallist = ulog.filter(x => (x.getString(7) != "80000001" && x.getString(7) != "80000002") && x.getInt(16) == 1)
    train("parser1", "external-list", externallist, "external-list.lrm")

    //external-content
    model.clearResult()
    val externalcontent = ulog.filter(x => (x.getString(7) != "80000001" && x.getString(7) != "80000002") && x.getInt(16) == 2)
    train("parser1", "external-content", externalcontent, "external-content.lrm")

    //all-interact
    model.clearResult()
    val allinteract = ulog.filter(x => x.getInt(16) == 3)
    train("parser1", "all-interact", allinteract, "all-interact.lrm")

    //all-media
    val allmedia = ulog
    train("parser1", "all-media", allmedia, "all-media.lrm")

    //TODO

    Utils.sendMail(trainLog.mkString("\n"), "TrainLog", Seq("rd@aiclk.com"))
    ulog.unpersist()
  }

  def train(parser: String, name: String, ulog: RDD[Row], destfile: String): Unit = {
    trainLog :+= "\n------train log--------"
    trainLog :+= "name=%s parser=%s".format(name, parser)

    val num = ulog.count().toDouble
    println("sample num", num)
    trainLog :+= "total size %.0f".format(num)

    //最多2000w条测试数据
    var testRate = 0.1
    if (num * testRate > 2e7) {
      testRate = 2e7 / num
    }

    val Array(train, test) = ulog.randomSplit(Array(1 - testRate, testRate), new Date().getTime)
    ulog.unpersist()

    val tnum = train.count().toDouble
    val pnum = train.filter(_.getInt(0) > 0).count().toDouble
    val nnum = tnum - pnum

    //保证训练数据正负比例 1:9
    val rate = (pnum * 9 / nnum * 1000).toInt
    println("total positive negative", tnum, pnum, nnum, rate)
    trainLog :+= "train size total=%.0f positive=%.0f negative=%.0f scaleRate=%d/1000".format(tnum, pnum, nnum, rate)

    val sampleTrain = formatSample(parser, train.filter(x => x.getInt(0) > 0 || Random.nextInt(1000) < rate))
    val sampleTest = formatSample(parser, test)

    println(sampleTrain.take(5).foreach(x => println(x.features)))
    model.run(sampleTrain, 200, 1e-8)
    model.test(sampleTest)
    model.printLrTestLog()
    trainLog :+= model.getLrTestLog()

    model.runIr(1000, 0.9)
    trainLog :+= model.binsLog.mkString("\n")

    val date = new SimpleDateFormat("yyyy-MM-dd-HH-mm").format(new Date().getTime)
    val lrfilepath = "/data/cpc/anal/model/ctr-model-%s-%s.lrm".format(name, date)

    model.saveHdfs("/user/cpc/ctrmodel/lrmodel/%s".format(date))
    model.savePbPack(parser, lrfilepath, dict.toMap)
    trainLog :+= "protobuf pack %s".format(lrfilepath)

    trainLog :+= "\n-------update server data------"
    if (destfile.length > 0) {
      trainLog :+= MUtils.updateOnlineData(lrfilepath, destfile, ConfigFactory.load())
    }
  }

  def formatSample(parser: String, ulog: RDD[Row]): RDD[LabeledPoint] = {
    val BcDict = spark.sparkContext.broadcast(dict)

    ulog
      .mapPartitions {
        p =>
          dict = BcDict.value
          p.map {
            u =>
              val vec = parser match {
                case "parser1" =>
                  getVector(u)
              }
              LabeledPoint(u.getInt(0).toDouble, vec)
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

  def initFeatureDict(): Unit = {
    val calendar = Calendar.getInstance()
    calendar.add(Calendar.DATE, -dayBefore)
    var pathSeps = Seq[String]()
    for (d <- 1 to days) {
      val date = new SimpleDateFormat("yyyy-MM-dd").format(calendar.getTime)
      pathSeps = pathSeps :+ date
      calendar.add(Calendar.DATE, 1)
    }
    trainLog :+= "\n------dict size------"
    for (name <- dictNames) {
      val pathTpl = "/user/cpc/feature_ids/%s/{%s}"
      var n = 0
      val ids = mutable.Map[Int, Int]()
      spark.read
        .parquet(pathTpl.format(name, pathSeps.mkString(",")))
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

  def getData(): RDD[Row] = {
    trainLog :+= "\n-------get ulog data------"
    val calendar = Calendar.getInstance()
    calendar.add(Calendar.DATE, -dayBefore)
    var pathSeps = Seq[String]()
    for (d <- 1 to days) {
      val date = new SimpleDateFormat("yyyy-MM-dd").format(calendar.getTime)
      pathSeps = pathSeps :+ date
      calendar.add(Calendar.DATE, 1)
    }

    val path = "/user/cpc/lrmodel/features/{%s}".format(pathSeps.mkString(","))
    println(path)
    trainLog :+= path
    spark.read.parquet(path).rdd.coalesce(2000)
  }

  /*
  def parseFeature(row: Row): Vector = {
    val (ad, m, slot, u, loc, n, d, t) = unionLogToObject(row)
    var svm = ""
    getVector(ad, m, slot, u, loc, n, d, t)
  }
  */

  def unionLogToObject(x: Row): (AdInfo, Media, AdSlot, User, Location, Network, Device, Long) = {
    val ad = AdInfo(
      ideaid = x.getInt(13),
      unitid = x.getInt(12),
      planid = x.getInt(11),
      adtype = x.getInt(10),
      _class = x.getInt(14)
    )
    val m = Media(
      mediaAppsid = x.getString(7).toInt
    )
    val slot = AdSlot(
      adslotid = x.getString(15).toInt,
      adslotType = x.getInt(16),
      pageNum = x.getInt(17),
      bookId = x.getString(18)
    )
    val u = User(
      sex = x.getInt(1),
      age = x.getInt(2)
    )
    val n = Network(
      network = x.getInt(5),
      isp = x.getInt(4)
    )
    val loc = Location(
      city = x.getInt(6)
    )
    val d = Device(
      os = x.getInt(3),
      phoneLevel = x.getInt(8)
    )
    (ad, m, slot, u, loc, n, d, x.getInt(9) * 1000L)
  }

  def getVector(x: Row): Vector = {

    val cal = Calendar.getInstance()
    cal.setTimeInMillis(x.getInt(9) * 1000L)
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
    els = els :+ (x.getInt(1) + i, 1d)
    i += 9

    //age
    els = els :+ (x.getInt(2) + i, 1d)
    i += 100

    //os 96 - 97 (2)
    els = els :+ (x.getInt(3) + i, 1d)
    i += 10

    //isp
    els = els :+ (x.getInt(4) + i, 1d)
    i += 20

    //net
    els = els :+ (x.getInt(5) + i, 1d)
    i += 10

    els = els :+ (dict("cityid").getOrElse(x.getInt(6), 0) + i, 1d)
    i += dict("cityid").size + 1

    //media id
    els = els :+ (dict("mediaid").getOrElse(x.getString(7).toInt, 0) + i, 1d)
    i += dict("mediaid").size + 1

    //ad slot id
    els = els :+ (dict("slotid").getOrElse(x.getString(15).toInt, 0) + i, 1d)
    i += dict("slotid").size + 1

    //0 to 4
    els = els :+ (x.getInt(8) + i, 1d)
    i += 10

    //pagenum
    var pnum = x.getInt(17)
    if (pnum < 0 || pnum > 50) {
      pnum = 0
    }
    els = els :+ (pnum + i, 1d)
    i += 100

    //bookid
    var bid = 0
    try {
      bid = x.getString(18).toInt
    } catch {
      case e: Exception =>
    }
    if (bid < 0 || bid > 50) {
      bid = 0
    }
    els = els :+ (bid + i, 1d)
    i += 100

    //ad class
    val adcls = dict("adclass").getOrElse(x.getInt(14), 0)
    els = els :+ (adcls + i, 1d)
    i += dict("adclass").size + 1

    //adtype
    els = els :+ (x.getInt(10) + i, 1d)
    i += 10

    //planid
    els = els :+ (dict("planid").getOrElse(x.getInt(11), 0) + i, 1d)
    i += dict("planid").size + 1

    //unitid
    els = els :+ (dict("unitid").getOrElse(x.getInt(12), 0) + i, 1d)
    i += dict("unitid").size + 1

    //ideaid
    els = els :+ (dict("ideaid").getOrElse(x.getInt(13), 0) + i, 1d)
    i += dict("ideaid").size + 1

    try {
      Vectors.sparse(i, els)
    } catch {
      case e: Exception =>
        throw new Exception(els.toString + " " + i.toString + " " + e.getMessage)
        null
    }
  }
}



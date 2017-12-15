package com.cpc.spark.ml.ctrmodel.v1

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import org.apache.spark.mllib.linalg.{Vector, Vectors}
import com.cpc.spark.log.parser.{ExtValue, UnionLog}
import com.cpc.spark.ml.train.LRIRModel
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.mutable
import mlserver.mlserver._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.mllib.regression.LabeledPoint

import scala.util.Random

/**
  * Created by roydong on 15/12/2017.
  */
object LRTrain {

  def main(args: Array[String]): Unit = {
    val model = new LRIRModel
    val spark = model.initSpark("ctr lr model")

    initFeatureDict(spark)

    val ulog = getUnionLog(spark)
    val num = ulog.count().toDouble
    println("sample num", num)

    //最多2000w条测试数据
    var testRate = 0.1
    if (num * testRate > 2e7) {
        testRate = 2e7 / num
    }

    val Array(train, test) = ulog.randomSplit(Array(1 - testRate, testRate), 1231245L)

    val tnum = train.count().toDouble
    val pnum = train.filter(_.isclick > 0).count().toDouble
    val nnum = tnum - pnum

    val rate = (pnum * 10 / nnum * 1000).toInt
    println("total positive negative", tnum, pnum, nnum, rate)

    val sampleTrain = formatSample(spark, train.filter(x => x.isclick > 0 || Random.nextInt(1000) < rate))
    val sampleTest = formatSample(spark, test)

    model.run(sampleTrain, 200, 1e-8)
    model.test(sampleTest)
    model.printLrTestLog()

    val date = new SimpleDateFormat("yyyy-MM-dd-HH-mm").format(new Date().getTime)
    model.saveHdfs("/user/cpc/ctrmodel/lrmodel/%s".format(date))
    val lrfilepath = "/data/cpc/anal/model/ctr-lr-model-%s.txt".format(date)
    model.saveText(lrfilepath)
  }

  def formatSample(spark: SparkSession, ulog: RDD[UnionLog]): RDD[LabeledPoint] = {
    val BcDict = spark.sparkContext.broadcast(dict)

    ulog
      .mapPartitions {
        p =>
          dict = BcDict.value
          p.map {
            u =>
              val vec = parseFeature(u)
              LabeledPoint(u.isclick.toDouble, vec)
          }
      }
  }

  var dict = mutable.Map[String, Map[Int, Int]]()
  val dictNames = Seq("planid", "unitid", "ideaid", "slotid", "adclass", "city")

  def initFeatureDict(spark: SparkSession): Unit = {
    val calendar = Calendar.getInstance()
    var pathSeps = Seq[String]()
    for (d <- 1 to 7) {
      calendar.add(Calendar.DATE, -1)
      val date = new SimpleDateFormat("yyyy-MM-dd").format(calendar.getTime)
      pathSeps = pathSeps :+ date
    }

    for (name <- dictNames) {
      val pathTpl = "/user/cpc/feature_ids/%s/{%s}"
      var n = 0
      val ids = mutable.Map[Int, Int]()
      spark.read
        .parquet(pathTpl.format(name, pathSeps.mkString(",")))
        .rdd
        .map(x => x.getInt(0))
        .distinct()
        .toLocalIterator
        .foreach {
          id =>
            n += 1
            ids.update(id, n)
        }
      println("dict", name, ids.size)
      dict.update(name, ids.toMap)
    }
  }


  def getUnionLog(spark: SparkSession): RDD[UnionLog] = {
    val calendar = Calendar.getInstance()
    val endDate = new SimpleDateFormat("yyyy-MM-dd").format(calendar.getTime)
    calendar.add(Calendar.DATE, -1)
    val startDate = new SimpleDateFormat("yyyy-MM-dd").format(calendar.getTime)
    val stmt =
      """
        |select * from dl_cpc.cpc_union_log where `date` >= "%s" and `date` < "%s"
      """.stripMargin.format(startDate, endDate)

    import spark.implicits._
    spark.sql(stmt).as[UnionLog].rdd
  }

  def parseFeature(ulog: UnionLog): Vector = {
    val (ad, m, slot, u, loc, n, d, t) = unionLogToObject(ulog)
    var svm = ""
    getVector(ad, m, slot, u, loc, n, d, ulog.timestamp * 1000L)
  }


  def unionLogToObject(x: UnionLog): (AdInfo, Media, AdSlot, User, Location, Network, Device, Long) = {
    var cls = 0
    var pagenum = 0
    var bookid = ""

    if (x.ext != null) {
      val ac = x.ext.getOrElse("adclass", null)
      if (ac != null) {
        cls = ac.int_value
      }

      val pn = x.ext.getOrElse("pagenum", null)
      if (pn != null) {
        pagenum = pn.int_value
      }

      val bi = x.ext.getOrElse("bookid", null)
      if (bi != null) {
        bookid = bi.string_value
      }
    }

    val ad = AdInfo(
      bid = x.bid,
      ideaid = x.ideaid,
      unitid = x.unitid,
      planid = x.planid,
      userid = x.userid,
      adtype = x.adtype,
      interaction = x.interaction,
      _class = cls
    )
    val m = Media(
      mediaAppsid = x.media_appsid.toInt,
      mediaType = x.media_type
    )
    val slot = AdSlot(
      adslotid = x.adslotid.toInt,
      adslotType = x.adslot_type,
      pageNum = pagenum,
      bookId = bookid
    )
    val interests = x.interests.split(",")
      .map{
        x =>
          val v = x.split("=")
          if (v.length == 2) {
            (v(0).toInt, v(1).toInt)
          } else {
            (0, 0)
          }
      }
      .filter(x => x._1 > 0 && x._2 >= 2)
      .sortWith((x, y) => x._2 > y._2)
      .map(_._1)
      .toSeq
    val u = User(
      sex = x.sex,
      age = x.age,
      coin = x.coin,
      uid = x.uid,
      interests = interests
    )
    val n = Network(
      network = x.network,
      isp = x.isp,
      ip = x.ip
    )
    val loc = Location(
      country = x.country,
      province = x.province,
      city = x.city
    )
    val d = Device(
      os = x.os,
      model = x.model
    )
    (ad, m, slot, u, loc, n, d, x.timestamp * 1000L)
  }

  def getVector(ad: AdInfo, m: Media, slot: AdSlot, u: User, loc: Location,
                n: Network, d: Device, timeMills: Long): Vector = {

    val cal = Calendar.getInstance()
    cal.setTimeInMillis(timeMills)
    val week = cal.get(Calendar.DAY_OF_WEEK)   //1 to 7
    val hour = cal.get(Calendar.HOUR_OF_DAY)
    var els = Seq[(Int, Double)]()
    var i = 0

    els = els :+ (week + i - 1, 1d)
    i += 7

    //(24)
    els = els :+ (hour + i, 1d)
    i += 24

    els = els :+ (u.sex + i, 1d)
    i += 9

    els = els :+ (u.age + i, 1d)
    i += 100

    //os 96 - 97 (2)
    els = els :+ (d.os + i, 1d)
    i += 10

    els = els :+ (n.isp + i, 1d)
    i += 20

    els = els :+ (n.network + i, 1d)
    i += 10

    val cityid = dict("city").getOrElse(loc.city, 0)
    els = els :+ (cityid + i, 1d)
    i += 1000

    //ad slot id
    val sid = dict("slotid").getOrElse(slot.adslotid, 0)
    els = els :+ (sid + i, 1d)
    i += 1000

    //0 to 4
    els = els :+ (d.phoneLevel + i, 1d)
    i += 10

    //pagenum
    var pnum = 0
    if (slot.pageNum >= 1 && slot.pageNum <= 50){
      pnum = slot.pageNum
    }
    els = els :+ (pnum + i, 1d)
    i += 100

    //bookid
    var bid = 0
    try {
      bid = slot.bookId.toInt
    } catch {
      case e: Exception =>
    }
    if (bid < 0 || bid > 50) {
      bid = 0
    }
    els = els :+ (bid + i, 1d)
    i += 100

    //ad class
    val adcls = dict("adclass").getOrElse(ad._class, 0)
    els = els :+ (adcls + i, 1d)
    i += 1000

    //adtype
    els = els :+ (ad.adtype + i, 1d)
    i += 10

    //planid
    els = els :+ (dict("planid").getOrElse(ad.planid, 0) + i, 1d)
    i += 30000

    //unitid
    els = els :+ (dict("unitid").getOrElse(ad.unitid, 0) + i, 1d)
    i += 50000

    //ideaid
    els = els :+ (dict("ideaid").getOrElse(ad.ideaid, 0) + i, 1d)
    i += 200000

    try {
      Vectors.sparse(i, els)
    } catch {
      case e: Exception =>
        throw new Exception(els.toString + " " + i.toString + " " + e.getMessage)
        null
    }
  }
}



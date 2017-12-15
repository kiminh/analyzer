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
import org.apache.spark.mllib.regression.LabeledPoint

/**
  * Created by roydong on 15/12/2017.
  */
object LRTrain {

  def main(args: Array[String]): Unit = {
    val model = new LRIRModel
    val spark = model.initSpark("ctr lr model")

    val ulog = getUnionLog(spark)
    val sample = formatSample(spark, ulog)

    val num = sample.count().toDouble
    println("sample num", num)

    //最多2000w条测试数据
    var testRate = 0.1
    if (num * testRate > 2e7) {
        testRate = 2e7 / num
    }

    val Array(train, test) = sample.randomSplit(Array(1 - testRate, testRate), 1231245L)

    model.run(train, 200, 1e-8)
    model.test(test)
    model.printLrTestLog()

    val date = new SimpleDateFormat("yyyy-MM-dd-HH-mm").format(new Date().getTime)
    model.saveHdfs("/user/cpc/ctrmodel/lrmodel/%s".format(date))
    val lrfilepath = "/data/cpc/anal/model/ctr-lr-model-%s.txt".format(date)
    model.saveText(lrfilepath)
  }

  def formatSample(spark: SparkSession, ulog: RDD[UnionLog]): RDD[LabeledPoint] = {
    val BcPlanid = spark.sparkContext.broadcast(planid)
    val BcUnitid = spark.sparkContext.broadcast(unitid)
    val BcIdeaid = spark.sparkContext.broadcast(ideaid)
    val BcAdclass = spark.sparkContext.broadcast(adclass)
    val BcSlotid = spark.sparkContext.broadcast(slotid)
    val BcCity = spark.sparkContext.broadcast(city)

    ulog
      .mapPartitions {
        p =>
          planid = BcPlanid.value
          unitid = BcUnitid.value
          ideaid = BcIdeaid.value
          adclass = BcAdclass.value
          slotid = BcSlotid.value
          city = BcCity.value
          p.map {
            u =>
              val vec = parseFeature(u)
              LabeledPoint(u.isclick, vec)
          }
      }
  }

  var planid = mutable.Map[Int, Int]()
  var unitid = mutable.Map[Int, Int]()
  var ideaid = mutable.Map[Int, Int]()
  var adclass = mutable.Map[Int, Int]()
  var slotid = mutable.Map[Int, Int]()
  var city = mutable.Map[Int, Int]()

  def analFeatureDict(ulog: RDD[UnionLog]): Unit = {
    //planid
    var n = 0
    ulog.map(x => x.planid).distinct()
      .toLocalIterator
      .foreach {
        id =>
          planid.update(id, n)
          n += 1
      }
    println("planid num", n)

    //unitid
    n = 0
    ulog.map(x => x.unitid).distinct()
      .toLocalIterator
      .foreach {
        id =>
          unitid.update(id, n)
          n += 1
      }
    println("unitid num", n)

    //ideaid
    n = 0
    ulog.map(x => x.ideaid).distinct()
      .toLocalIterator
      .foreach {
        id =>
          ideaid.update(id, n)
          n += 1
      }
    println("ideaid num", n)

    //adclass
    n = 0
    ulog.map(x => x.ext.getOrElse("adclass", ExtValue()).int_value).distinct()
      .toLocalIterator
      .foreach {
        id =>
          ideaid.update(id, n)
          n += 1
      }
    println("adclass num", n)

    //slotid
    n = 0
    ulog.map(x => x.adslotid.toInt).distinct()
      .toLocalIterator
      .foreach {
        id =>
          slotid.update(id, n)
          n += 1
      }
    println("slotid num", n)

    //city
    n = 0
    ulog.map(x => x.city).distinct()
      .toLocalIterator
      .foreach {
        id =>
          city.update(id, n)
          n += 1
      }
    println("city num", n)
  }

  def getUnionLog(spark: SparkSession): RDD[UnionLog] = {
    val calendar = Calendar.getInstance()
    val endDate = new SimpleDateFormat("yyyy-MM-dd").format(calendar.getTime)
    calendar.add(Calendar.DATE, -7)
    val startDate = new SimpleDateFormat("yyyy-MM-dd").format(calendar.getTime)
    val stmt =
      """
        |select * from dl_cpc.cpc_union_log where `date` >= "%s" and `date` < "%s"
      """.stripMargin.format(startDate, endDate)

    import spark.implicits._
    spark.sql(stmt).as[UnionLog].rdd.cache()
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

    els = els :+ (u.age, 1d)
    i += 100

    //os 96 - 97 (2)
    els = els :+ (d.os + i, 1d)
    i += 10

    els = els :+ (n.isp + i, 1d)
    i += 20

    els = els :+ (n.network + i, 1d)
    i += 10

    val cityid = city.getOrElse(loc.city, 0)
    els = els :+ (cityid + i, 1d)
    i += 1000

    //ad slot id
    val sid = slotid.getOrElse(slot.adslotid, 0)
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
    els = els :+ (pnum + 1 + i, 1d)
    i += 100

    //bookid
    var bid = slot.bookId.toInt
    if (bid < 0 || bid > 50) {
      bid = 0
    }
    els = els :+ (bid + i, 1d)
    i += 100

    //ad class
    val adcls = adclass.getOrElse(ad._class, 0)
    els = els :+ (adcls + i, 1d)
    i += 1000

    //adtype
    els = els :+ (ad.adtype + i, 1d)
    i += 10

    //planid
    els = els :+ (planid.getOrElse(ad.planid,0 ) + i, 1d)
    i += 30000

    //unitid
    els = els :+ (planid.getOrElse(ad.planid,0 ) + i, 1d)
    i += 50000

    //ideaid
    els = els :+ (ideaid.getOrElse(ad.ideaid, 0) + i, 1d)
    i += 200000

    Vectors.sparse(i, els)
  }
}



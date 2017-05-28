package com.cpc.spark.ml.parser

import java.text.{ParseException, SimpleDateFormat}
import java.util.{Calendar, Date}

import com.cpc.spark.log.parser.UnionLog
import mlserver.mlserver._

import scala.util.Random
import org.apache.spark.mllib.linalg.{Vector, Vectors}

import scala.collection.mutable

/**
  * Created by Roy on 2017/5/15.
  */
object FeatureParser {

  def parseUnionLog(x: UnionLog): String = {
    // 随机 1/5 的负样本
    if (x.isclick == 1 || Random.nextInt(5) == 0) {
      val ad = AdInfo(
        bid = x.bid,
        ideaid = x.ideaid,
        unitid = x.unitid,
        planid = x.planid,
        userid = x.userid,
        adtype = x.adtype,
        interaction = x.interaction
      )
      val m = Media(
        mediaAppsid = x.media_appsid.toInt,
        mediaType = x.media_type,
        adslotid = x.adslotid.toInt,
        adslotType = x.adslot_type,
        floorbid = x.floorbid
      )
      val u = User(
        sex = x.sex,
        age = x.age,
        coin = x.coin
      )
      val n = Network(
        network = x.network,
        isp = x.isp
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

      val vector = parse(ad, m, u, loc, n, d, x.date, x.hour.toInt)
      var i = 1
      var svm = x.isclick.toString
      for (v <- vector.toArray) {
        svm = svm + " %d:%f".format(i, v)
        i += 1
      }
      svm
    } else {
      ""
    }
  }

  def parse(ad: AdInfo, m: Media, u: User, loc: Location, n: Network, d: Device, date: String, hour: Int): Vector = {
    //属性展开
    val props = Seq[Double](
      u.age,
      u.sex,
      u.coin,
      //pcategory
      //interests
      //x.country,
      loc.province,
      loc.city,

      n.isp,
      n.network,

      d.os,
      //os version,
      d.model.hashCode,
      //browser,
      m.mediaAppsid,
      m.mediaType,
      //x.mediaclass,
      //x.channel,
      m.adslotid,
      m.adslotType,
      //adstlotsize,
      m.floorbid,

      ad.adtype,
      ad.interaction,
      ad.userid,
      ad.planid,
      ad.unitid,
      ad.ideaid,
      ad.bid,
      //ad class,
      //x.usertype,
      dateToDouble(date),
      dateToWeek(date),
      hour
    )

    //以对象作为特征
    val objs = Seq[String](
      ad.toString,
      u.toString,
      m.toString,
      n.toString,
      d.toString,
      loc.toString
    )

    //obj特征的所有组合
    var objCombs = Seq[String]()
    for (m <- 2 to objs.length) {
      objCombs ++= getCombination(objs, m).map(_.mkString(" "))
    }

    Vectors.dense((props ++ (objs ++ objCombs).map(_.hashCode().toDouble)).toArray)
  }

  //得到所有排列组合 C(n, m)
  def getCombination(all: Seq[String], m: Int): Seq[Array[String]] = {
    var combs = mutable.Seq[Array[String]]()
    def mapCombination(all: Seq[String], m: Int, start: Int, idx: Int, comb: Array[String]): Unit = {
      if (m > 0) {
        for (i <- start to all.length - 1) {
          comb(idx) = all(i)
          mapCombination(all, m - 1, i + 1, idx + 1, comb)
        }
      } else {
        combs :+= comb
      }
    }
    val comb = new Array[String](m)
    mapCombination(all, m, 0, 0, comb)
    combs
  }

  def dateToDouble(date: String): Double = {
    try {
      date.replace("-", "").toDouble
    } catch {
      case e: Exception =>
        new SimpleDateFormat("yyyyMMdd").format(new Date().getTime).toDouble
    }
  }

  def dateToWeek(date: String): Double = {
    val cal = Calendar.getInstance()
    if (date.length > 0) {
      new SimpleDateFormat("yyyy-MM-dd").parse(date)
    }
    cal.get(Calendar.DAY_OF_WEEK).toDouble
  }


  //model v2
  val min = Vectors.dense(Array(-2.14748336E9,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,-2.147047769E9,
    8.0000001E7,0.0,1018976.0,1.0,1.0,3.0,1.0,18.0,50.0,52.0,91.0,15.0,2.0170512E7,0.0))
  val max = Vectors.dense(Array(2.147482965E9,6.0,2.0,1.111188234E9,34.0,131100.0,18.0,4.0,2.0,2.147228562E9,
    8.0000005E7,0.0,1026459.0,2.0,500.0,4.0,1.0,316.0,694.0,1335.0,3216.0,300.0,2.0170522E7,23.0))

  def normalize(min: Vector, max: Vector, row: Vector): Vector = {
    var els = Seq[(Int, Double)]()
    row.foreachActive {
      (i, v) =>
        var rate = 0.5D
        if (max(i) > min(i)) {
          if (v < min(i)) {
            rate = 1e-6
          } else if (v > max(i)) {
            rate = 1 - 1e-6
          } else {
            rate = (v - min(i)) / (max(i) - min(i))
          }
        }
        els = els :+ (i, rate)
    }
    Vectors.sparse(row.size, els)
  }
}


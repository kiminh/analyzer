package com.cpc.spark.ml.parser

import java.text.SimpleDateFormat
import java.util.Date
import java.util.logging.SimpleFormatter

import com.cpc.spark.log.parser.UnionLog
import mlserver.mlserver.{AdInfo, MediaInfo, UserInfo}

import scala.util.Random
import scala.util.hashing.MurmurHash3.stringHash
import org.apache.spark.mllib.linalg.{Vector, Vectors}

/**
  * Created by Roy on 2017/5/15.
  */
object MLParser {

  def unionLogToSvm(x: UnionLog): String = {
    // 随机 1/5 的负样本
    if (x.isclick == 1 || Random.nextInt(5) == 0) {
      val cols = Seq[Double](
        stringHash(x.uid).toDouble,
        x.age.toDouble,
        x.sex.toDouble,
        x.coin.toDouble,
        //pcategory
        //interests
        //x.country.toDouble,
        x.province.toDouble,
        x.city.toDouble,
        x.isp.toDouble,
        x.network.toDouble,
        x.os.toDouble,
        //os version,
        stringHash(x.model).toDouble,
        //browser,

        x.media_appsid.toDouble,
        x.media_type.toDouble,
        //x.mediaclass,
        //x.channel,
        x.adslotid.toDouble,
        x.adslot_type.toDouble,
        //adstlotsize,
        x.floorbid.toDouble,

        x.adtype.toDouble,
        x.interaction.toDouble,
        x.userid.toDouble,
        x.planid.toDouble,
        x.unitid.toDouble,
        x.ideaid.toDouble,
        x.bid.toDouble,
        //ad class,
        //x.usertype,

        x.date.replace("-", "").toDouble,
        x.hour.toDouble


        //组合



      )

      var n = 1
      var svm = x.isclick.toString
      for (col <- cols) {
        svm = svm + " %d:%f".format(n, col)
        n += 1
      }
      svm
    } else {
      ""
    }
  }

  //model v2
  val min = Vectors.dense(Array(-2.14748336E9,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,-2.147047769E9,
    8.0000001E7,0.0,1018976.0,1.0,1.0,3.0,1.0,18.0,50.0,52.0,91.0,15.0,2.0170512E7,0.0))
  val max = Vectors.dense(Array(2.147482965E9,6.0,2.0,1.111188234E9,34.0,131100.0,18.0,4.0,2.0,2.147228562E9,
    8.0000005E7,0.0,1026459.0,2.0,500.0,4.0,1.0,316.0,694.0,1335.0,3216.0,300.0,2.0170522E7,23.0))

  def sparseVector(m: MediaInfo, u: UserInfo, ad: AdInfo): Vector = {
    val v =Vectors.dense(Array(
      stringHash(m.uid).toDouble,
      u.age.toDouble,
      u.sex.toDouble,
      u.coin.toDouble,
      //pcategory
      //interests
      //x.country.toDouble,
      m.province.toDouble,
      m.city.toDouble,
      m.isp.toDouble,
      m.network.toDouble,
      m.os.toDouble,
      //os version,
      stringHash(m.model).toDouble,
      //browser,

      m.mediaAppsid.toDouble,
      m.mediaType.toDouble,
      //x.mediaclass,
      //x.channel,
      m.adslotid.toDouble,
      m.adslotType.toDouble,
      //adstlotsize,
      m.floorbid.toDouble,

      ad.adtype.toDouble,
      ad.interaction.toDouble,
      ad.userid.toDouble,
      ad.planid.toDouble,
      ad.unitid.toDouble,
      ad.ideaid.toDouble,
      ad.bid.toDouble,
      //ad class,
      //x.usertype,

      getDateDouble(m.date),
      m.hour.toDouble
    ))
    normalize(min, max, v.toSparse)
  }

  val dateFormat = new SimpleDateFormat("yyyyMMdd")

  def getDateDouble(date: String): Double = {
    try {
      date.replace("-", "").toDouble
    } catch {
      case e: Exception =>
       dateFormat.format(new Date().getTime).toDouble
    }
  }


  def normalize(min: Vector, max: Vector, row: Vector): Vector = {
    var els = Seq[(Int, Double)]()
    row.foreachActive {
      (i, v) =>
        var rate = 0.5D
        if (max(i) > min(i)) {
          if (v < min(i)) {
            rate = 0
          } else if (v > max(i)) {
            rate = 1
          } else {
            rate = (v - min(i)) / (max(i) - min(i))
          }
        }
        els = els :+ (i, rate)
    }
    Vectors.sparse(row.size, els)
  }
}


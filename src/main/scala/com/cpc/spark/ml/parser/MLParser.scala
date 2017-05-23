package com.cpc.spark.ml.parser

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
    if (x.isclick == 1 || Random.nextInt(5) == 1) {
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

  val min = Vectors.dense(Array(1.0))
  val max = Vectors.dense(Array(1.0))

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

      m.date.replace("-", "").toDouble,
      m.hour.toDouble
    ))
    normalize(min, max, v.toSparse)
  }

  def normalize(min: Vector, max: Vector, row: Vector): Vector = {
    var els = Seq[(Int, Double)]()
    row.foreachActive {
      (i, v) =>
        var rate = 0.5D
        if (max(i) > min(i)) {
          rate = (v - min(i)) / (max(i) - min(i))
        }
        els = els :+ (i, rate)
    }
    Vectors.sparse(row.size, els)
  }
}


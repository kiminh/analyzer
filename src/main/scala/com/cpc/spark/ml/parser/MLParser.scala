package com.cpc.spark.ml.parser

import com.cpc.spark.log.parser.UnionLog
import mlserver.mlserver.{AdInfo, MediaInfo}

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
        x.network.toDouble,
        x.isp.toDouble,
        x.media_appsid.toDouble,
        x.bid.toDouble,
        x.ideaid.toDouble,
        x.unitid.toDouble,
        x.planid.toDouble,
        x.city.toDouble,
        x.adslotid.toDouble,
        x.adtype.toDouble,
        x.interaction.toDouble,
        Math.abs(stringHash(x.date)).toDouble,
        x.hour.toDouble
      )

      var n = 1
      var svm = x.isclick.toString
      for (col <- cols) {
        svm = svm + " %d:%f".format(n, col)
        n = n + 1
      }
      svm
    } else {
      ""
    }
  }

  def sparseVector(media: MediaInfo, ad: AdInfo): Vector = {
    val vals = Seq(
      (0, media.network.toDouble),
      (1, media.isp.toDouble),
      (2, media.mediaAppsid.toDouble),
      (3, ad.bid.toDouble),
      (4, ad.ideaid.toDouble),
      (5, ad.unitid.toDouble),
      (6, ad.planid.toDouble),
      (7, media.city.toDouble),
      (8, media.adslotid.toDouble),
      (9, media.adslotType.toDouble),
      (10, ad.interaction.toDouble),
      (11, Math.abs(stringHash(media.date)).toDouble),
      (12, media.hour.toDouble)
    )
    Vectors.sparse(vals.length, vals)
  }
}


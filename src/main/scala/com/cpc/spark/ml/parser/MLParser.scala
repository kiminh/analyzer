package com.cpc.spark.ml.parser

import com.cpc.spark.log.parser.UnionLog

import scala.util.Random
import scala.util.hashing.MurmurHash3.stringHash

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
}


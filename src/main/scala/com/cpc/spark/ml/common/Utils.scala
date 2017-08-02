package com.cpc.spark.ml.common

import com.typesafe.config.Config
import scala.collection.mutable
import sys.process._

/**
  * Created by roydong on 23/06/2017.
  */
object Utils {

  /*
  返回组合特征的位置，和最大位置号
   */
  def combineIntFeatureIdx(ids: Int*): Int = {
    var idx = 0
    for (i <- 0 until ids.length) {
      var v = 1
      for (j <- i + 1 until ids.length) {
        v = v * ids(j)
      }
      idx = idx + (ids(i) - 1) * v
      println(idx)
    }
    idx
  }

  def combineIntFeatureMax(m: Int*): Int = {
    var max = 1
    for (i <- 0 until m.length) {
      max = max * m(i)
    }
    max
  }

  //得到所有排列组合 C(n, m)
  def getCombination[T: Manifest](all: Seq[T], n: Int): Seq[Seq[T]] = {
    var combs = mutable.Seq[Seq[T]]()
    val comb = new Array[T](n)
    def mapCombination(n: Int, start: Int, idx: Int, comb: Array[T]): Unit = {
      if (n > 0) {
        for (i <- start until all.length) {
          comb(idx) = all(i)
          mapCombination(n - 1, i + 1, idx + 1, comb)
        }
      } else {
        var seq = mutable.Seq[T]()
        comb.foreach {
          v => seq = seq :+ v
        }
        combs :+= seq
      }
    }
    mapCombination(n, 0, 0, comb)
    combs
  }

  def updateOnlineData(srcfile: String, destfile: String, conf: Config): Unit = {
    val nodes = conf.getStringList("mlserver.nodes")
    for (i <- 0 until nodes.size()) {
      val node = nodes.get(i)
      val ret = s"scp $srcfile work@$node:/home/work/ml/model/$destfile" !
    }
  }
}

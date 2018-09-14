package com.cpc.spark.ml.train

import com.cpc.spark.common.Utils

/**
  * author: huazhenhao
  * date: 9/14/18
  */
object ModelVisualization {
  def main(args: Array[String]): Unit = {
    val pack = Utils.readPackFromFile("/Users/zhenhaohua/Downloads/ctr-qtt-list-17.mlm")
    println(s"full feature size: ${pack.lr.get.featureNum}")
    println(s"adv size: ${pack.dict.get.advertiserid.size}")
    println(s"plan size: ${pack.dict.get.planid.size}")
    var nonEmpty = 0
    pack.lr.get.weights.foreach( x => {
      if (Math.abs(x._2) > 1e-6) {
        nonEmpty += 1
      }
    })
    println(nonEmpty)
  }
}

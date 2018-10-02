package com.cpc.spark.ml.ftrl

/**
  * author: huazhenhao
  * date: 10/2/18
  */

@SerialVersionUID(100L)
class FtrlSerializable() extends Serializable {
  var alpha: Double = 0.01
  var beta: Double = 1.0
  var L1: Double = 0.0
  var L2: Double = 0.0
  var nDict, zDict, wDict = Map[Int, Double]()


}

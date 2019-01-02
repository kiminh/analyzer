package com.cpc.spark.ml.dnn.baseData

/**
  * 评估推到redis占用空间大小
  * created time : 2018/12/28 12:06
  *
  * @author zhj
  * @version 1.0
  *
  */
object EvalRedisVol {
  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      System.err.println(
        """
          |you have to input 2 parameters !!!
          |1) string to describe data
          |2) key prefix
        """.stripMargin)
      System.exit(1)
    }
    val Array(str, prefix) = args
    Utils.evalRedisVol(str, prefix)
  }
}

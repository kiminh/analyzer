package com.cpc.spark.ml.dnn.baseData

/**
  * 保存特征到redis
  * created time : 2018/12/17 10:43
  *
  * @author zhj
  * @version 1.0
  *
  */
object Save2RedisCluster {
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
    Utils.save2RedisCluster(str, prefix)

  }
}

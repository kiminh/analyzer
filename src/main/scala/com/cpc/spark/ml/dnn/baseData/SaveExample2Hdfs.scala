package com.cpc.spark.ml.dnn.baseData

/**
  * 保存tfrecord到hdfs
  * created time : 2018/12/17 10:38
  *
  * @author zhj
  * @version 1.0
  *
  */
object SaveExample2Hdfs {
  def main(args: Array[String]): Unit = {

    if (args.length != 3) {
      System.err.println(
        """
          |you have to input 3 parameters !!!
        """.stripMargin)
    }
    val Array(str, path, numPartitions) = args
    Utils.saveExample2Hdfs(str, path, numPartitions.toInt)
  }
}

package com.cpc.spark.ml.dnn.baseData

import scala.io.Source

/**
  * 解析dnn 模型生成的proto文件
  * created time : 2019/1/11 10:28
  *
  * @author zhj
  * @version 1.0
  *
  */
object ParseDnnGraph {
  def main(args: Array[String]): Unit = {
    val file = args(0)
    val graph = dnnmodel.Dnnmodel.DNNGraph
    val data = Source.fromFile(file)
    print(data.getLines())
    //graph.parseFrom()
  }
}

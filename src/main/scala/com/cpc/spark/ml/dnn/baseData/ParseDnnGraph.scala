package com.cpc.spark.ml.dnn.baseData

import java.io.{File, FileInputStream, PrintWriter}
import scala.collection.JavaConversions._
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

    val Array(src, dir, _*) = args
    val file = new FileInputStream(src)

    val graph = dnnmodel.Dnnmodel.DNNGraph.parseFrom(file)

    for (l <- graph.getTensorsList) {
      val out_name = dir + "/" + l.getName + "_" + l.getDimsList.mkString("-")
      val writer = new PrintWriter(new File(out_name))
      writer.write(l.getFloatDataList.mkString(","))
      writer.close()
    }
  }
}

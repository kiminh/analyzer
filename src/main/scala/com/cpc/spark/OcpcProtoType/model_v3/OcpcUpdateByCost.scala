package com.cpc.spark.OcpcProtoType.model_v3

import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object OcpcUpdateByCost {
  def main(args: Array[String]): Unit = {
    /*
    检查最近一个小时是否有oCPC消费，如果有消费，则更新用于计算cali_value和k值的pcoc和jfb以及post_cvr，否则不更新
     */
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()
    Logger.getRootLogger.setLevel(Level.WARN)

    val date = args(0).toString
    val hour = args(1).toString
    val media = args(2).toString
    val version = args(3).toString

    println("parameters:")
    println(s"date=$date, hour=$hour, media:$media, version:$version")


  }
}

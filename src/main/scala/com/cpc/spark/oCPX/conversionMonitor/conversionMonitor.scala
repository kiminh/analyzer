package com.cpc.spark.oCPX.conversionMonitor

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession


/**
  * @author 作者 :wangjun
  * @version 创建时间：2019-05-23
  * @desc
  */

object conversionMonitor {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("[ocpc-monitor] extract log data")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .enableHiveSupport()
      .getOrCreate()
    Logger.getRootLogger.setLevel(Level.WARN)

    val date = args(0).toString
    val dayCnt = args(1).toInt
    val minCV = args(2).toInt
    println("parameters:")
    println(s"date=$date")

    // todo 抽取点击数据和消费数据，按照unitid, userid, conversion_goal, conversion_from, is_ocpc做grouby
    // todo 抽取转化数据，按照unitid, userid, conversion_goal, conversion_from做groupby
    // todo 按照最小点击数限额，找到对应conversion_goal与conversion_from中无转化的单元
    // todo 根据是否是ocpc单元选择发送报警邮件


  }


}

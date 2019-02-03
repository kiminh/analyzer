package com.cpc.spark.OcpcProtoType.experiment

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import ocpcexps.ocpcexps.{OcpcexpsProto, OcpcList}

object OcpcABtest {
  def main(args: Array[String]): Unit = {
    /*
    identifier维度下的ab实验：提供cpcbid和是否开启ab实验的开关
     */
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()

    // 计算日期周期
    val date = args(0).toString
    val hour = args(1).toString


  }


}


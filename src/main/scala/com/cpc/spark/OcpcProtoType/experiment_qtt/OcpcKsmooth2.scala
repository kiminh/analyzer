package com.cpc.spark.OcpcProtoType.experiment_qtt

import java.io.FileOutputStream
import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.ocpc.OcpcUtils.getTimeRangeSql2
import com.typesafe.config.ConfigFactory
import ocpc.Ocpc
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import ocpcabtest.ocpcabtest.{OcpcList, SingleRecord}

import scala.collection.mutable.ListBuffer

object OcpcKsmooth2 {
  def main(args: Array[String]): Unit = {
    /*
    k值平滑策略：

     */
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()

    // 计算日期周期
    //    scp ocpc_abtest.pb cpc@192.168.80.23:/home/cpc/model_server/data/ocpc_abtest.pb
    val date = args(0).toString
    val hour = args(1).toString
    println("parameters:")
    println(s"date=$date, hour=$hour")


  }


}


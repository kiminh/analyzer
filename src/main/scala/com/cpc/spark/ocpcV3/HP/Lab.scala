package com.cpc.spark.ocpcV3.HP

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._


object Lab {
  def main(args: Array[String]): Unit ={
    val spark = SparkSession.builder().appName("appInstallation").enableHiveSupport().getOrCreate()
    val date = args(0).toString
    import spark.implicits._

    val sql1 =
      s"""
         |select
         | uid,
         | pkgs
         |from dl_cpc.cpc_user_installed_apps a
         |where load_date = '$date'
       """.stripMargin
    val pkgs = spark.sql(sql1)
    pkgs.show(3)


    val appFreq = pkgs.rdd
      .map(x =>  x.getAs[String]("pkgs").stripPrefix("[").stripSuffix("]"))
      .flatMap(x => x.split(","))
      .map(x => (x,1)).reduceByKey((x, y) => x+y).map( x => AppCount(x._1, x._2) ).toDF()
      .orderBy("count","desc")

    appFreq.show(5)

  }
  case class AppCount( var appName: String, var count: Int)
}

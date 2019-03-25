package com.cpc.spark.ocpcV3.HP

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._

object Lab {
  def main(args: Array[String]): Unit ={
    val spark = SparkSession.builder().appName("appInstallation").enableHiveSupport().getOrCreate()
    val date = args(0).toString
    import spark.implicits._

    val app_cat = spark.read.csv("/home/cpc/sunjianqiang/app_cat.csv")
    app_cat.show(10)




    val sql1 =
      s"""
         |select
         | uid,
         | concat_ws(',', app_name) as pkgs1
         |from dl_cpc.cpc_user_installed_apps a
         |where load_date = '$date'
       """.stripMargin
    val pkgs = spark.sql(sql1)
    pkgs.show(3)

    val appFreq = pkgs.rdd
      .map(x =>  x.getAs[String]("pkgs1") )
      .flatMap(x => x.split(","))
      .map(x => (x,1)).reduceByKey((x, y) => x+y).map(x => (x._1.split("-"), x._2)).map( x => if(x._1.length > 1) AppCount(x._1(0), x._1(1), x._2) else AppCount(x._1(0), "" , x._2)).toDF()

    appFreq.show(10)

    appFreq.write.mode("overwrite").saveAsTable("test.appInstalledCount_sjq")

//    appFreq.orderBy( appFreq("count").desc ).show(50, false)

  }

//  case class AppCat( var appName: String, )
  case class AppCount( var appName1: String, appName2: String, var count: Int)
}

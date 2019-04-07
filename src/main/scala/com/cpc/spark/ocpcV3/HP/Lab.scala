package com.cpc.spark.ocpcV3.HP

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.fpm.FPGrowth

object Lab {
  def main(args: Array[String]): Unit = {
    val date = args(0).toString
    val spark = SparkSession.builder().appName("Lab").enableHiveSupport().getOrCreate()

    val baseData = getBaseData(spark, date)
    baseData.write.mode("overwrite").saveAsTable("test.app_count_sjq")

  }

  def getBaseData(spark: SparkSession, date: String)={
    import spark.implicits._
    val sqlRequest =
      s"""
         |select
         | concat_ws(',', app_name) as pkgs
         | from dl_cpc.cpc_user_installed_apps a
         |where load_date = '$date'
       """.stripMargin

    val df = spark.sql(sqlRequest).rdd
      .map(x => x.getAs[String]("pkgs") )
      .flatMap( x => x.split(",") )
      .map(x => (x, 1))
      .reduceByKey((x, y) => x + y)
      .map( x => (x._1.split("-",2)(1), x._2) )
      .reduceByKey( (x, y) => x + y )
      .map( x => AppCount(x._1, x._2)).toDF
    df
  }

  case class AppCount(var appName: String, var count: Int)

}









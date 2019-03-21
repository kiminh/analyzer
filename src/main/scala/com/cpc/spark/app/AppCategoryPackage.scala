package com.cpc.spark.app

import com.cpc.spark.util.GetAppCateFromBaiduUil
import org.apache.spark.sql.SparkSession

import scala.collection.mutable
import scala.io.Source

object AppCategoryPackage {
  def main(args: Array[String]): Unit = {
    val date = args(0)
    val outdb = args(1)

    val appList = readAppList()
    val appCategory = appList.flatMap(name => GetAppCateFromBaiduUil.getAppCate(name, 50))
      .map(row => AppCategory(row._1, row._2))

    val spark = SparkSession.builder()
      .appName(s"AppCategoryPackage CoinUnionLog date = $date")
      .enableHiveSupport()
      .getOrCreate()

    val appCategoryRdd = spark.sparkContext.parallelize(appCategory)
    val appCategoryDF = spark.createDataFrame(appCategoryRdd).repartition(1).createOrReplaceTempView("t")
    spark.sql(
      s"""
         | insert overwrite table $outdb.app_category_package partition(dt='$date')
         | select name,pak from t
       """.stripMargin)


  }

  def readAppList() = {
    val file = Source.fromFile("appList")

    val list = new mutable.ArrayBuffer[String]()
    for (line <- file.getLines()) {
      list.append(line)
    }
    list
  }

  case class AppCategory(name: String, pak: String)

}

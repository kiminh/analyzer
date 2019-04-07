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

  def getBaseData(spark: SparkSession, date: String) = {
    import spark.implicits._
    val sqlRequest =
      s"""
         |select
         | uid,
         | concat_ws(',', app_name) as pkgs
         | from dl_cpc.cpc_user_installed_apps a
         |where load_date = '$date'
       """.stripMargin

    val df1 = spark.sql(sqlRequest).rdd
      .map(x => (x.getAs[String]("uid"), x.getAs[String]("pkgs").split(",")))
      .flatMap(x => {
        val uid = x._1
        val pkgs = x._2
        val lb = scala.collection.mutable.ListBuffer[UidComb]()
        for (comb <- pkgs) {
          lb += UidComb(uid, comb)
        }
        lb.distinct
      }).toDF("uid", "comb").rdd
      .map(x => (x.getAs[String]("uid"), x.getAs[String]("comb")))
      .map(x => {
        val uid = x._1
        val arr = x._2.split("-", 2)
        if (arr.size == 2) (uid, arr(1)) else (uid, "")
      }).toDF("uid", "appName")

    val apps_exception = Array("", "趣头条").mkString("('", "','", "')")
    val countLimit = 100

    val df2 = df1.rdd
      .map(x => x.getAs[String]("appName"))
      .map(x => (x, 1))
      .reduceByKey((x, y) => x + y).toDF("appName", "count")
      .filter(s"appName not in ${apps_exception} and  count >= ${countLimit}")

    val df3 = df1.join(df2, Seq("appName"), "left").select("uid", "appName", "count")

    df3
  }

  case class UidComb(var uid: String, var comb: String)

  //  case class AppCount(var appName: String, var count: Int)

}









package com.cpc.spark.ocpcV3.HP

import org.apache.spark.sql.{SparkSession, DataFrame}


object Lab {
  def main(args: Array[String]): Unit ={
    val spark = SparkSession.builder().appName("Lab").enableHiveSupport().getOrCreate()

    val date = args(0).toString
    val result = getPkgAppMap(spark, date)
    result.write.mode("overwrite").saveAsTable("test.result_sjq")

  }

  def getPkgAppMap(spark: SparkSession, date: String) = {
    import spark.implicits._
    val sql1 =
      s"""
         |select
         | concat_ws(',', app_name) as pkgs1
         |from dl_cpc.cpc_user_installed_apps a
         |where load_date = '$date'
       """.stripMargin

    val pkgs = spark.sql(sql1)
    val result = pkgs.rdd
      .map(x => x.getAs[String]("pkgs1"))
      .flatMap(x => x.split(","))
      .map(x => (x, 1))
      .reduceByKey((x, y) => x + y)
      .map(x => (x._1, x._1.split("-"), x._2))
      .map(x =>
        if (x._2.length > 1) {
          AppPkgMap(x._1, x._2(0), x._2(1), x._3)
        } else {
          AppPkgMap(x._1, x._2(0), "", x._3)
        }
      ).toDF()
    result
  }
  case class AppPkgMap(var comb: String, var pkg: String, appName: String, var count: Int)
}









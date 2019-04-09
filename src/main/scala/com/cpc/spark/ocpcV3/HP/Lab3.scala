package com.cpc.spark.ocpcV3.HP

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.rdd.RDD

object Lab3 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("Lab3").enableHiveSupport().getOrCreate()
    val date = args(0).toString
    val targetApp = args(1).toString

    val antecedent = getAntecedent(spark, targetApp).zipWithIndex()
      .map(x => (x._1.stripPrefix("{").stripSuffix("}").split(","), x._2))
      .map(x => (x._2, x._1, x._1.size))

    getMatchUid(spark, antecedent)

  }

  def getAntecedent(spark: SparkSession, targetApp: String) = {
    val totalLogs = spark.table("test.associationRule_base_data_sjq").count()
    val sql1 =
      s"""
         |select
         |  itemset,
         |  freq
         |from
         |  test.itemsetfreq_sjq b
         |where
         |  itemset = '{${targetApp}}'
       """.stripMargin
    println(sql1)
    val appFreq = spark.sql(sql1).rdd.map(x => (x.getAs[Long]("freq"))).reduce(_ + _)
    val support = appFreq.toDouble / totalLogs.toDouble

    val sql2 =
      s"""
         |select
         |  ante,
         |  cons,
         |  `conf`,
         |  ${support} as support,
         |  `conf`/${support} as lift
         |from
         |  test.AnteConsConf_sjq a
         |where
         |  cons = '{${targetApp}}'
       """.stripMargin
    println(sql2)

    val df1 = spark.sql(sql2).filter("lift >= 1.5")
    df1.show()

    val result = df1.rdd.map { x => x.getAs[String]("ante") }
    result
  }

  def getMatchUid(spark: SparkSession, antecedent: RDD[(Long, Array[String], Int)]) = {
    import spark.implicits._
    val df0 = spark.table("test.associationRule_base_data_sjq") // uid, appNames
      .rdd.map(x => (x.getAs[String]("uid"), x.getAs[String]("appNames").split(",")))
      .flatMap(x => {
        val uid = x._1
        val apps = x._2
        val lb = scala.collection.mutable.ListBuffer[UidApp]()
        for (app <- apps) {
          lb += UidApp(uid, app)
        }
        lb.distinct
      }).toDF("uid", "appName")

    val ante = antecedent.flatMap(
      x => {
        val id = x._1
        val apps = x._2
        val n = x._3
        val lb = scala.collection.mutable.ListBuffer[AnteComb]()
        for (app <- apps) {
          lb += AnteComb(id, app, n)
        }
        lb.distinct
      }
    ).toDF("id", "appName", "n")

    val df1 = df0.join(ante, "appName")
      .groupBy("uid", "id", "n")
      .agg(
        countDistinct("appName").alias("matchNum")
      ).withColumn("if_match", when(col("matchNum") === col("n"), lit(1)).otherwise(lit(0)) )

    df1.write.mode("overwrite").saveAsTable("test.if_match_sjq")

  }

  case class UidApp(var uid: String, var app: String)

  case class AnteComb(var id: Double, var app: String, var num: Int)

}

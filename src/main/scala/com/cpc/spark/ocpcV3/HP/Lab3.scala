package com.cpc.spark.ocpcV3.HP

import java.util.Calendar
import java.text.SimpleDateFormat
import org.apache.spark.sql.{SparkSession, DataFrame, Dataset, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.rdd.RDD
import com.cpc.spark.qukan.userprofile.SetUserProfileTag.SetUserProfileTagInHiveDaily

object Lab3 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("Lab3").enableHiveSupport().getOrCreate()
    val date = args(0).toString
    val targetApp = args(1).toString

    val antecedent = getAntecedent(spark, targetApp).zipWithIndex()
      .map(x => (x._1.stripPrefix("{").stripSuffix("}").split(","), x._2))
      .map(x => (x._2, x._1, x._1.size)) //RDD[(Long, Array[String], Int)]

    val targetUid = getMatchUid(spark, antecedent) //DataFrame: uid

    val targetUid2 = getMatchUid2(spark, date, targetUid) //DataFrame: uid, date
    //    targetUid2.write.mode("overwrite").saveAsTable("test.targetUid_sjq")
    targetUid2.write.mode("overwrite").insertInto("dl_cpc.hottopic_crowd_bag_collection_sjq")
//    update(spark, date)

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
    val appFreq = spark.sql(sql1).rdd.map(x => (x.getAs[Double]("freq"))).reduce(_ + _)
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
      ).withColumn("if_match", when(col("matchNum") === col("n"), lit(1)).otherwise(lit(0)))

    df1.write.mode("overwrite").saveAsTable("test.if_match_sjq")

    df1.where("if_match = 1").select("uid").distinct().toDF("uid")

  }

  def getMatchUid2(spark: SparkSession, date: String, targetUid: DataFrame) = {
    import spark.implicits._

    val sdf = new SimpleDateFormat("yyyy-MM-dd")
    val calendar = Calendar.getInstance
    val yesterday = sdf.parse(date)
    calendar.setTime(yesterday)
    calendar.add(Calendar.DATE, -7)
    val firstDay = calendar.getTime
    val date0 = sdf.format(firstDay)

    val sql1 =
      s"""
         | select
         |  uid
         | from dl_cpc.slim_union_log
         |where dt between '$date0' and '$date'
         |  and adsrc = 1
         |  and userid >0
         |  and isshow = 1
         |  and antispam = 0
         |  and media_appsid = '80002819'
         | group by uid
       """.stripMargin

    println(sql1)
    val df = spark.sql(sql1).persist()
    df.show()
    val df1 = df.join(targetUid, "uid").select("uid")
      .withColumn("tag", lit(337))
      .withColumn("date", lit(date))
      .select("uid", "tag", "date")
    df1
  }

  def update(spark: SparkSession, date: String): Unit = {
    val sdf = new SimpleDateFormat("yyyy-MM-dd")
    val calendar = Calendar.getInstance
    val today = sdf.parse(date)
    calendar.setTime(today)
    calendar.add(Calendar.DATE, -1)
    val yesterday = calendar.getTime()
    calendar.add(Calendar.DATE, -6)
    val startday = calendar.getTime()
    val date0 = sdf.format(yesterday)
    val date_1 = sdf.format(startday)

    val sqlRequest =
      s"""
         |select
         |  coalesce(t1.uid, t2.uid) as uid,
         |  tag0,
         |  tag1
         |from
         |  (
         |    select
         |      uid,
         |      1 as tag1
         |    from
         |      dl_cpc.hottopic_crowd_bag_collection_sjq
         |    where
         |      `date` = '$date'
         |	  and tag = 337
         |	  group by uid, 1
         |  ) t1 full
         |  outer join (
         |    select
         |      uid,
         |      1 as tag0
         |    from
         |      dl_cpc.hottopic_crowd_bag_collection_sjq
         |    where
         |      `date` between '$date_1' and '$date0'
         |	  and tag = 337
         |   group by uid, 1
         |  ) t2 on t1.uid = t2.uid
         """.stripMargin

    println(sqlRequest)
    val df = spark.sql(sqlRequest)
      .withColumn("id", lit(337))
      .withColumn("io", when(col("tag1").isNotNull, lit(true)).otherwise(lit(false)))
      .select("uid", "tag0", "tag1", "id", "io")

    //    df.write.mode("overwrite").saveAsTable("test.putOrDrop_sjq")

    val rdd1 = df.select("uid", "id", "io").rdd.map(x => (x.getAs[String]("uid"), x.getAs[Int]("id"), x.getAs[Boolean]("io")))
    val result = SetUserProfileTagInHiveDaily(rdd1)

  }


  case class UidApp(var uid: String, var app: String)

  case class AnteComb(var id: Double, var app: String, var num: Int)

}

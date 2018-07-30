package com.cpc.spark.log.anal

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  * Created by roydong on 25/07/2018.
  */
object OwnerStats {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("ad owner stats")
      .enableHiveSupport()
      .getOrCreate()

    val days = args(0).toInt
    val cal = Calendar.getInstance()
    cal.add(Calendar.DATE, -days)
    for (d <- 1 to days) {
      cal.add(Calendar.DATE, 1)
      val date = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime)
      val stmt =
        """
          |select ideaid, isshow, isclick, price, userid from dl_cpc.cpc_union_log
          |where `date` = "%s" and adsrc = 1
        """.stripMargin.format(date)
      println(stmt)
      val d = spark.sql(stmt).rdd
        .map {
          r =>
            val ideaid = r.getInt(0)
            val isshow = r.getInt(1)
            val isclick = r.getInt(2)
            val price = r.getInt(3)
            val userid = r.getInt(4)

            (ideaid, isshow, isclick, price, userid)
        }
        .cache()

      println("idea")
      val idea = d.map(x => (x._5, x._1)).distinct()
        .map {
          x =>
            (x._1, 1)
        }
        .reduceByKey(_ + _)
        .sortByKey(false)
        .take(20)
        .foreach {
          x =>
            println(x)
        }

      println("show")
      val show = d.map(x => (x._5, x._2)).reduceByKey(_ + _).sortByKey(false)
        .take(20)
        .foreach(println)

      println("cost")
      val cost = d.map(x => (x._5, x._3 * x._4)).reduceByKey(_ + _).sortByKey(false)
        .take(20)
        .foreach(println)

    }
  }
}

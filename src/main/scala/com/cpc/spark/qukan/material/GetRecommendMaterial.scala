package com.cpc.spark.qukan.material

import java.io.{File, PrintWriter}
import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import java.security.MessageDigest

import scala.io.Source


/**
  * Created by roydong on 07/06/2017.
  * get qukan recommend material
  */
object GetRecommendMaterial {

  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      System.err.println(
        s"""
           |Usage: GetRM <day_before> <int>
           |
        """.stripMargin)
      System.exit(1)
    }

    Logger.getRootLogger.setLevel(Level.WARN)
    val dayBefore = args(0).toInt
    val cal = Calendar.getInstance()
    cal.add(Calendar.DATE, -dayBefore)
    val day = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime)

    val ctx = SparkSession.builder()
      .appName("cpc get qukan dev tag [%s]".format(day))
      .enableHiveSupport()
      .getOrCreate()


    var lines = Seq[String]()
    for (line <- Source.fromFile("/home/work/mr/qukan/recommend_material.txt", "UTF8").getLines()) {
      lines = lines :+ line
    }
    var oldRdd = ctx.sparkContext.parallelize(lines)

    var newRdd: RDD[String] = ctx.sql(
      "select * from rpt_qukan.qukan_log_cpc_top_yhf where thedate >= \"%s\"".format(day)).rdd
      .map {
        x =>
          if (x.size > 8) {
            val title = x.getString(1).trim
            val img1 = x.getString(2)
            val img2 = x.getString(3)
            val img3 = x.getString(4)
            val show = x.getLong(5)
            val pv = x.getLong(6)
            val clk = x.getDouble(7)
            val tag = ""

            "%s\t%s\t%s\t%s\t%s\t%d\t%d\t%f\t%s".format("-", title, img1, img2, img3, show, pv, clk, tag)
          } else {
            ""
          }
      }
      .filter(_.length > 0)

    if (oldRdd.count() > 0) {
      newRdd = newRdd.union(oldRdd)
    }

    lazy val w = new PrintWriter(new File("/home/work/mr/qukan/recommend_material.txt"))
    var n = 0
    newRdd.map(_.split("\t"))
      .filter(_.length == 9)
      .map {
        x =>
          val hash = MessageDigest.getInstance("SHA-1").digest(x(1).getBytes())
          val hid = hash.map("%02x".format(_)).mkString
          x(0) = hid
          (hid, x)
      }
      .reduceByKey {
        (x, y) =>
          if (x(7).toFloat > y(7).toFloat) {
            x
          } else {
            y
          }
      }
      .map {
        x =>
          (x._2(7), x._2)
      }
      .sortByKey(false)
      .map(_._2)
      .take(2000000)
      .foreach {
        x =>
          w.write(x.mkString("\t") + "\n")
          n += 1
      }
    w.close()
    println("done", day, n)
    ctx.stop()
  }
}

package com.cpc.spark.qukan.material

import java.io.{File, PrintWriter}
import java.text.SimpleDateFormat
import java.util.Calendar

import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}


/**
  * Created by roydong on 07/06/2017.
  * get qukan recommend material
  */
object GetRecommendMaterial {

  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      System.err.println(
        s"""
           |Usage: GetUserProfile <day_before> <int>
           |
        """.stripMargin)
      System.exit(1)
    }

    Logger.getRootLogger.setLevel(Level.WARN)
    val dayBefore = args(0).toInt
    val cal = Calendar.getInstance()
    cal.add(Calendar.DATE, -dayBefore)
    val day = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime)
    val conf = ConfigFactory.load()

    val ctx = SparkSession.builder()
      .appName("cpc get qukan dev tag [%s]".format(day))
      .enableHiveSupport()
      .getOrCreate()

    var all: RDD[String] = null
    try {
      all = ctx.read.text("file:///home/work/mr/qukan/recommend_material.txt").rdd
        .map {
          x =>
            x.getString(0)
        }
    } catch {
      case e: Exception =>
    }


    val w = new PrintWriter(new File("/home/work/mr/qukan/recommend_material.txt"))
    var newRdd: RDD[String] = ctx.sql("select * from rpt_qukan.qukan_log_cpc_top_yhf").rdd
      .map {
        x =>
          if (x.size > 8) {
            val title = x.getString(1)
            val img1 = x.getString(2)
            val img2 = x.getString(3)
            val img3 = x.getString(4)
            val show = x.getLong(5)
            val pv = x.getLong(6)
            val clk = x.getDouble(7)
            "%s\t%s\t%s\t%s\t%d\t%d\t%f".format(title, img1, img2, img3, show, pv, clk)
          } else {
            ""
          }
      }
      .filter(_.length > 0)

    if (all != null) {
      newRdd = newRdd.union(newRdd)
    }

    var n = 0
    newRdd.map(_.split("\t"))
      .filter(_.length == 7)
      .map {
        x =>
          val hash = java.security.MessageDigest.getInstance("SHA-1").digest(x(0).getBytes).toString
          (hash, x)
      }
      .reduceByKey {
        (x, y) =>
          if (x(6).toFloat > y(6).toFloat) {
            x
          } else {
            y
          }
      }
      .map {
        x =>
          (x._2(6), x._2)
      }
      .sortByKey(false)
      .map(_._2)
      .take(500000)
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

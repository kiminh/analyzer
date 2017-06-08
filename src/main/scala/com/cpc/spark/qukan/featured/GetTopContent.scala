package com.cpc.spark.qukan.featured

import java.io.{File, PrintWriter}
import java.text.SimpleDateFormat
import java.util.Calendar

import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

import scala.reflect.io.Path

/**
  * Created by roydong on 07/06/2017.
  */
object GetTopContent {

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

    val w = new PrintWriter(new File(conf.getString("qukan.top_content.dump_file").format(day)))
    val topRdd = ctx.sql("select * from rpt_qukan.qukan_log_cpc_top_yhf").rdd
    topRdd.toLocalIterator
      .foreach {
        x =>
          if (x.size > 8) {
            val title = x.getString(1)
            val img1 = x.getString(2)
            val img2 = x.getString(3)
            val img3 = x.getString(4)
            val show = x.getLong(5)
            val pv = x.getLong(6)
            val clk = x.getDouble(7)
            w.write("%s\t%s\t%s\t%s\t%d\t%d\t%f\n".format(title, img1, img2, img3, show, pv, clk))
          }
      }
    w.close()
  }
}

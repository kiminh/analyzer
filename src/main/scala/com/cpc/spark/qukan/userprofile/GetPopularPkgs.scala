package com.cpc.spark.qukan.userprofile

import java.util.Calendar

import com.cpc.spark.qukan.parser.HdfsParser
import org.apache.spark.sql.SparkSession

/**
  * Created by Roy on 2017/5/3.
  */
object GetPopularPkgs {

  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      System.err.println(
        s"""
           |Usage: GetPopularPkgs <top n>
           |
        """.stripMargin)
      System.exit(1)
    }
    val cal = Calendar.getInstance()
    cal.add(Calendar.DATE, -1)
    val day = HdfsParser.dateFormat.format(cal.getTime)

    val ctx = SparkSession.builder()
      .appName("cpc get popular pkgs [%s]".format(day))
      .getOrCreate()

    //user app install info
    val aiPath = "/gobblin/source/lechuan/qukan/extend_report/%s".format(day)
    val aiRdd = ctx.read.orc(aiPath).rdd
      .map(HdfsParser.parseInstallApp(_, x => true))
      .filter(x => x != null && x.pkgs.length > 0)
      .flatMap(_.pkgs)
      .map(x => (x.name, 1))
      .reduceByKey((x, y) => x + y)
      .map(x => (x._2, x._1))
      .sortByKey(false)
      .take(args(0).toInt)
      .foreach {
        x =>
          println("%s %d".format(x._2, x._1))
      }

    ctx.stop()
  }
}

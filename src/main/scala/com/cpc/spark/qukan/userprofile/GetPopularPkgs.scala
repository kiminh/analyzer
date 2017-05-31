package com.cpc.spark.qukan.userprofile

import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.qukan.parser.HdfsParser
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession

import scala.collection.mutable

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
    val day = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime)

    val ctx = SparkSession.builder()
      .appName("cpc get popular pkgs [%s]".format(day))
      .getOrCreate()

    println("-----pkg usage-----")
    //user app install info
    val aiPath = "/gobblin/source/lechuan/qukan/extend_report/%s".format(day)
    val aiRdd = ctx.read.orc(aiPath).rdd
      .map(HdfsParser.parseInstallApp(_, x => true, null))
      .filter(x => x != null && x.pkgs.length > 0)
      .flatMap(_.pkgs)
      .cache()

    aiRdd.map(x => (x.name, 1))
      .reduceByKey((x, y) => x + y)
      .map(x => (x._2, x._1))
      .sortByKey(false)
      .take(args(0).toInt)
      .foreach {
        x =>
          println("%s %d".format(x._2, x._1))
      }

    println("-----tag usage-----")
    val conf = ConfigFactory.load()
    val pkgConf = conf.getConfig("userprofile.pkg_tags")
    aiRdd
      .flatMap {
        x =>
          val key = x.name.replace('.', '|')
          var vals = List[Int]()
          if (pkgConf.hasPath(key)) {
            val tags = pkgConf.getIntList(key)
            if (tags.size() > 0) {
              val it = tags.iterator()
              while (it.hasNext) {
                vals = vals :+ it.next().toInt
              }
            }
          }
          vals
      }
      .map(x => (x, 1))
      .reduceByKey((x, y) => x + y)
      .sortByKey(false)
      .toLocalIterator
      .foreach {
        x =>
          println(x)
      }

    ctx.stop()
  }
}




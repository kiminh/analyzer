package com.cpc.spark.qukan.interest

import java.sql.{DriverManager, ResultSet}
import java.text.SimpleDateFormat
import java.util.{Calendar, Properties}

import com.hankcs.hanlp.HanLP
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession

import scala.io.Source
import scala.collection.JavaConversions._

/**
  * Created by roydong on 25/09/2017.
  */
object TagArticle {


  def main(args: Array[String]): Unit = {
    val dayBefore = args(0).toInt
    val days = args(1).toInt

    val cal = Calendar.getInstance()
    cal.add(Calendar.DATE, -dayBefore)
    val dataStart = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime)
    cal.add(Calendar.DATE, days)
    val dataEnd = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime)

    val ctx = SparkSession.builder()
      .appName("user article ")
      .enableHiveSupport()
      .getOrCreate()
    import ctx.implicits._

    var words = Seq[String]()
    Source.fromFile("/data/cpc/anal/conf/words/finance.txt", "utf8")
      .getLines()
      .filter(_.length > 0)
      .foreach {
        line =>
          words = words :+ line
      }

    val bcWords = ctx.sparkContext.broadcast(words)
    var stmt = """
                 |SELECT DISTINCT qc.title,qc.detail
                 |from rpt_qukan.qukan_log_cmd qkc
                 |INNER JOIN gobblin.qukan_content qc ON qc.id=qkc.content_id
                 |WHERE qkc.cmd=301 AND qkc.thedate>="%s" AND qkc.thedate<"%s" AND qkc.member_id IS NOT NULL
                 |AND qkc.device IS NOT NULL
                 |""".stripMargin.format(dataStart, dataEnd)
    println(stmt)

    val articlePoints = ctx.sql(stmt).rdd
      .mapPartitions {
        partition =>
          val words = bcWords.value
          partition.map {
            row =>
              val title = row.getString(0)
              val content = row.getString(1)

              var sum = HanLP.segment(title)
                .filter(x => x.length() > 1)
                .map {
                  w =>
                    if (words.contains(w.word)) {
                      1
                    } else {
                      0
                    }
                }
                .sum

              sum += HanLP.segment(content)
                .filter(x => x.length() > 1)
                .map {
                  w =>
                    if (words.contains(w.word)) {
                      1
                    } else {
                      0
                    }
                }
                .sum

              (title, sum)
          }
      }.cache()

    stmt = """
                 |SELECT DISTINCT qkc.device,qc.title
                 |from rpt_qukan.qukan_log_cmd qkc
                 |INNER JOIN gobblin.qukan_content qc ON qc.id=qkc.content_id
                 |WHERE qkc.cmd=301 AND qkc.thedate>="%s" AND qkc.thedate<"%s" AND qkc.member_id IS NOT NULL
                 |AND qkc.device IS NOT NULL
                 |""".stripMargin.format(dataStart, dataEnd)
    println(stmt)

    val userPoints = ctx.sql(stmt).rdd
      .map {
        row =>
          val did = row.getString(0)
          val title = row.getString(1)
          (title, did)
      }
      .join(articlePoints)
      .map{
         x =>
           (x._2._1, x._2._2)
      }
      .reduceByKey(_ + _)

    println("article sum")
    articlePoints
      .map {
        x =>
          (x._2, 1)
      }
      .reduceByKey(_ + _)
      .sortByKey()
      .toLocalIterator
      .foreach {
        x =>
          println(x)
      }


    println("user sum")
    var n = 0
    userPoints
      .map {
        x =>
          (x._2, 1)
      }
      .reduceByKey(_ + _)
      .sortByKey()
      .filter(_._1 < 1000)
      .toLocalIterator
      .foreach {
        x =>
          println(x)
          n = n + x._1
      }


    articlePoints.unpersist()

    val adtitle = getAdDbResult("mariadb.adv")
      .map {
        x =>
          val sum = HanLP.segment(x._2)
            .filter(x => x.length() > 1)
            .map {
              w =>
                if (words.contains(w.word)) {
                  1
                } else {
                  0
                }
            }
            .sum

          (x._1, sum)
      }
      .filter(_._2 > 0)
      .toMap

    val bcadtitle = ctx.sparkContext.broadcast(adtitle)


    val ulog = ctx.sql(
      """
        |select uid,ideaid,isclick from dl_cpc.cpc_union_log
        |where isclick = 1 and adslot_type = 2
        |and `date` >= "%s" and `date` < "%s"
      """.stripMargin.format(dataStart, dataEnd))
      .rdd
      .map {
        row =>
          val uid = row.getString(0)
          val adid = row.getInt(1)
          val click = row.getInt(2)
          val adtitle = bcadtitle.value
          val sum = adtitle.getOrElse(adid, 0)


          if (sum > 0) {
            (uid, (1, 1, 1))
          } else {
            (uid, (0, 1, 1))
          }
      }


    val userCtr = ulog
      //.filter(_._2._1 > 0)
      .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2, x._3 + y._3))


    //var bins = Seq[(Double, Double, Double)]()
    var binSize = 10000
    var sum = 0d
    var adsum = 0d
    var click = 0d
    var show = 0d
    userCtr.join(userPoints)
      .map {
        x =>
          val sum = x._2._2
          val u = x._2._1
          val adsum = u._1
          val click = u._2
          val show = u._3
          (sum, u)
      }
      .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2, x._3 + y._3))
      .sortByKey()
      .toLocalIterator
      .foreach {
        x =>
          val u = x._2
          adsum = adsum + u._1.toDouble
          click = click + u._2.toDouble
          show = show + u._3.toDouble

          sum += x._1.toDouble * u._3.toDouble

          if (show > binSize) {
            val bin = (sum / show, adsum / show, click / show, show)
            //bins = bins :+ bin
            println("%.0f %.6f %.6f %.0f".format(bin._1, bin._2, bin._3, bin._4))

            click = 0
            show = 0
            sum = 0
            adsum = 0
          }
      }
  }

  def getAdDbResult(confKey: String): Seq[(Int, String)] = {
    val conf = ConfigFactory.load()
    val mariadbProp = new Properties()
    mariadbProp.put("url", conf.getString(confKey + ".url"))
    mariadbProp.put("user", conf.getString(confKey + ".user"))
    mariadbProp.put("password", conf.getString(confKey + ".password"))
    mariadbProp.put("driver", conf.getString(confKey + ".driver"))

    Class.forName(mariadbProp.getProperty("driver"))
    val conn = DriverManager.getConnection(
      mariadbProp.getProperty("url"),
      mariadbProp.getProperty("user"),
      mariadbProp.getProperty("password"))
    val stmt = conn.createStatement()
    val result = stmt.executeQuery("select id, user_id, plan_id, target_url, title from idea where action_type = 1")

    var rs = Seq[(Int, String)]()
    while (result.next()) {
      rs = rs :+ (result.getInt("id"), result.getString("title"))
    }
    rs
  }
}

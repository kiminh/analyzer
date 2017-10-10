package com.cpc.spark.qukan.interest

import java.text.SimpleDateFormat
import java.util.Calendar

import com.hankcs.hanlp.HanLP
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
    Source.fromFile("/data/cpc/anal/conf/words/finance.txt")
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
                      5
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
          val n = x._2 / 10
          (n.toInt, 1)
      }
      .reduceByKey(_ + _)
      .sortByKey()
      .toLocalIterator
      .foreach {
        x =>
          println(x)
      }

    println("user sum")
    userPoints.map {
        x =>
          val n = x._2 / 10
          (n.toInt, 1)
      }
      .reduceByKey(_ + _)
      .sortByKey()
      .toLocalIterator
      .foreach {
        x =>
          println(x)
      }

    articlePoints.unpersist()


    val userCtr = ctx.sql(
      """
        |select uid,isclick from dl_cpc.cpc_union_log
        |where isshow = 1 and adslot_type = 2 and round(ext['adclass'].int_value / 1e6, 0) == 104 and `date` >= "%s" and `date` < "%s"
      """.stripMargin.format(dataStart, dataEnd))
      .rdd
      .map {
        row =>
          val uid = row.getString(0)
          val isclick = row.getInt(1)
          (uid, (1, isclick))
      }
      .reduceByKey {
        (x, y) =>
          (x._1 + y._1, x._2 + y._2)
      }
      .filter {
        x =>
          x._2._2 > 0
      }

    userCtr.join(userPoints)
      .map {
        x =>

          val sum = x._2._2
          val log = x._2._1
          val n = sum / 10

          (n.toInt, (log._1, log._2, 1))
      }
      .reduceByKey {
        (x, y) =>
          (x._1 + y._1, x._2 + y._2, x._3 + y._3)
      }
      .map {
        x =>
          val info = x._2
          val ctr = info._2.toDouble / info._1.toDouble
          (x._1, (ctr, info._2, info._1, info._3))
      }
      .sortByKey()
      .toLocalIterator
      .foreach {
        x =>
          val info = x._2
          println("%d ctr=%.6f click=%d show=%d num=%d".format(x._1, info._1, info._2, info._3, info._4))
      }
  }
}

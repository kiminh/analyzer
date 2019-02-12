package com.cpc.spark.qukan.interest

import java.sql.DriverManager
import java.text.SimpleDateFormat
import java.util.{Calendar, Properties}

import com.hankcs.hanlp.HanLP
import com.redis.RedisClient
import com.redis.serialization.Parse.Implicits._
import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import userprofile.Userprofile.{InterestItem, UserProfile}

import scala.collection.JavaConversions._
import scala.io.Source

/**
  * Created by Roy on 2017/5/17.
  */
@deprecated
object InterestedWords {

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
    val days = args(1).toInt

    val conf = ConfigFactory.load()
    val ctx = SparkSession.builder()
      .appName("cpc get user interested words")
      .enableHiveSupport()
      .getOrCreate()

    val cal = Calendar.getInstance()
    cal.add(Calendar.DATE, -dayBefore)
    val dataStart = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime)
    cal.add(Calendar.DATE, days)
    val dataEnd = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime)


    val wordsPack = conf.getConfigList("userprofile.words_pack")
    for (i <- 0 until wordsPack.size()) {
      val pack = wordsPack.get(i)
      val tag = pack.getInt("tag")
      val thres = pack.getDouble("thres")
      println("start %s %d %.2f".format(pack.getString("name"), tag, thres))
      val articlePoints = getArticlePoints(ctx, pack.getString("file"), thres)
      val userPoints = getUserPoints(ctx, articlePoints, dataStart, dataEnd)
      val n = userPoints.filter(_._2 > 50).count()
      println(">50%", n)
      val sum = userPoints.mapPartitions {
        p =>
          var n = 0
          val redis = new RedisClient(conf.getString("redis.host"), conf.getInt("redis.port"))
          p.foreach {
            row =>
              n = n + 1
              val key = row._1 + "_UPDATA"
              val buffer = redis.get[Array[Byte]](key).orNull
              if (buffer != null) {
                val user = UserProfile.parseFrom(buffer).toBuilder
                val in = InterestItem.newBuilder()
                  .setTag(tag)
                  .setScore(row._2)
                var has = false
                for (i <- 0 until user.getInterestedWordsCount) {
                  val w = user.getInterestedWords(i)
                  if (w.getTag == in.getTag) {
                    if (!has) {
                      user.setInterestedWords(i, in)
                      has = true
                    } else {
                      user.removeInterestedWords(i)
                    }
                  }
                }
                if (!has) {
                  user.addInterestedWords(in)
                }
                redis.setex(key, 3600 * 24 * 7, user.build().toByteArray)
              }
          }
          Seq(n).iterator
      }
      println("%s updated: %.0f".format(pack.getString("name"), sum.sum()))
    }
  }

  def getArticlePoints(spark: SparkSession, wordsFile: String, thres: Double): RDD[(String, Double)] = {
    var words = Seq[String]()
    Source.fromFile(wordsFile, "utf8")
      .getLines()
      .filter(_.length > 0)
      .foreach {
        line =>
          words = words :+ line
      }

    val bcWords = spark.sparkContext.broadcast(words)
    val cal = Calendar.getInstance()
    cal.add(Calendar.DATE, -10)
    val aStart = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime)
    cal.add(Calendar.DATE, 10)
    val aEnd = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime)
    var stmt = """
                 |SELECT DISTINCT qc.title,qc.detail
                 |from rpt_qukan.qukan_log_cmd qkc
                 |INNER JOIN gobblin.qukan_content qc ON qc.id=qkc.content_id
                 |WHERE qkc.cmd=301 AND qkc.thedate>="%s" AND qkc.thedate<"%s" AND qkc.member_id IS NOT NULL
                 |AND qkc.device IS NOT NULL
                 |""".stripMargin.format(aStart, aEnd)
    println(stmt)

    val article = spark.sql(stmt).rdd
      .mapPartitions {
        partition =>
          val words = bcWords.value
          partition.map {
            row =>
              val title = row.getString(0)
              val content = row.getString(1)

              val awords = HanLP.segment(title + content)
                .filter(x => x.length() > 1)
                .map(_.word)

              val sum = awords
                .map {
                  w =>
                    if (words.contains(w)) {
                      1
                    } else {
                      0
                    }
                }
                .sum

              val score = sum.toDouble / awords.length.toDouble
              (title, score)
          }
      }
      .filter(_._2 > thres)
      .sortBy(x => x._2, false)
      .take(20000)

    article.foreach(println)

    spark.sparkContext.parallelize(article)
  }

  def getUserPoints(spark: SparkSession, articlePoints: RDD[(String, Double)], dataStart: String, dataEnd: String): RDD[(String, Int)] = {

    val stmt = """
             |SELECT DISTINCT qkc.device,qc.title
             |from rpt_qukan.qukan_log_cmd qkc
             |INNER JOIN gobblin.qukan_content qc ON qc.id=qkc.content_id
             |WHERE qkc.cmd=301 AND qkc.thedate>="%s" AND qkc.thedate<"%s" AND qkc.member_id IS NOT NULL
             |AND qkc.device IS NOT NULL
             |""".stripMargin.format(dataStart, dataEnd)
    println(stmt)

    val userPoints = spark.sql(stmt).rdd
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
      .filter(_._2 >= 0.05)

    val sum = userPoints.map(_._2).sum()
    val num = userPoints.count().toDouble
    val avg = sum / num
    println(sum, num, avg)
    userPoints.map {x => (x._1, (x._2.toDouble * 100 / avg).toInt)}.filter(_._2 >= 10)
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


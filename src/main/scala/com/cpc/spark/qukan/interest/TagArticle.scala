package com.cpc.spark.qukan.interest

import java.io.{FileWriter, PrintWriter}
import java.sql.{DriverManager, ResultSet}
import java.text.SimpleDateFormat
import java.util.{Calendar, Properties}

import com.cpc.spark.common.Utils
import com.hankcs.hanlp.HanLP
import com.hankcs.hanlp.corpus.tag.Nature
import com.typesafe.config.ConfigFactory
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.feature.{Word2Vec, Word2VecModel}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.rdd.RDD
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

    val spark = SparkSession.builder()
      .appName("user article word2Vec")
      .enableHiveSupport()
      .getOrCreate()

    val articleWords = getArticleWords(spark, days)
    //val model = trainWord2Vec(spark, articleWords, "/user/cpc/article_word2vec")
    val model = Word2VecModel.load(spark.sparkContext, "/user/cpc/article_word2vec")

    /*
    val twords = Seq("股票", "彩票", "棋牌", "麻将", "减肥", "投资", "理财")
    for (w <- twords) {
      println("find top synonyms for ", w, model.transform(w).size)
      val synonyms = model.findSynonyms(w, 50)
      for((synonym, cosineSimilarity) <- synonyms) {
        println(s"$synonym $cosineSimilarity")
      }
    }
    */

    val sample = articleWords.flatMap(_._2)
      .distinct()
      .map {
        w =>
          try {
            (w, model.transform(w))
          } catch {
            case e : Exception => null
          }
      }
      .filter(_ != null)
      .randomSplit(Array(0.8, 0.2), 1356L)

    val numClusters = 50
    val numIterations = 50
    println(sample(0).count(), sample(0).first()._2.size)
    val clusters = KMeans.train(sample(0).map(_._2), numClusters, numIterations)
    Utils.deleteHdfs("/user/cpc/kmeansmodel")
    clusters.save(spark.sparkContext, "/user/cpc/kmeansmodel")
    //val clusters = KMeansModel.load(spark.sparkContext, "/user/cpc/kmeansmodel")

    //val WSSSE = clusters.computeCost(sample(1))
    //println("Within Set Sum of Squared Errors = " + WSSSE)

    val fw = new PrintWriter("kmeans.txt")
    sample(1)
      .map {
        x =>
          (clusters.predict(x._2), Seq(x._1))
      }
      .reduceByKey((x, y) => x ++ y)
      .toLocalIterator
      .foreach {
        x =>
          println("cluster", x._1)
          x._2.take(20).foreach(println)

          fw.write("\ncluster %d\n".format(x._1))
          x._2.foreach {
            w =>
              fw.write("%s\n".format(w))
          }
      }
    fw.close()


    val stmt = """
                 |SELECT DISTINCT qkc.device,qc.title
                 |from rpt_qukan.qukan_log_cmd qkc
                 |INNER JOIN gobblin.qukan_content qc ON qc.id=qkc.content_id
                 |WHERE qkc.cmd=301 AND qkc.thedate>="%s" AND qkc.thedate<"%s" AND qkc.member_id IS NOT NULL
                 |AND qkc.device IS NOT NULL
                 |""".stripMargin.format(dataStart, dataEnd)
    //println(stmt)
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

  def getArticleWords(spark: SparkSession, days: Int): RDD[(String, Seq[String])] = {
    val cal = Calendar.getInstance()
    cal.add(Calendar.DATE, -days)
    val dataStart = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime)
    cal.add(Calendar.DATE, days)
    val dataEnd = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime)

    var stmt = """
                 |SELECT DISTINCT qc.title,qc.detail
                 |from rpt_qukan.qukan_log_cmd qkc
                 |INNER JOIN gobblin.qukan_content qc ON qc.id=qkc.content_id
                 |WHERE qkc.cmd=301 AND qkc.thedate>="%s" AND qkc.thedate<"%s" AND qkc.member_id IS NOT NULL
                 |AND qkc.device IS NOT NULL
                 |""".stripMargin.format(dataStart, dataEnd)
    println(stmt)

    spark.sql(stmt).rdd
      .map {
        row =>
          val title = row.getString(0)
          val content = row.getString(1)
          val words = HanLP.segment(title + " " + content)
            .filter {
              w =>
                w.nature.startsWith("n") || w.nature.startsWith("v")
            }
            .map {
              w =>
                w.word
            }
            .slice(0, 50)
          (title, words)
      }
  }

  def trainWord2Vec(spark: SparkSession, articleWords: RDD[(String, Seq[String])], path: String): Word2VecModel = {
    val words = articleWords.map(_._2)
    val maxRowLen = words.map(_.length).max()
    val minRowLen = words.map(_.length).min()
    println("row count", words.count(), "max row words num", maxRowLen, minRowLen)
    val w2v = new Word2Vec()
    val model = w2v.fit(words)
    Utils.deleteHdfs(path)
    model.save(spark.sparkContext, path)
    model
  }
}

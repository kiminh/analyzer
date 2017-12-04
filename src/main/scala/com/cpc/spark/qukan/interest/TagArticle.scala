package com.cpc.spark.qukan.interest

import java.io.{FileWriter, PrintWriter}
import java.sql.{DriverManager, ResultSet}
import java.text.SimpleDateFormat
import java.util.{Calendar, Properties}

import com.cpc.spark.common.Utils
import com.cpc.spark.log.parser.{ExtValue, TraceLog, UnionLog}
import com.cpc.spark.ml.cvrmodel.v1.FeatureParser
import com.cpc.spark.ml.train.LRIRModel
import com.hankcs.hanlp.HanLP
import com.hankcs.hanlp.corpus.tag.Nature
import com.typesafe.config.ConfigFactory
import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.feature.{Word2Vec, Word2VecModel}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.optimization.L1Updater
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.io.Source
import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.util.Random

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
    import spark.implicits._

    val userCvr = getUserCvr(spark, 1)
    //val userCvr = readUserCvr(spark).filter(_._2 == 104)


    val articleWords = getArticleWords(spark, days)
    //val model = trainWord2Vec(spark, articleWords, "/user/cpc/article_word2vec")
    val model = Word2VecModel.load(spark.sparkContext, "/user/cpc/article_word2vec")

    //val clusters = trainKmeans(spark, articleWords, model, 60)
    val clusters = KMeansModel.load(spark.sparkContext, "/user/cpc/kmeansmodel")

    val stmt = """
                 |SELECT DISTINCT qkc.device,qc.id
                 |from rpt_qukan.qukan_log_cmd qkc
                 |INNER JOIN gobblin.qukan_content qc ON qc.id=qkc.content_id
                 |WHERE qkc.cmd=301 AND qkc.thedate>="%s" AND qkc.thedate<"%s" AND qkc.member_id IS NOT NULL
                 |AND qkc.device IS NOT NULL
                 |""".stripMargin.format(dataStart, dataEnd)

    val userCate = spark.sql(stmt).rdd
      .map {
        row =>
          val uid = row.getString(0)
          val id = row.getLong(1)
          (id, uid)
      }
      .join(articleWords)
      .map(_._2)
      .reduceByKey(_ ++ _)
      .map {
        x =>
          val uid = x._1
          val kval = mutable.Map[Int, Int]()
          x._2.distinct
            .foreach {
              w =>
                try {
                  val vec = model.transform(w)
                  val k = clusters.predict(vec)
                  val v = kval.getOrElse(k, 0)
                  kval.update(k, v + 1)
                } catch {
                  case e : Exception => null
                }
            }
          (uid, kval.toSeq.filter(_._2 > 50))
      }
      .filter(_._2.length > 0)

    println("user cate", userCate.count())

    val adclk = userCvr.map(x => ((x._1, x._2), 1))
      .reduceByKey(_ + _)
      .map(x => (x._1._1, (x._1._2, x._2)))
      .join(userCate.map(x => (x._1, x._2.map(_._1))))
      .flatMap {
        x =>
          val adclass = x._2._1._1
          val clk = x._2._1._2
          val cates = x._2._2

          cates.map {
            x =>
              ((x, adclass), clk)
          }
      }
      .reduceByKey(_ + _)
      .sortBy(x => x._2, false)
      .take(200)
      .foreach(println)
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

  def getArticleWords(spark: SparkSession, days: Int): RDD[(Long, Seq[String])] = {
    val cal = Calendar.getInstance()
    cal.add(Calendar.DATE, -days)
    val dataStart = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime)
    cal.add(Calendar.DATE, days)
    val dataEnd = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime)

    var stmt = """
                 |SELECT DISTINCT qc.id,qc.title,qc.detail
                 |from rpt_qukan.qukan_log_cmd qkc
                 |INNER JOIN gobblin.qukan_content qc ON qc.id=qkc.content_id
                 |WHERE qkc.cmd=301 AND qkc.thedate>="%s" AND qkc.thedate<"%s" AND qkc.member_id IS NOT NULL
                 |AND qkc.device IS NOT NULL
                 |""".stripMargin.format(dataStart, dataEnd)
    println(stmt)

    spark.sql(stmt).rdd
      .map {
        row =>
          val id = row.getLong(0)
          val title = row.getString(1)
          val content = row.getString(2)
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
          (id, words)
      }
  }

  def trainWord2Vec(spark: SparkSession, articleWords: RDD[(Long, Seq[String])], path: String): Word2VecModel = {
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

  def trainKmeans(spark: SparkSession, articleWords: RDD[(Long, Seq[String])], model: Word2VecModel, k: Int): KMeansModel = {
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
      .randomSplit(Array(0.9, 0.1), 1356L)

    var clusters: KMeansModel = null
    println(sample(0).count(), sample(0).first()._2.size)
    clusters = KMeans.train(sample(0).map(_._2), k, 100)
    Utils.deleteHdfs("/user/cpc/kmeansmodel")
    clusters.save(spark.sparkContext, "/user/cpc/kmeansmodel")
    /*
    val WSSSE = clusters.computeCost(sample(1).map(_._2))
    println("Within Set Sum of Squared Errors = " + WSSSE)
    */

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
    clusters
  }

  val topCates = Seq(
    100,
    104,
    110,
    113,
    118,
    125,
    130
  )

  def getUserCvr(spark: SparkSession, days: Int): RDD[(String, Int, Int)] = {
    import spark.implicits._

    val cal = Calendar.getInstance()
    cal.add(Calendar.DATE, -days)
    val dataStart = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime)
    cal.add(Calendar.DATE, days)
    val dataEnd = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime)

    val sql = s"""
                 |select * from dl_cpc.cpc_union_log where `date` >= "%s" and `date` < "%s"
                 |and isclick = 1
        """.stripMargin.format(dataStart, dataEnd)

    val clicklog = spark.sql(sql)
      .as[UnionLog].rdd
      .map {
        x =>
          val adclass = x.ext.getOrElse("adclass", ExtValue()).int_value / 1e6
          (x.searchid, (x.uid, adclass.toInt))
      }
    println("click", clicklog.count())
    clicklog.map(x => (x._2._1, x._2._2, 1))

    /*
    val cvrlog = spark.sql(
      s"""
         |select * from dl_cpc.cpc_union_trace_log where `date` >= "%s" and `date` < "%s"
      """.stripMargin.format(dataStart, dataEnd))
      .as[TraceLog].rdd
      .coalesce(100)
      .map {
        x =>
          val u: UnionLog = null
          (x.searchid, Seq(x))
      }
      .reduceByKey(_ ++ _)
      .join(clicklog)
      .map {
        x =>
          val uid = x._2._2._1
          val adclass = x._2._2._2
          val traces = x._2._1
          if (cvrPositive(traces:_*)) {
            ((uid, adclass), 1)
          } else {
            ((uid, adclass), 0)
          }
      }
      .reduceByKey(_ + _)
      .filter(_._2 > 0)
      .map(x => (x._1._1, x._1._2, x._2))

    println("cvr", cvrlog.count())
    cvrlog
      .map {
        x =>
          (x._1, 1)
      }
      .reduceByKey(_+_)
      .map(x => (x._2, 1))
      .reduceByKey(_+_)
      .toLocalIterator
      .foreach(println)

    val df = cvrlog.map(x => "%s %d %d".format(x._1, x._2, x._3))
      .toDF()
      .write
      .mode(SaveMode.Overwrite)
      .text("/user/cpc/userAdclassCvr/v1")

    cvrlog
    */
  }

  def readUserCvr(spark: SparkSession): RDD[(String, Int, Int)] = {
    spark.read.text("/user/cpc/userAdclassCvr/v1").rdd
      .map {
        row =>
          val rs = row.getString(0).split(" ").toSeq
          if (rs.length == 3) {
            (rs(0), rs(1).toInt, rs(2).toInt)
          } else {
            null
          }
      }
      .filter(_ != null)
  }

  def cvrPositive(traces: TraceLog*): Boolean = {
    var stay = 0
    var click = 0
    var active = 0
    traces.foreach {
      t =>
        t.trace_type match {
          case s if s.startsWith("active") => active += 1

          case "buttonClick" => click += 1

          //case "clickMonitor" => click += 1

          case "inputFocus" => click += 1

          case "press" => click += 1

          case "stay" =>
            if (t.duration > stay) {
              stay = t.duration
            }

          case _ =>
        }

    }
    (stay >= 30 && click > 0) || active > 0
  }
}





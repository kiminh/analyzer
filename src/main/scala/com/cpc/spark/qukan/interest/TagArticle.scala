package com.cpc.spark.qukan.interest

import java.io.{FileWriter, PrintWriter}
import java.sql.{DriverManager, ResultSet}
import java.text.SimpleDateFormat
import java.util.{Calendar, Properties}

import com.cpc.spark.common.Utils
import com.cpc.spark.log.parser.{ExtValue, TraceLog, UnionLog}
import com.cpc.spark.ml.cvrmodel.v1.FeatureParser
import com.cpc.spark.ml.train.LRIRModel
import com.cpc.spark.qukan.parser.HdfsParser
import com.hankcs.hanlp.HanLP
import com.hankcs.hanlp.corpus.tag.Nature
import com.typesafe.config.ConfigFactory
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.feature.{Word2Vec, Word2VecModel}
import org.apache.spark.mllib.linalg.Vectors
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

    //val articleWords = getArticleWords(spark, days)
    //val model = trainWord2Vec(spark, articleWords, "/user/cpc/article_word2vec")
    //val model = Word2VecModel.load(spark.sparkContext, "/user/cpc/article_word2vec")

    //getUserInterested(spark, days)
    val userIntr = retrieveUserInterested(spark).filter(_._2 == 125).map(x => (x._1, x._3))
    println("user intr", userIntr.count())
    userIntr.take(10).foreach(println)

    val userPkgs = getUserPkgs(spark, days)
    println("user pkgs", userPkgs.count())
    userPkgs.take(10).foreach(println)

    val topPkgMap = mutable.Map[String, (Int, Double)]()
    var n = 0
    val topPkgs = userPkgs.flatMap(x => x._2.map(v => (v, 1d)))
      .reduceByKey(_ + _)
      .sortBy(x => x._2, false)
      .take(5000)
      .foreach {
        x =>
          topPkgMap.update(x._1, (n, 1 / x._2 * 1000))
          n += 1
          if (n > 4000) {
            println(x._1, x._2)
          }
      }

    val topPkgMapSpark = spark.sparkContext.broadcast(topPkgMap)
    val sample = userPkgs.leftOuterJoin(userIntr)
      .map {
        x =>
          val pkgMap = topPkgMapSpark.value
          val uid = x._1
          var label = 0d
          if (x._2._2.isDefined) {
            label = 1
          }
          var elems = Seq[(Int, Double)]()
          x._2._1.foreach {
            name =>
              if (pkgMap.isDefinedAt(name)) {
                elems = elems :+ pkgMap(name)
              }
          }
          val vec = Vectors.sparse(5000, elems.sortBy(x => x._1))
          LabeledPoint(label, vec)
      }
      .randomSplit(Array(0.9, 0.1), 666L)

    var train = sample(0)
    val pnum = train.filter(_.label > 0).count()
    val num = train.count()

    val rate = pnum.toDouble / (num.toDouble - pnum.toDouble) * 10 * 1000
    train = train.filter(x => x.label > 0 || Random.nextInt(1000) < rate.toInt)
    val test = sample(1)

    println(train.count(), pnum, train.count(), train.first())
    val lr = new LRIRModel
    lr.setSpark(spark)
    lr.run(train, 100, 1e-8)
    lr.test(test)
    lr.printLrTestLog()
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

    println(sample(0).count(), sample(0).first()._2.size)
    val clusters = KMeans.train(sample(0).map(_._2), k, 100)
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

  def getUserInterested(spark: SparkSession, days: Int): RDD[(String, Int, Int)] = {
    import spark.implicits._

    val cal = Calendar.getInstance()
    cal.add(Calendar.DATE, -days)
    val dataStart = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime)
    cal.add(Calendar.DATE, days)
    val dataEnd = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime)

    val sql = s"""
                 |select searchid,uid,ext['adclass'].int_value as adclass
                 |from dl_cpc.cpc_union_log where `date` >= "%s" and `date` < "%s"
                 |and isclick = 1
        """.stripMargin.format(dataStart, dataEnd)

    val clicklog = spark.sql(sql).rdd
      .map {
        x =>

          val adclass = x.getAs[Int]("adclass") / 1e6
          (x.getAs[String]("searchid"), (x.getAs[String]("uid"), adclass.toInt))
      }
    println("click", clicklog.count())
    clicklog.map(x => (x._2._1, x._2._2, 1))

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
          if (isInterested(traces:_*)) {
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
  }

  def retrieveUserInterested(spark: SparkSession): RDD[(String, Int, Int)] = {
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

  def isInterested(traces: TraceLog*): Boolean = {
    var stay = 0
    var click = 0
    var active = 0
    traces.foreach {
      t =>
        t.trace_type match {
          case s if s.startsWith("active") => active += 1

          case "buttonClick" => click += 1

          case "clickMonitor" => click += 1

          case "inputFocus" => click += 1

          case "press" => click += 1

          case "stay" =>
            if (t.duration > stay) {
              stay = t.duration
            }

          case _ =>
        }

    }
    (stay >= 10 && click > 0) || active > 0
  }

  def getUserPkgs(spark: SparkSession, days: Int): RDD[(String, List[String])] = {
    val cal = Calendar.getInstance()
    cal.add(Calendar.DATE, -days)
    var pathSep = Seq[String]()
    for (n <- 1 to days) {
      val date = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime)
      pathSep = pathSep :+ date
      cal.add(Calendar.DATE, 1)
    }

    val aiPath = "/gobblin/source/lechuan/qukan/extend_report/{%s}".format(pathSep.mkString(","))
    println(aiPath)
    spark.read.orc(aiPath).rdd
      .map(HdfsParser.parseInstallApp(_, x => true, null))
      .filter(x => x != null && x.pkgs.length > 0)
      .map(x => (x.devid, x.pkgs.map(_.name)))
      .reduceByKey(_ ++ _)
      .map(x => (x._1, x._2.distinct))
  }
}





package com.cpc.spark.qukan.interest

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.spark.ml.classification.LogisticRegressionModel
import org.apache.spark.ml.linalg.Vectors
import com.typesafe.config.ConfigFactory
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.sql.{Row, SparkSession}
import com.redis.RedisClient
import com.redis.serialization.Parse.Implicits._

import scala.util.control._
import ml.dmlc.xgboost4j.scala.spark._

import scala.collection.mutable
import com.cpc.spark.qukan.userprofile.SetUserProfileTag

/**
  * Created by myt on 11/08/2018.
  */
object PredictAgeHourly {
  val words_fnum = 41e4
  val word_num = 44e4
  val app_num = 1000
  val hour_num = 24
  val leaf_num = 64
  val round_num = 50
  def main(args: Array[String]): Unit = {
    val days = args(0).toInt
    val is_set = args(3).toBoolean

    val cal = Calendar.getInstance()
    cal.add(Calendar.HOUR, -2)
    val date = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime)
    val hour = new SimpleDateFormat("HH").format(cal.getTime)

    val spark = SparkSession.builder()
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.kryoserializer.buffer.max", "2047MB")
      .appName("predict age".format())
      .enableHiveSupport()
      .getOrCreate()
    import spark.implicits._

    val stmt =
      """
        |select distinct uid as did from dl_cpc.cpc_union_log
        |where `date` = "%s" and `hour` = "%s" and media_appsid in ("80000001","80000002")
        |and interests not like "%%224=100%%" and interests not like "%%225=100%%"
      """.stripMargin.format(date,hour)
    println(stmt)
    val uv = spark.sql(stmt)
    println(uv.count())
    val appids = mutable.Map[String,(Int, Int)]()
    spark.read.parquet("/user/cpc/qtt-appids/map").rdd.toLocalIterator.foreach {
      x =>
        appids.update(x.getAs[String](0),(x.getAs[Int](1), x.getAs[Int](2)))
    }
    val uidApp = spark.read.parquet("/user/cpc/userInstalledApp/%s".format(date)).rdd
      .map(x => (x.getAs[String]("uid"),x.getAs[mutable.WrappedArray[String]]("pkgs")))
      .reduceByKey(_ ++ _)
      .map(x => (x._1,x._2.distinct))
    val bappids = spark.sparkContext.broadcast(appids)
    val apps = uidApp
      .map {
        x =>
          val appids = bappids.value
          val did = x._1
          val idx = x._2
            .map{
              v =>
                val app = appids.get(v)
                if (app.isDefined) {
                  (v, app.get._1, app.get._2)
                } else {
                  null
                }
            }
            .filter(_ != null)
          (did, idx)
      }
      .toDF("did", "apps")

    val sample = uv.join(apps, Seq("did")).rdd
      .map {
        r =>
          val did = r.getAs[String]("did")
          val apps = r.getAs[Seq[Row]]("apps")
          var els = Seq[(Int, Double)]()
          val size = word_num + app_num + hour_num

          if (apps != null) {
            apps.foreach {
              app =>
                els = els :+ ((app.getInt(1) + word_num).toInt, 1d)
            }
          }
          if (apps != null) {
            els = els :+ ((size + 2).toInt, apps.length.toDouble)
          }
          val vec = Vectors.sparse((size + 3).toInt, els)
          if (apps != null) {
            (did, vec)
          } else {
            null
          }
      }.filter(_ != null)
      .toDF("did", "features")

    println(sample.count())
    sample.rdd.take(10).foreach(println)
    val lr = LogisticRegressionModel.load("/user/cpc/qtt-age-lrmodel/%s".format(days))
    val xgb = XGBoostModel.load("/user/cpc/qtt-age-xgmodel/%s".format(days))
    val xg_result = xgb.transformLeaf(sample)
    val lr_input = xg_result.rdd.map {
      r =>
        val did = r.getAs[String]("did")
        val leaf = r.getAs[mutable.WrappedArray[Float]](2).toArray
        var n_els = Seq[(Int, Double)]()

        for (i <- 0 to round_num - 1) {
          n_els = n_els :+ ((i * leaf_num + leaf(i)).toInt, 1d)
        }
        var vec = Vectors.sparse((leaf_num * round_num), n_els)
        (did, vec)
    }.toDF("did", "features")
    val result = lr.transform(lr_input)
    val f = args(1).toDouble
    val m = args(2).toDouble
    val conf = ConfigFactory.load()
    var st : String = ""
    val bst = spark.sparkContext.broadcast(st)
    val predict = result.rdd.map {
      r =>
        val did = r.getAs[String]("did")
        val score = r.getAs[Vector]("probability").toArray
        (did, score(1))
    }
    predict.take(10).foreach(println)
    println(predict.filter(_._2 > m).count())
    println(predict.filter(_._2 < f).count())
    val toSet = predict.flatMap {
      x =>
        if (x._2 > m) {
          Seq((x._1, 225, true), (x._1, 224, false))
        } else if (x._2 < f){
          Seq((x._1, 224, true), (x._1, 225, false))
        } else {
          Seq()
        }
    }
    println(toSet.count())
    val rs = SetUserProfileTag.SetUserProfileTagInHiveHourly(toSet, date, hour)
    rs.foreach(println)
  }
}

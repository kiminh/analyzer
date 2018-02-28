package com.cpc.spark.ml.ctrmodel.gbdt

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import ml.dmlc.xgboost4j.scala.spark.{XGBoost, XGBoostEstimator, XGBoostModel, XGBoostRegressionModel}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.linalg.{SQLDataTypes, Vector, VectorUDT, Vectors}
import org.apache.spark.ml.feature.{LabeledPoint, StringIndexer, VectorAssembler, VectorIndexer}
import org.apache.spark.ml.regression.{GBTRegressionModel, GBTRegressor}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.rdd.RDD
import com.cpc.spark.common.Utils
import org.apache.spark.sql.types.{DoubleType, StructField, StructType}

import scala.util.Random

/**
  * Created by roydong on 31/01/2018.
  */
object Train {

  Logger.getRootLogger.setLevel(Level.WARN)

  val cols = Seq(
    "sex", "age", "os", "isp", "network", "city",
    "mediaid_", "adslotid_", "phone_level", "adclass",
    "pagenum", "bookid_", "adtype", "adslot_type", "planid",
    "unitid", "ideaid", "user_req_ad_num", "user_req_num"
  )

  def main(args: Array[String]): Unit = {


    val spark = SparkSession.builder()
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .appName("xgboost")
      .enableHiveSupport()
      .getOrCreate()
    import spark.implicits._

    var pathSep = Seq[String]()
    val cal = Calendar.getInstance()
    for (n <- 1 to 7) {
      val date = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime)
      val hour = new SimpleDateFormat("HH").format(cal.getTime)
      pathSep = pathSep :+ date
      cal.add(Calendar.DATE, -1)
    }

    val path = "/user/cpc/lrmodel/ctrdata_v1/{%s}/*".format(pathSep.mkString(","))
    println(path)
    val srcdata = spark.read.parquet(path).coalesce(1000)
    val data = srcdata.map {
        r =>
          val vec = getVectorParser2(r)
          (r.getAs[Int]("label"), vec)
      }
      .toDF("label", "features")

    val Array(tmp1, tmp2) = data.randomSplit(Array(0.9, 0.1), 123L)
    val test = getLimitedData(1e7, tmp2)

    val totalNum = data.count().toDouble
    val pnum = tmp1.filter(x => x.getAs[Int]("label") > 0).count().toDouble
    val rate = (pnum * 10 / totalNum * 1000).toInt
    println(pnum, totalNum, rate)
    val tmp = tmp1.filter(x => x.getAs[Int]("label") > 0 || Random.nextInt(1000) < rate)
    val train = getLimitedData(4e7, tmp1)
    //val Array(train, test) = data.randomSplit(Array(0.9, 0.1), 123L)

    val params = Map(
      //"eta" -> 1f,
      //"lambda" -> 2.5
      "num_round" -> 20,
      //"max_delta_step" -> 4,
      "colsample_bytree" -> 0.8,
      "max_depth" -> 10, //数的最大深度。缺省值为6 ,取值范围为：[1,∞]
      "objective" -> "reg:logistic" //定义学习任务及相应的学习目标
    )

    val xgb = new XGBoostEstimator(params)
    val model = xgb.train(train)

    Utils.deleteHdfs("/user/cpc/xgboost/ctr_v1")
    model.save("/user/cpc/xgboost/ctr_v1")
    model.booster.saveModel("/home/cpc/qtt.gbm")

    val predictions = model.transform(test)

    val evaluator = new RegressionEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("rmse")
    val rmse = evaluator.evaluate(predictions)
    println("Root Mean Squared Error (RMSE) on test data = " + rmse)

    val result = predictions.rdd
      .map {
        r =>
          val label = r.getAs[Int]("label").toDouble
          val p = r.getAs[Float]("prediction").toDouble
          (p, label)
      }

    printLrTestLog(result)

    val metrics = new BinaryClassificationMetrics(result)

    // AUPRC
    val auPRC = metrics.areaUnderPR

    // ROC Curve
    //val roc = metrics.roc

    // AUROC
    val auROC = metrics.areaUnderROC

    println(auPRC, auROC)

  }

  //限制总的样本数
  def getLimitedData(limitedNum: Double, ulog: DataFrame): DataFrame = {
    var rate = 1d
    val num = ulog.count().toDouble

    if (num > limitedNum){
      rate = limitedNum / num
    }

    ulog.randomSplit(Array(rate, 1 - rate), new Date().getTime)(0).coalesce(1000)
  }

  def printLrTestLog(lrTestResults: RDD[(Double, Double)]): Unit = {
    val testSum = lrTestResults.count()
    if (testSum < 0) {
      throw new Exception("must run lr test first or test results is empty")
    }
    var test0 = 0
    var test1 = 0
    lrTestResults
      .map {
        x =>
          var label = 0
          if (x._2 > 0.01) {
            label = 1
          }
          (label, 1)
      }
      .reduceByKey((x, y) => x + y)
      .toLocalIterator
      .foreach {
        x =>
          if (x._1 == 1) {
            test1 = x._2
          } else {
            test0 = x._2
          }
      }

    var log = "predict distribution %s %d(1) %d(0)\n".format(testSum, test1, test0)
    lrTestResults
      .map {
        x =>
          val v = (x._1 * 100).toInt / 5
          ((v, x._2.toInt), 1)
      }
      .reduceByKey((x, y) => x + y)
      .map {
        x =>
          val key = x._1._1
          val label = x._1._2
          if (label == 0) {
            (key, (x._2, 0))
          } else {
            (key, (0, x._2))
          }
      }
      .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
      .sortByKey(false)
      .toLocalIterator
      .foreach {
        x =>
          val sum = x._2
          log = log + "%.2f %d %.4f %.4f %d %.4f %.4f %.4f\n".format(x._1.toDouble * 0.05,
            sum._2, sum._2.toDouble / test1.toDouble, sum._2.toDouble / testSum.toDouble,
            sum._1, sum._1.toDouble / test0.toDouble, sum._1.toDouble / testSum.toDouble,
            sum._2.toDouble / (sum._1 + sum._2).toDouble)
      }

    println(log)
  }

  def getVectorParser2(x: Row): Vector = {
    val cal = Calendar.getInstance()
    cal.setTimeInMillis(x.getAs[Int]("timestamp") * 1000L)
    val week = cal.get(Calendar.DAY_OF_WEEK)   //1 to 7
    val hour = cal.get(Calendar.HOUR_OF_DAY)
    var els = Seq[Int]()

    els = els :+ week
    els = els :+ hour
    els = els :+ x.getAs[Int]("sex")
    els = els :+ x.getAs[Int]("age")
    els = els :+ x.getAs[Int]("os")
    els = els :+ x.getAs[Int]("isp")
    els = els :+ x.getAs[Int]("network")
    els = els :+ x.getAs[Int]("city")
    els = els :+ x.getAs[String]("media_appsid").toInt
    els = els :+ x.getAs[String]("adslotid").toInt
    els = els :+ x.getAs[Int]("phone_level")
    els = els :+ x.getAs[Int]("pagenum")

    try {
      els = els :+ x.getAs[String]("bookid").toInt
    } catch {
      case e: Exception =>
        els = els :+ 0
    }

    els = els :+ x.getAs[Int]("adclass")
    els = els :+ x.getAs[Int]("adtype")
    els = els :+ x.getAs[Int]("adslot_type")
    els = els :+ x.getAs[Int]("planid")
    els = els :+ x.getAs[Int]("unitid")
    els = els :+ x.getAs[Int]("ideaid")
    els = els :+ x.getAs[Int]("user_req_ad_num")
    els = els :+ x.getAs[Int]("user_req_num")

    Vectors.dense(els.map(_.toDouble).toArray)
  }
}




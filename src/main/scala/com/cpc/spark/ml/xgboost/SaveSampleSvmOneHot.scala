package com.cpc.spark.ml.xgboost

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import com.cpc.spark.common.Utils
import ml.dmlc.xgboost4j.scala.spark.{XGBoostEstimator, XGBoostModel}
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.mllib.linalg.{SparseVector => MSVec, Vector => MVec, Vectors => MVecs}
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.classification.{LogisticRegressionModel, LogisticRegressionWithLBFGS}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.optimization.L1Updater
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}

import scala.collection.mutable
import scala.util.Random

/**
  * Created by roydong on 19/03/2018.
  */
object SaveSampleSvmOneHot {

  var spark: SparkSession = null

  def main(args: Array[String]): Unit = {
    val sc = SparkSession.builder()
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.kryoserializer.buffer.max", "1024m")
      .appName("xgboost get train sample")
      .enableHiveSupport()
      .getOrCreate()
    import sc.implicits._
    spark = sc

    train(spark, null, "")
    System.exit(1)

    var pathSep = Seq[String]()
    val cal = Calendar.getInstance()
    for (n <- 1 to args(1).toInt) {
      cal.add(Calendar.DATE, -1)
      val date = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime)
      val hour = new SimpleDateFormat("HH").format(cal.getTime)
      pathSep = pathSep :+ date
    }

    initFeatureDict(spark, pathSep)

    val path = "/user/cpc/lrmodel/%s/{%s}/*".format(args(0), pathSep.mkString(","))
    println(path)
    var qtt = spark.read.parquet(path).filter(_.getAs[Int]("ideaid") > 0)

    if (args(2) == "qtt-list") {
      qtt = qtt.filter{
        x =>
          Seq("80000001", "80000002").contains(x.getAs[String]("media_appsid")) &&
            Seq(1).contains(x.getAs[Int]("adslot_type"))
      }
    } else if (args(2) == "qtt-content") {
      qtt = qtt.filter{
        x =>
          Seq("80000001", "80000002").contains(x.getAs[String]("media_appsid")) &&
            Seq(2).contains(x.getAs[Int]("adslot_type"))
      }
    } else if (args(2) == "qtt-all") {
      qtt = qtt.filter{
        x =>
          Seq("80000001", "80000002").contains(x.getAs[String]("media_appsid")) &&
            Seq(1, 2).contains(x.getAs[Int]("adslot_type"))
      }
    }

    qtt = getLimitedData(4e8, qtt)
    qtt = intFreqTransform(qtt, "city", "citydx")
    qtt = stringFreqTransform(qtt, "adslotid", "adslotidx")
    qtt = intFreqTransform(qtt, "adclass", "adclassdx")
    qtt = intFreqTransform(qtt, "planid", "plandx")
    qtt = intFreqTransform(qtt, "unitid", "unitdx")
    qtt = intFreqTransform(qtt, "ideaid", "ideadx")

    val BcDict = spark.sparkContext.broadcast(dict)
    qtt = qtt
      .mapPartitions {
        p =>
          dict = BcDict.value
          p.map {
            r =>
              val vec1 = getVectorParser2(r)
              val vec2 = parseOneHotVector(r)
              (r.getAs[Int]("label"), vec1, vec2)
          }
      }
      .toDF("label", "features", "lr_features")

    train(spark, qtt, "")
  }

  def train(spark: SparkSession, data: DataFrame,  destfile: String): Unit = {
    /*
    val Array(tmp1, tmp2) = data.randomSplit(Array(0.9, 0.1), 123L)
    var test = getLimitedData(1e7, tmp2)
    val totalNum = data.count().toDouble
    val pnum = tmp1.filter(x => x.getAs[Int]("label") > 0).count().toDouble
    val rate = (pnum * 10 / totalNum * 1000).toInt // 1.24% * 10000 = 124
    println("xgb train: pnum=%.0f totalNum=%.0f rate=%d/1000".format(pnum, totalNum, rate))
    val tmp = tmp1.filter(x => x.getAs[Int]("label") > 0 || Random.nextInt(1000) < rate) //之前正样本数可能占1/1000，可以变成占1/100
    var train = getLimitedData(2e7, tmp)

    train.toDF().write.mode(SaveMode.Overwrite).parquet("/user/cpc/xgboost_traindata")
    test.toDF().write.mode(SaveMode.Overwrite).parquet("/user/cpc/xgboost_testdata")
    */

    var train = spark.read.parquet("/user/cpc/xgboost_traindata")
    var test = spark.read.parquet("/user/cpc/xgboost_testdata")

    val params = Map(
      "eta" -> 0.2,
      //"lambda" -> 2.5
      "gamma" -> 0,
      "num_round" -> 25,
      //"max_delta_step" -> 4,
      "subsample" -> 0.8,
      "colsample_bytree" -> 0.4,
      "min_child_weight" -> 1,
      "max_depth" -> 40,
      "reg_alpha" -> 0.2,
      "nthread" -> 10,
      "seed" -> 0,
      "objective" -> "reg:logistic"
    )

    //val xgb = new XGBoostEstimator(params)
    //val model = xgb.train(train)
    //Utils.deleteHdfs("/user/cpc/xgboost_tmpmodel")
    //model.save("/user/cpc/xgboost_tmpmodel")
    val model = XGBoostModel.load("/user/cpc/xgboost_tmpmodel")
    train = model.transformLeaf(train)
    test = model.transformLeaf(test)

    println(train.schema, train.columns.mkString(" "))
    println(train.first())




    //lr
    val lbfgs = new LogisticRegressionWithLBFGS().setNumClasses(2)
    lbfgs.optimizer.setUpdater(new L1Updater())
    lbfgs.optimizer.setNumIterations(200)
    lbfgs.optimizer.setConvergenceTol(1e-8)
    val lrtrain = train.rdd
      .map {
        r =>
          val vec = r.getAs[Vector]("lr_features")
          val leaves = r.getAs[mutable.WrappedArray[Float]]("predLeaf")

          var els = Seq[(Int, Double)]()
          vec.foreachActive {
            (k, v) =>
              els = els :+ (k, v)
          }
          var i = vec.size
          leaves.foreach {
            x =>
              els = els :+ (i, x.toDouble)
              i = i + 1
          }
          LabeledPoint(label = r.getAs[Int]("label").toDouble, MVecs.sparse(i, els))
      }
    val lrtest = test.rdd
      .map {
        r =>
          val vec = r.getAs[Vector]("lr_features")
          val leaves = r.getAs[mutable.WrappedArray[Float]]("predLeaf")

          var els = Seq[(Int, Double)]()
          vec.foreachActive {
            (k, v) =>
              els = els :+ (k, v)
          }
          var i = vec.size
          leaves.foreach {
            x =>
              els = els :+ (i, x.toDouble)
              i = i + 1
          }
          LabeledPoint(label = r.getAs[Int]("label").toDouble, MVecs.sparse(i, els))
      }
    val lr = lbfgs.run(lrtrain)
    lr.clearThreshold()

    /*
    val evaluator = new RegressionEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("rmse")
    val rmse = evaluator.evaluate(spark.createDataFrame(predictions))
    println("Root Mean Squared Error (RMSE) on test data = " + rmse)
    */
    val result = lrtest
      .map {
        r =>
          val p = lr.predict(r.features)
          (p, r.label)
      }
      .repartition(600)

    printXGBTestLog(result)

    val metrics = new BinaryClassificationMetrics(result)

    // AUPRC
    val auPRC = metrics.areaUnderPR

    // ROC Curve
    //val roc = metrics.roc

    // AUROC
    val auROC = metrics.areaUnderROC

    println(auPRC, auROC)
    println("auPRC=%.3f auROC=%.3f ".format(auPRC, auROC))
    // zyc
    xgbTestResults = result

    val date = new SimpleDateFormat("yyyy-MM-dd-HH-mm").format(new Date().getTime)
    model.save("/user/cpc/xgboost/xgbmodeldata/%s.xgm".format(date)) //在hdfs存xg模型
    model.booster.saveModel("/home/cpc/model_zyc/xg_model/%s.xgm".format(date)) //存xg模型

  }

  private var xgbTestResults: RDD[(Double, Double)] = null

  def printXGBTestLog(lrTestResults: RDD[(Double, Double)]): Unit = {
    val testSum = lrTestResults.count()
    if (testSum < 0) {
      throw new Exception("must run lr test first or test results is empty")
    }
    var test0 = 0 //反例数
    var test1 = 0 //正例数
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
    lrTestResults  //(p, label)
      .map {
      x =>
        val v = (x._1 * 100).toInt / 5
        ((v, x._2.toInt), 1)
    }  //  ((预测值,lable),1)
      .reduceByKey((x, y) => x + y)  //  ((预测值,lable),num)
      .map {
      x =>
        val key = x._1._1
        val label = x._1._2
        if (label == 0) {
          (key, (x._2, 0))  //  (预测值,(num1,0))
        } else {
          (key, (0, x._2))  //  (预测值,(0,num2))
        }
    }
      .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))  //  (预测值,(num1反,num2正))
      .sortByKey(false)
      .toLocalIterator
      .foreach {
        x =>
          val sum = x._2
          val pre = x._1.toDouble * 0.05
          if (pre>0.2) {
            //isUpdateModel = true
          }
          log = log + "%.2f %d %.4f %.4f %d %.4f %.4f %.4f\n".format(
            pre, //预测值
            sum._2, //正例数
            sum._2.toDouble / test1.toDouble, //该准确率下的正例数/总正例数
            sum._2.toDouble / testSum.toDouble, //该准确率下的正例数/总数
            sum._1,  //反例数
            sum._1.toDouble / test0.toDouble, //该准确率下的反例数/总反例数
            sum._1.toDouble / testSum.toDouble, //该准确率下的反例数/总数
            sum._2.toDouble / (sum._1 + sum._2).toDouble)  // 真实值
      }

    println(log)
  }

  def getLimitedData(limitedNum: Double, ulog: DataFrame): DataFrame = {
    var rate = 1d
    val num = ulog.count().toDouble

    if (num > limitedNum) {
      rate = limitedNum / num
    }

    ulog.randomSplit(Array(rate, 1 - rate), new Date().getTime)(0)
  }

  var dict = mutable.Map[String, Map[Int, Int]]()
  val dictNames = Seq(
    "mediaid",
    "planid",
    "unitid",
    "ideaid",
    "slotid",
    "adclass",
    "cityid"
  )
  var dictStr = mutable.Map[String, Map[String, Int]]()

  def initFeatureDict(spark: SparkSession, pathSep: Seq[String]): Unit = {

    for (name <- dictNames) {
      val pathTpl = "/user/cpc/lrmodel/feature_ids_v1/%s/{%s}"
      var n = 0
      val ids = mutable.Map[Int, Int]()
      println(pathTpl.format(name, pathSep.mkString(",")))
      spark.read
        .parquet(pathTpl.format(name, pathSep.mkString(",")))
        .rdd
        .map(x => x.getInt(0))
        .distinct()
        .sortBy(x => x)
        .toLocalIterator
        .foreach {
          id =>
            n += 1
            ids.update(id, n)
        }
      dict.update(name, ids.toMap)
      println("dict", name, ids.size)
    }
  }

  def intFreqTransform(src: DataFrame, in: String, out: String): DataFrame = {
    var n = 0
    var freq = Seq[(Int, Int)]()
    src.rdd.map(r => (r.getAs[Int](in), 1))
      .reduceByKey(_ + _)
      .sortBy(x => x._2)
      .toLocalIterator
      .foreach {
        x =>
          n = n + 1
          freq = freq :+ (x._1, n)
      }

    println(in, out, n)
    val dict = spark.createDataFrame(freq)
      .toDF(in, out)

    dict.write.mode(SaveMode.Overwrite).parquet("/user/cpc/xgboost_dict/%s-%s".format(in, out))
    src.join(dict, Seq(in), "left_outer")
  }

  def stringFreqTransform(src: DataFrame, in: String, out: String): DataFrame = {
    var n = 0
    var freq = Seq[(String, Int)]()
    src.rdd.map(r => (r.getAs[String](in), 1))
      .reduceByKey(_ + _)
      .sortBy(x => x._2)
      .toLocalIterator
      .foreach {
        x =>
          n = n + 1
          freq = freq :+ (x._1, n)
      }

    println(in, out, n)
    val dict = spark.createDataFrame(freq)
      .toDF(in, out)

    dict.write.mode(SaveMode.Overwrite).parquet("/user/cpc/xgboost_dict/%s-%s".format(in, out))
    src.join(dict, Seq(in), "left_outer")
  }

  def getVectorParser2(x: Row): Vector = {
    val cal = Calendar.getInstance()
    cal.setTimeInMillis(x.getAs[Int]("timestamp") * 1000L)
    val week = cal.get(Calendar.DAY_OF_WEEK) //1 to 7
    val hour = cal.get(Calendar.HOUR_OF_DAY)
    var els = Seq[Double]()

    els = els :+ week.toDouble
    els = els :+ hour.toDouble
    els = els :+ x.getAs[Int]("sex").toDouble
    els = els :+ x.getAs[Int]("age").toDouble
    els = els :+ x.getAs[Int]("os").toDouble
    els = els :+ x.getAs[Int]("isp").toDouble
    els = els :+ x.getAs[Int]("network").toDouble
    //    els = els :+ x.getAs[Int]("city")  //1111
    els = els :+ x.getAs[Int]("citydx").toDouble  //1111


    //    els = els :+ x.getAs[String]("media_appsid").toInt
    //    els = els :+ x.getAs[String]("adslotid").toInt  //1111
    els = els :+ x.getAs[Int]("adslotidx").toDouble   //1111
    els = els :+ x.getAs[Int]("phone_level").toDouble
    els = els :+ x.getAs[Int]("pagenum").toDouble

    try {
      els = els :+ x.getAs[String]("bookid").toDouble
    } catch {
      case e: Exception =>
        els = els :+ 0d
    }

    //    els = els :+ x.getAs[Int]("adclass")  //1111
    els = els :+ x.getAs[Int]("adclassdx").toDouble  //1111
    els = els :+ x.getAs[Int]("adtype").toDouble
    els = els :+ x.getAs[Int]("adslot_type").toDouble
    //    els = els :+ x.getAs[Int]("planid")  //1111
    els = els :+ x.getAs[Int]("plandx").toDouble  //1111
    //    els = els :+ x.getAs[Int]("unitid")  //1111
    els = els :+ x.getAs[Int]("unitdx").toDouble  //1111
    //    els = els :+ x.getAs[Int]("ideaid")  //1111
    els = els :+ x.getAs[Int]("ideadx").toDouble  //1111
    els = els :+ x.getAs[Int]("user_req_ad_num").toDouble
    els = els :+ x.getAs[Int]("user_req_num").toDouble
    els = els :+ x.getAs[Int]("user_click_num").toDouble
    els = els :+ x.getAs[Int]("user_click_unit_num").toDouble

    Vectors.dense(els.toArray)
  }

  def parseOneHotVector(x: Row): Vector = {

    val cal = Calendar.getInstance()
    cal.setTimeInMillis(x.getAs[Int]("timestamp") * 1000L)
    val week = cal.get(Calendar.DAY_OF_WEEK)   //1 to 7
    val hour = cal.get(Calendar.HOUR_OF_DAY)
    var els = Seq[(Int, Double)]()
    var i = 0

    els = els :+ (week + i - 1, 1d)
    i += 7

    //(24)
    els = els :+ (hour + i, 1d)
    i += 24

    //sex
    els = els :+ (x.getAs[Int]("sex") + i, 1d)
    i += 9

    //age
    els = els :+ (x.getAs[Int]("age") + i, 1d)
    i += 100

    //os 96 - 97 (2)
    els = els :+ (x.getAs[Int]("os") + i, 1d)
    i += 10

    //isp
    els = els :+ (x.getAs[Int]("isp") + i, 1d)
    i += 20

    //net
    els = els :+ (x.getAs[Int]("network") + i, 1d)
    i += 10

    els = els :+ (dict("cityid").getOrElse(x.getAs[Int]("city"), 0) + i, 1d)
    i += dict("cityid").size + 1

    //ad slot id
    els = els :+ (dict("slotid").getOrElse(x.getAs[String]("adslotid").toInt, 0) + i, 1d)
    i += dict("slotid").size + 1

    //0 to 4
    els = els :+ (x.getAs[Int]("phone_level") + i, 1d)
    i += 10

    //pagenum
    var pnum = x.getAs[Int]("pagenum")
    if (pnum < 0 || pnum > 50) {
      pnum = 0
    }
    els = els :+ (pnum + i, 1d)
    i += 100

    //bookid
    var bid = 0
    try {
      bid = x.getAs[String]("bookid").toInt
    } catch {
      case e: Exception =>
    }
    if (bid < 0 || bid > 50) {
      bid = 0
    }
    els = els :+ (bid + i, 1d)
    i += 100

    //ad class
    val adcls = dict("adclass").getOrElse(x.getAs[Int]("adclass"), 0)
    els = els :+ (adcls + i, 1d)
    i += dict("adclass").size + 1

    //adtype
    els = els :+ (x.getAs[Int]("adtype") + i, 1d)
    i += 10

    //adslot_type
    els = els :+ (x.getAs[Int]("adslot_type") + i, 1d)
    i += 10

    //planid
    els = els :+ (dict("planid").getOrElse(x.getAs[Int]("planid"), 0) + i, 1d)
    i += dict("planid").size + 1

    //unitid
    els = els :+ (dict("unitid").getOrElse(x.getAs[Int]("unitid"), 0) + i, 1d)
    i += dict("unitid").size + 1

    //ideaid
    els = els :+ (dict("ideaid").getOrElse(x.getAs[Int]("ideaid"), 0) + i, 1d)
    i += dict("ideaid").size + 1

    //user_req_ad_num
    var uran_idx = 0
    val uran = x.getAs[Int]("user_req_ad_num")
    if (uran >= 1 && uran <= 10 ){
      uran_idx = uran
    }
    if (uran > 10){
      uran_idx = 11
    }
    els = els :+ (uran_idx + i, 1d)
    i += 12 + 1

    //user_req_num
    var urn_idx = 0
    val urn = x.getAs[Int]("user_req_num")
    if (urn >= 1 && urn <= 10){
      urn_idx = 1
    }else if(urn > 10 && urn <= 100){
      urn_idx = 2
    }else if(urn > 100 && urn <= 1000){
      urn_idx = 3
    }else if(urn > 1000){
      urn_idx = 4
    }
    els = els :+ (urn_idx + i, 1d)
    i += 5 + 1

    var user_click = x.getAs[Int]("user_click_num")
    if (user_click >= 5) {
      user_click = 5
    }
    els = els :+ (user_click + i, 1d)
    i += 6

    var user_click_unit = x.getAs[Int]("user_click_unit_num")
    if (user_click_unit >= 5) {
      user_click_unit = 5
    }
    els = els :+ (user_click + i, 1d)
    i += 6

    try {
      Vectors.sparse(i, els)
    } catch {
      case e: Exception =>
        throw new Exception(els.toString + " " + i.toString + " " + e.getMessage)
        null
    }
  }
}

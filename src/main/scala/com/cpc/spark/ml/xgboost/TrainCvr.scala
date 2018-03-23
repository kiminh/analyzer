package com.cpc.spark.ml.xgboost

import java.io.FileOutputStream
import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import com.cpc.spark.common.Utils
import ml.dmlc.xgboost4j.scala.spark.XGBoostEstimator
import mlmodel.mlmodel.{IRModel, Pack}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.regression.{IsotonicRegression, IsotonicRegressionModel}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import com.cpc.spark.ml.common.{Utils => MUtils}
import com.typesafe.config.ConfigFactory
import scala.util.Random

/**
  * Created by roydong on 31/01/2018.
  */
object TrainCvr {

  Logger.getRootLogger.setLevel(Level.WARN)

  val cols = Seq(
    "sex", "age", "os", "isp", "network", "city",
    "mediaid_", "adslotid_", "phone_level", "adclass",
    "pagenum", "bookid_", "adtype", "adslot_type", "planid",
    "unitid", "ideaid", "user_req_ad_num", "user_req_num"
  )

  private var ctx: SparkSession = null


  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .appName("zyc_cvr_xgboost")
      .enableHiveSupport()
      .getOrCreate()

    ctx = spark

    import spark.implicits._

    var pathSep = Seq[String]()
    val cal = Calendar.getInstance()
    for (n <- 1 to 20) {
      cal.add(Calendar.DATE, -1)
      val date = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime)
      val hour = new SimpleDateFormat("HH").format(cal.getTime)
      pathSep = pathSep :+ date
    }

    //val path = "/user/cpc/lrmodel/ctrdata_v1/{%s}/*".format(pathSep.mkString(",")) //ctrdata_v1下的东西是cpc_union_log表SaveFeatures产生的
    val path = "/user/cpc/lrmodel/cvrdata_v2/{%s}/*".format(pathSep.mkString(","))
    println(path)
    trainLog :+= path
    val srcdata = spark.read.parquet(path).coalesce(1000).cache()

    println("training qtt_cvr ----------------------------")
    trainLog :+= "training qtt_cvr -------------------------------"
//    val qttAll = srcdata.filter(x => Seq("80000001", "80000002").contains(x.getAs[String]("media_appsid")) && Seq(1, 2).contains(x.getAs[Int]("adslot_type")))
    train(spark, srcdata, "qtt_cvr")  //趣头条广告
//    train(spark, qttAll, "cvr_qtt")  //趣头条广告

//    if (isMorning()) {
//      modelClear()
//      val extAll = srcdata.filter(x => !Seq("80000001", "80000002").contains(x.getAs[String]("media_appsid")) && Seq(1, 2).contains(x.getAs[Int]("adslot_type")))
//      train(spark, qttAll, "external_cvr")  //外媒广告
//
//      modelClear()
//      val interactAll = srcdata.filter(x => x.getAs[Int]("adslot_type") == 3)  //互动广告=趣头条的互动广告 + 外媒互动广告
//      train(spark, qttAll, "interact_cvr")
//
//
//    }
    Utils.sendMail(trainLog.mkString("\n"), "xg_cvr_trainLog", Seq("rd@aiclk.com"))
    srcdata.unpersist()
  }
  def isMorning(): Boolean = {
    new SimpleDateFormat("HH").format(new Date().getTime) < "08"
  }

  def modelClear(): Unit = {
    binsLog = Seq[String]()
    irBinNum = 0
    irError = 0
    xgbTestResults = null
    irmodel = null
    isUpdateModel = false
  }
  //  private val minBinSize = 10000d
  private var binNum = 400d
  private var binsLog = Seq[String]()
  private var irBinNum = 0
  private var irError = 0d
  private var isUpdateModel = false
  private var xgbTestResults: RDD[(Double, Double)] = null
  private var trainLog = Seq[String]()
  private var irmodel: IsotonicRegressionModel = _


  def train(spark: SparkSession, srcdata: DataFrame,  destfile: String): Unit = {

    import spark.implicits._
    trainLog :+= "\n------train log--------"
    trainLog :+= "destfile = %s".format(destfile)
    val data = srcdata.map {
      r =>
        val vec = getVectorParser2(r) //解析数据
        (r.getAs[Int]("label"), vec)
    }
      .toDF("label", "features")

    val Array(tmp1, tmp2) = data.randomSplit(Array(0.9, 0.1), 123L)
    val test = getLimitedData(1e7, tmp2)
    val totalNum = data.count().toDouble
    val pnum = tmp1.filter(x => x.getAs[Int]("label") > 0).count().toDouble
//    val rate = (pnum * 10 / totalNum * 1000).toInt // 1.24% * 10000 = 124
    println(pnum, totalNum)
    trainLog :+= "xgb train: pnum=%.0f totalNum=%.0f ".format(pnum, totalNum)
//    val tmp = tmp1.filter(x => x.getAs[Int]("label") > 0 || Random.nextInt(1000) < rate) //之前正样本数可能占1/1000，可以变成占1/100
    val train = getLimitedData(2e7, tmp1)
    //val Array(train, test) = data.randomSplit(Array(0.9, 0.1), 123L)

    val params = Map(
      //"eta" -> 1f,
      //"lambda" -> 2.5
      "num_round" -> 20, //20
      //"max_delta_step" -> 4,
      "colsample_bytree" -> 0.8,
      "max_depth" -> 10, //数的最大深度。缺省值为6 ,取值范围为：[1,∞]
      "objective" -> "reg:logistic" //定义学习任务及相应的学习目标
    )

    val xgb = new XGBoostEstimator(params)
    val model = xgb.train(train)

    val predictions = model.transform(test)

    val evaluator = new RegressionEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("rmse")
    val rmse = evaluator.evaluate(predictions)
    println("Root Mean Squared Error (RMSE) on test data = " + rmse)
    trainLog :+= "Root Mean Squared Error (RMSE) on test data =%.3f ".format(rmse)
    val result = predictions.rdd
      .map {
        r =>
          val label = r.getAs[Int]("label").toDouble
          val p = r.getAs[Float]("prediction").toDouble
          (p, label)
      }

    printXGBTestLog(result)

    val metrics = new BinaryClassificationMetrics(result)

    // AUPRC
    val auPRC = metrics.areaUnderPR

    // ROC Curve
    //val roc = metrics.roc

    // AUROC
    val auROC = metrics.areaUnderROC

    println(auPRC, auROC)
    trainLog :+= "auPRC=%.3f auROC=%.3f ".format(auPRC, auROC)
    // zyc
    xgbTestResults = result

    runIr(binNum.toInt, 0.95)
    trainLog :+= binsLog.mkString("\n")

    val date = new SimpleDateFormat("yyyy-MM-dd-HH-mm").format(new Date().getTime)

    //Utils.deleteHdfs("/user/cpc/xgboost/ctr_v1") 赵翌臣调试地址
    /*  model.save("/user/cpc/xgboost/xgbmodeldata/cvr_%s.xgm".format(date)) //在hdfs存xg模型
        model.booster.saveModel("/home/cpc/model_zyc/xg_model/cvr_%s.xgm".format(date)) //存xg模型
        val xgbfilepath = "/home/cpc/model_zyc/xg_model/cvr_%s.xgmpb".format(date) // 存xg模型的pb
        savePbPack(xgbfilepath)
        irmodel.save(ctx.sparkContext, "/user/cpc/xgboost/xgbmodeldata/cvr_%s.ir".format(date)) // 存IR模型的pb
        Utils.sendMail(trainLog.mkString("\n"), "zyc_cvr_TrainLog", Seq("zhaoyichen@aiclk.com")) //Utils.sendMail(trainLog.mkString("\n"), "zyc_TrainLog", Seq("rd@aiclk.com"))
    */
    val hdfsPath ="/user/cpc/xgboost/xgbmodeldata/cvr_%s.xgm".format(date)
    model.save(hdfsPath) //在hdfs存xg模型
    trainLog :+= "hdfsPath: " + hdfsPath

    val xgmodelfilepath = "/home/cpc/anal/xgmodel/cvr_%s.xgm".format(date) // 存xg模型
    model.booster.saveModel(xgmodelfilepath)
    trainLog :+= "xgmodelfilepath: " + xgmodelfilepath

    val xgbfilePBpath = "/home/cpc/anal/xgmodel/cvr_%s.xgmpb".format(date) // 存xg模型的pb
    savePbPack(xgbfilePBpath)
    trainLog :+= "xgbfilePBpath: " + xgbfilePBpath

    val irPath = "/user/cpc/xgboost/xgbmodeldata/cvr_%s.ir".format(date)
    irmodel.save(ctx.sparkContext, irPath) // 存IR模型的pb
    trainLog :+= "irPath: " + irPath

    trainLog :+= "\n-------update server data------"
    if (isUpdateModel) {
      println("update model~~~~~~~~~~~~~~~~~~~~~~")
      trainLog :+= "\n-------update model------"
      trainLog :+= MUtils.updateMlcppOnlineData(xgmodelfilepath, destfile + ".gbm", ConfigFactory.load())
      trainLog :+= MUtils.updateMlcppOnlineData(xgbfilePBpath, destfile + ".mlm", ConfigFactory.load())
    }else {
      println("not update model~~~~~~~~~~~~~~~~~~~~~~")
    }
  }


  //限制总的样本数
  def getLimitedData(limitedNum: Double, ulog: DataFrame): DataFrame = {
    var rate = 1d
    val num = ulog.count().toDouble

    if (num > limitedNum) {
      rate = limitedNum / num
    }

    ulog.randomSplit(Array(rate, 1 - rate), new Date().getTime)(0).coalesce(1000)
  }

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
            isUpdateModel = true
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
    trainLog :+= log
  }

  def getVectorParser2(x: Row): Vector = {
    val cal = Calendar.getInstance()
    cal.setTimeInMillis(x.getAs[Int]("timestamp") * 1000L)
    val week = cal.get(Calendar.DAY_OF_WEEK) //1 to 7
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


  def runIr(binNum: Int, rate: Double): Double = {
    irBinNum = binNum
    val sample = xgbTestResults.randomSplit(Array(rate, 1 - rate), seed = new Date().getTime)
    val bins = binData(sample(0), irBinNum)
    val sc = ctx.sparkContext
    val ir = new IsotonicRegression().setIsotonic(true).run(sc.parallelize(bins.map(x => (x._1, x._3, 1d))))    // 真实值y，预测均值x
    val sum = sample(1)
      .map(x => (x._2, ir.predict(x._1))) //(click, calibrate ctr)
      .reduce((x, y) => (x._1 + y._1, x._2 + y._2))

    irError = (sum._2 - sum._1) / sum._1 //误差比
    irmodel = ir
    irError
  }

  private def binData(sample: RDD[(Double, Double)], binNum: Int): Seq[(Double, Double, Double, Double)] = {
    val binSize = sample.count().toInt / binNum  //每个桶的容量
    var bins = Seq[(Double, Double, Double, Double)]()
    var click = 0d  //正例数
    var pv = 0d  //总数或展示数
    var pSum = 0d  //预测值的累加
    var pMin = 1d  // 最小的预测值
    var pMax = 0d  // 最大的预测值
    var n = 0  //控制打印
    sample.sortByKey()  //(p, label)按照XGB预测值升序排序
      .toLocalIterator
      .foreach {
        x =>
          pSum = pSum + x._1
          if (x._1 < pMin) {
            pMin = x._1
          }
          if (x._1 > pMax) {
            pMax = x._1
          }
          if (x._2 > 0.01) {
            click = click + 1
          }
          pv = pv + 1
          if (pv >= binSize) {  //如果超过通的容量，就换下一个桶
            val ctr = click / pv    //  点击/展示
            bins = bins :+ (ctr, pMin, pSum / pv, pMax)  // 真实值，最小值，预测均值，最大值
            n = n + 1
            //if (n < 50 || n > binNum - 50) {
            val logStr2 = "bin %d: %.6f(%d/%d) %.6f %.6f %.6f".format(
              n, ctr, click.toInt, pv.toInt, pMin, pSum / pv, pMax) //桶号：真实ctr（点击/展示），最小值，预测均值，最大值
            println(logStr2)

            if (n > binNum - 20) {
              val logStr = "bin %d: %.6f(%d/%d) %.6f %.6f %.6f".format(
                n, ctr, click.toInt, pv.toInt, pMin, pSum / pv, pMax)

              binsLog = binsLog :+ logStr
              println(logStr)
            }

            click = 0d
            pv = 0d
            pSum = 0d
            pMin = 1d
            pMax = 0d
          }
      }
    bins
  }


  def savePbPack(path: String): Unit = {
    println("irmodel.boundaries:" + irmodel.boundaries.toSeq)
    println("irmodel.predictions:" + irmodel.predictions.toSeq)
    val ir = IRModel(
      boundaries = irmodel.boundaries.toSeq,
      predictions = irmodel.predictions.toSeq,
      meanSquareError = irError * irError
    )
    val pack = Pack(
      createTime = new Date().getTime,
      ir = Option(ir)

    )
    pack.writeTo(new FileOutputStream(path))
  }
}




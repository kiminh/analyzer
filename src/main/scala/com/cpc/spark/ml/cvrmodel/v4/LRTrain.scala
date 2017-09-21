package com.cpc.spark.ml.cvrmodel.v4

import java.sql.{DriverManager, ResultSet}
import java.text.SimpleDateFormat

import scala.collection.JavaConversions._
import java.util.{Calendar, Date, Properties}

import com.cpc.spark.common.{Utils => CUtils}
import com.cpc.spark.ml.common.{FeatureDict, Utils}
import com.cpc.spark.ml.train.LRIRModel
import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.MLUtils

import scala.collection.mutable
import scala.util.Random

/**
  * Created by roydong on 06/07/2017.
  */
object LRTrain {

  def main(args: Array[String]): Unit = {
    if (args.length < 9) {
      System.err.println(
        s"""
           |Usage: CtrModel <mode:train/test[+ir]>
           |  <svmPath:string> <dayBefore:int> <day:int>
           |  <modelPath:string> <sampleRate:float> <PNRate:int>
           |  <IRBinNum:int> <lrfile:string> <irfile:string>
        """.stripMargin)
      System.exit(1)
    }

    Logger.getRootLogger.setLevel(Level.WARN)
    val mode = args(0).trim
    val inpath = args(1).trim
    val daybefore = args(2).toInt
    val days = args(3).toInt
    val modelPath = args(4).trim
    val sampleRate = args(5).toFloat
    val pnRate = args(6).split("/").map(_.toInt)
    val binNum = args(7).toInt
    val lrfile = args(8)
    val irfile = args(9)
    val adclass = 0 //args(10).toInt

    val model = new LRIRModel
    val ctx = model.initSpark("cpc cvr model %s [%s]".format(mode, modelPath))

    val fmt = new SimpleDateFormat("yyyy-MM-dd")
    val date = new SimpleDateFormat("yyyy-MM-dd-HH").format(new Date().getTime)
    val yesterday = fmt.format(new Date().getTime - 3600L * 24000L)
    val cal = Calendar.getInstance()
    cal.add(Calendar.DATE, -daybefore)

    var pathSep = Seq[String]()
    for (n <- 1 to days) {
      val date = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime)
      pathSep = pathSep :+ date
      cal.add(Calendar.DATE, 1)
    }
    println("%s/{%s}".format(inpath, pathSep.mkString(",")))
    val admap = getTitleDict()
    println(admap.size)
    admap.toSeq.take(10).foreach(println)

    val badmap = ctx.sparkContext.broadcast(admap)
    val rawData = MLUtils.loadLibSVMFile(ctx.sparkContext, "%s/{%s}".format(inpath, pathSep.mkString(",")))
    val totalNum = rawData.count()
    val svm = rawData.coalesce(totalNum.toInt / 50000)
      .mapPartitions {
        p =>
          p.map {
            x =>
              var els = Seq[(Int, Double)]()
              val admap = badmap.value
              var id = 0
              x.features.foreachActive {
                (i, v) =>
                  if (i < 3595) {
                    els = els :+ (i, v)
                  } else if (i == 3595) {
                    id = v.toInt
                  }
              }

              //val idx = adidIndex(id, 3595)
              //els = els :+ (idx, 1d)

              val ad = admap.getOrElse(id, null)
              if (ad != null) {
                els = els :+ (3595 + ad.turl, 1d)
                els = els :+ (3595 + 15000 + ad.adid, 1d)

                val v = Vectors.sparse(3595 + 15000 + 80000, els)
                LabeledPoint(x.label, v)
              } else {
                null
              }
          }
          .filter(_ != null)
      }
      .randomSplit(Array(sampleRate, 1 - sampleRate), seed = new Date().getTime)
    val testSample = svm(1)
    if (mode.startsWith("test")) {
      model.loadLRmodel(modelPath)
    } else {
      val sample = svm(0)
        .filter(x => x.label > 0.01 || Random.nextInt(pnRate(1)) < pnRate(0))
        .cache()

      println("sample count", sample.count(), sample.partitions.length)
      sample
        .map {
          x =>
            var label = 0
            if (x.label > 0.01) {
              label = 1
            }
            (label, 1)
        }
        .reduceByKey((x, y) => x + y)
        .toLocalIterator
        .foreach(println)

      sample.take(1).foreach(x => println(x.features))
      println("training...")
      model.run(sample, 0, 1e-8)
      model.saveHdfs(modelPath + "/" + date)
      sample.unpersist()
      println("done")
    }

    println("testing...")
    model.test(testSample)
    model.printLrTestLog()
    val lrTestLog = model.getLrTestLog()
    println("done")
    var updateOnlineData = 0
    val lrfilepath = "/data/cpc/anal/model/cvr_logistic_%d_%s.txt".format(adclass, date)
    if (mode.startsWith("train")) {
      model.saveText(lrfilepath)
      //满足条件的模型直接替换线上数据
      if (lrfile.length > 0 && model.getAuPRC() > 0.1 && model.getAuROC() > 0.7) {
        updateOnlineData += 1
      }
    }

    var irError = 0d
    val irfilepath = "/data/cpc/anal/model/cvr_isotonic_%d_%s.txt".format(adclass, date)
    if (mode.endsWith("+ir")) {
      println("start isotonic regression")
      irError = model.runIr(binNum, 0.9)
      model.saveIrHdfs(modelPath + "/" + date + "_ir")
      model.saveIrText(irfilepath)
      if (irfile.length > 0 && math.abs(irError) < 0.1) {
        updateOnlineData += 1
      }
    }

    val irBinsLog = model.binsLog.mkString("\n")
    val conf = ConfigFactory.load()
    var result = "failed"
    var nodes = ""
    if (updateOnlineData == 2) {
      println("replace online data")
      nodes = Utils.updateOnlineData(lrfilepath, lrfile, conf)
      Utils.updateOnlineData(irfilepath, irfile, conf)
      result = "success"
    }
    val txt =
      """
        |
        |date: %s
        |adclass: %d
        |LRfile: %s
        |auPRC: %.6f  need > 0.1
        |auROC: %.6f  need > 0.7
        |IRError: %.6f need < |0.1|
        |
        |===========================
        |%s
        |
        |===========================
        |%s
        |
        |===========================
        |%s
        |
        """.stripMargin.format(date, adclass, lrfilepath, model.getAuPRC(), model.getAuROC(),
        irError, lrTestLog, irBinsLog, nodes)

    CUtils.sendMail(txt, "CVR model train " + result, Seq("cpc-rd@innotechx.com"))

    println("all done")
    model.stopSpark()
  }

  def getAdDbResult(confKey: String): ResultSet = {
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
    stmt.executeQuery("select id, user_id, plan_id, target_url from idea where action_type = 1")
  }

  def getTitleDict(): Map[Int, AdInfo] = {
    var rows = Seq[(Int, Int, Int, String)]()
    var adids = Seq[Int]()
    var userids = Seq[Int]()
    var planids = Seq[Int]()
    var urls = Seq[String]()
    var result = getAdDbResult("mariadb.adv")
    while (result.next()) {
      val row = (result.getInt("id"), result.getInt("user_id"), result.getInt("plan_id"), result.getString("target_url"))
      rows = rows :+ row
      adids :+= row._1
      userids :+= row._2
      planids :+= row._3
      urls :+= row._4
    }
    result = getAdDbResult("mariadb.adv_old")
    while (result.next()) {
      val row = (result.getInt("id"), result.getInt("user_id"), result.getInt("plan_id"), result.getString("target_url"))
      rows = rows :+ row
      adids :+= row._1
      userids :+= row._2
      planids :+= row._3
      urls :+= row._4
    }

    var n = 0
    val adidmap = mutable.Map[Int, Int]()
    adids.distinct.foreach{
      x =>
        n += 1
        adidmap.update(x, n)
    }

    println("adid", adidmap.size, n)

    n = 0
    val useridmap = mutable.Map[Int, Int]()
    userids.distinct.foreach{
      x =>
        n += 1
        useridmap.update(x, n)
    }
    println("userid", useridmap.size, n)

    n = 0
    val planidmap = mutable.Map[Int, Int]()
    planids.distinct.foreach{
      x =>
        n += 1
        planidmap.update(x, n)
    }
    println("planid", planidmap.size, n)

    n = 0
    val urlmap = mutable.Map[String, Int]()
    urls.distinct.foreach{
      x =>
        n += 1
        urlmap.update(x, n)
    }
    println("url", urlmap.size, n)

    val admap = mutable.Map[Int, AdInfo]()
    rows
      .foreach {
        x =>
          val ad = AdInfo(
            adid = adidmap.getOrElse(x._1, 0),
            userid = useridmap.getOrElse(x._2, 0),
            planid = planidmap.getOrElse(x._3, 0),
            turl = urlmap.getOrElse(x._4, 0)
          )

          admap.update(x._1, ad)
      }

    admap.toMap
  }

  case class AdInfo(adid: Int, userid: Int, planid: Int, turl: Int) {

  }
}

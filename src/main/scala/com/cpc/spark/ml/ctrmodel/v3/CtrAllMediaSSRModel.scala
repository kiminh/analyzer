package com.cpc.spark.ml.ctrmodel.v3

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import com.cpc.spark.common.{Utils => CUtils}
import com.cpc.spark.ml.common.Utils
import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.rdd.RDD
import com.cpc.spark.ml.train.LRIRModel
import scala.util.Random

/**
  * Created by zhaolei on 14/11/2017.
  * 全媒体模型;
  * 训练集:合并正负样本1:10比例的列表页和详情页,
  */
object CtrAllMediaSSRModel {

  def main(args: Array[String]): Unit = {
    if (args.length < 12) {
      System.err.println(
        s"""
           |Usage: CtrModel <mode:train/test[+ir]>
           |  <svmPath:string> <dayBefore:int> <day:int>
           |  <modelPath:string> <sampleRate:float> <PNRate:int>
           |  <IRBinNum:int> <LRFile:string> <IRFile:string>
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
    val pnRate = args(6).toInt
    val binNum = args(7).toInt
    val lrfile = args(8)
    val irfile = args(9)
    val inpathList = args(10)     //列表页数据路径
    val inpathDetail = args(11)   //详情页数据路径

    val model = new LRIRModel
    val ctx = model.initSpark("cpc ctr model %s [%s]".format(mode, modelPath))

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

    println("%s/{%s}".format(inpathList, pathSep.mkString(",")))
    println("%s/{%s}".format(inpathDetail, pathSep.mkString(",")))

    var testSample: RDD[LabeledPoint] = null
    if (mode.startsWith("test")) {
      testSample = MLUtils.loadLibSVMFile(ctx.sparkContext, "%s/{%s}".format(inpath, pathSep.mkString(",")))
        .coalesce(2000)
      model.loadLRmodel(modelPath)
    } else {

      var rawData : RDD[LabeledPoint] = null

      var rawDataList : RDD[LabeledPoint] = null
      var rawDataDetail : RDD[LabeledPoint] = null

      //前3天-前1天,取全量数据
      rawDataList = MLUtils.loadLibSVMFile(ctx.sparkContext,"%s/{%s}".format(inpathList, pathSep.takeRight(3).mkString(",")))
      rawDataDetail = MLUtils.loadLibSVMFile(ctx.sparkContext,"%s/{%s}".format(inpathDetail, pathSep.takeRight(3).mkString(",")))

      statistic_info("rawDataList",rawDataList,pathSep.takeRight(3).mkString(","))
      statistic_info("rawDataDetail",rawDataDetail,pathSep.takeRight(3).mkString(",") )

      //前10天-前7天,分别取1/10,2/10,...,7/10的训练数据
      var num = 1
      pathSep.take(7).foreach{
        pathSeqDay =>
          val dayDataList = MLUtils.loadLibSVMFile(ctx.sparkContext,"%s/{%s}".format(inpathList, pathSeqDay))
            .filter(x => x.label > 0.01 || Random.nextInt(pnRate) == 0)
            .randomSplit(Array( num/10.0, 1 - num/10.0 ), seed = new Date().getTime)

          rawDataList = rawDataList.union(dayDataList(0))

          val dayDataDetail = MLUtils.loadLibSVMFile(ctx.sparkContext,"%s/{%s}".format(inpathDetail, pathSeqDay))
            .filter(x => x.label > 0.01 || Random.nextInt(pnRate) == 0)
            .randomSplit(Array( num/10.0, 1 - num/10.0 ), seed = new Date().getTime)

          rawDataDetail = rawDataDetail.union(dayDataDetail(0))

          num += 1

          statistic_info("rawDataList",rawDataList,pathSeqDay)
          statistic_info("rawDataDetail",rawDataDetail,pathSeqDay)
      }

      rawData = rawDataList.union(rawDataDetail)

      val totalNum = rawData.count()
      val svm = rawData.coalesce(totalNum.toInt / 20000)
        //random pick 1/pnRate negative sample
        .filter(x => x.label > 0.01 || Random.nextInt(pnRate) == 0)
        .randomSplit(Array(sampleRate, 1 - sampleRate), seed = new Date().getTime)

      val sample = svm(0).cache()
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

    if (testSample == null) {
      testSample = MLUtils.loadLibSVMFile(ctx.sparkContext, "%s_full/%s".format(inpathList, yesterday))
        .union(MLUtils.loadLibSVMFile(ctx.sparkContext, "%s_full/%s".format(inpathDetail, yesterday))).coalesce(2000)
    }
    println("testing...")
    model.test(testSample)
    val lrTestLog = model.getLrTestLog()
    println(lrTestLog)
    println("done")

    var updateOnlineData = 0
    val lrfilepath = "/data/cpc/anal/model/logistic_all_media_same_sample_rate_%s.txt".format(date)
    if (mode.startsWith("train")) {
      model.saveText(lrfilepath)

      //满足条件的模型直接替换线上数据
      if (lrfile.length > 0 && model.getAuPRC() > 0.07 && model.getAuROC() > 0.80) {
        updateOnlineData += 1
      }
    }

    var irError = 0d
    val irfilepath = "/data/cpc/anal/model/iogistic_all_media_same_sample_rate_%s.txt".format(date)
    if (mode.endsWith("+ir")) {
      println("start isotonic regression")
      irError = model.runIr(binNum, 0.9)
      model.saveIrHdfs(modelPath + "/" + date + "_ir")
      model.saveIrText(irfilepath)
      if (irfile.length > 0 && math.abs(irError) < 0.01) {
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
        |date: %s
        |LRfile: %s
        |auPRC: %.6f need > 0.07
        |auROC: %.6f need > 0.80
        |IRError: %.6f need < |0.01|
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
        """.stripMargin.format(date, lrfilepath, model.getAuPRC(), model.getAuROC(), irError, lrTestLog, irBinsLog, nodes)
    CUtils.sendMail(txt, "CTR all media model(list: 1:10; detail: 1:10) train " + result, Seq("cpc-rd@innotechx.com","rd@aiclk.com"))

    println("all done")
    model.stopSpark()
  }

  def statistic_info(rawDataName : String, rawData : RDD[LabeledPoint], pathSepInfo : String): Unit ={
    val tmp = rawData.cache()
    tmp.map {
      x =>
        var label = 0
        if (x.label > 0.01) {
          label = 1
        }
        (label, 1)
    }
      .reduceByKey((x, y) => x + y)
      .toLocalIterator
      .foreach( x => println(rawDataName + " " + pathSepInfo + " " + x))

    tmp.unpersist()
  }
}



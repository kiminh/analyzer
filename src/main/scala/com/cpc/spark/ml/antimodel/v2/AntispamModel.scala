package com.cpc.spark.ml.antimodel.v2

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SaveMode

/**
  * Created by roydong on 06/07/2017.
  */
object AntispamModel {

  def main(args: Array[String]): Unit = {
    if (args.length < 3) {
      System.err.println(
        s"""
           |Usage: CtrModel <mode:train/test>
           |  <svmPath:string> <dayBefore:int> <day:int>
           |  <modelPath:string> <sampleRate:float> <PNRate:int>
        """.stripMargin)
      System.exit(1)
    }
    Logger.getRootLogger.setLevel(Level.WARN)
    // "train+ir" \
   // "/user/cpc/svmdata/v5" 10 10 \
   //   "/user/cpc/model/v5" \
    //  1 1 5000 "20170801.logistic" "20170801.isotonic"

    Logger.getRootLogger.setLevel(Level.WARN)
    val mode = args(0).trim
    val daybefore = args(1).toInt
    val pnRate = args(2).toInt

    val cal = Calendar.getInstance()
    cal.add(Calendar.DATE, -daybefore)

    val toDate = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime)

    val model = new LRIRModel
    val ctx = model.initSpark("cpc antispam model v2")

    println("pathSep/user/cpc/antispam/v2/svm/train/%s".format(toDate))

    var testSample:RDD[(String, LabeledPoint)]= null
    if (mode.startsWith("test")) {
      testSample = MyMLUtils.loadLibSVMFile(ctx.sparkContext, "/user/cpc/antispam/v2/svm/test/%s".format(toDate))
      model.loadLRmodel("/user/cpc/antispam/v2/model/%s".format(toDate))
    } else {
      val svm = MyMLUtils.loadLibSVMFile(ctx.sparkContext, "/user/cpc/antispam/v2/svm/train/%s".format(toDate))
        //random pick 1/pnRate negative sample
      //  .filter(x => x.label > 0.01 || Random.nextInt(pnRate) == 0)
        .randomSplit(Array(1, 0), seed = new Date().getTime)
      val sample = svm(0).cache()
      println("sample count", sample.count())
      sample
        .map {
          x =>
            var label = 0
            if (x._2.label > 0.01) {
              label = 1
            }
            (label, 1)
        }
        .reduceByKey((x, y) => x + y)
        .toLocalIterator
        .foreach(println)

      println("training...")
      model.run(sample.map(x => x._2), 0, 0)
      model.saveHdfs("/user/cpc/antispam/v2/model/" + toDate)
      sample.unpersist()
      println("done")
    }
    if (testSample == null) {
      println("/user/cpc/antispam/v2/svm/test/%s".format(toDate))
      testSample = MyMLUtils.loadLibSVMFile(ctx.sparkContext, "/user/cpc/antispam/v2/svm/test/%s".format(toDate))
    }
    println("testing...")
    model.test(testSample.map(x => x._2))
    model.printLrTestLog()
    println("done")
    println("exprot uid ...")
    import ctx.implicits._
    model.test2(testSample).map{
      case (uid,predict, lable )=>
      uid + " " + predict + " " + lable
    }.toDF()
    .write
    .mode(SaveMode.Overwrite)
    .text("/user/cpc/antispam/v2/device/" + toDate)
    println("done")
    model.stopSpark()
  }
}



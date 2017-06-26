package com.cpc.spark.ml.train

import java.io.PrintWriter

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.classification.LogisticRegressionModel
import org.apache.spark.sql.SparkSession

/**
  * Created by roydong on 13/06/2017.
  */
object GetModelInfo {

  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      System.err.println(s"""
                            |Usage: <model_data_path>
        """.stripMargin)
      System.exit(1)
    }
    Logger.getRootLogger().setLevel(Level.WARN)

    val dataPath = args(0).trim
    val ctx = SparkSession.builder()
      .appName("get weights")
      .getOrCreate()
    val spark = ctx.sparkContext

    val model = LogisticRegressionModel.load(spark, dataPath)
    model.clearThreshold()

    val w = new PrintWriter("/home/cpc/t/bin/model.txt")
    w.write("version 0.1\n")
    w.write("model_path %s\n".format(dataPath))
    w.write("num_features %d\n".format(model.numFeatures - 1))
    w.write("num_classes %d\n".format(model.numClasses))
    model.weights.toSparse.foreachActive {
      (i, v) =>
        w.write("%d %.10f\n".format(i, v))
    }
    w.close()
    println("done")
  }
}

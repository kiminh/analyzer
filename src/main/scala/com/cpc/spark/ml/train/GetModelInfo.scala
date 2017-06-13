package com.cpc.spark.ml.train

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
    println("model data loaded", model.toString())

    model.weights.toSparse.foreachActive {
      (i, v) =>
        println("%d %f".format(i, v))
    }

  }
}

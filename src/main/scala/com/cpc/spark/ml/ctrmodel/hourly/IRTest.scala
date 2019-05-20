package com.cpc.spark.ml.ctrmodel.hourly

import com.cpc.spark.ml.train.LRIRModel
import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.sql.SparkSession

/**
  * @author fym
  * @version created: 2019-05-15 17:47
  * @desc
  */
class IRTest {
  private var trainLog = Seq[String]()
  private val model = new LRIRModel

  def main(args: Array[String]): Unit = {

    Logger.getRootLogger.setLevel(Level.WARN)

    val spark: SparkSession = model
      .initSpark("[cpc-model] ir-model test")

    // model
    model.loadIRModel("hdfs://emr-cluster/user/cpc/lrmodel/irmodeldata/2019-05-15-16-53")

    // generate feature vector manually.coh

    println("-- prediction result: %s --"
      .format(
        model
          .getIRmodel()
          .predict(args(0).toDouble)
      )
    )

  }
}

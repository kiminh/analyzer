package com.cpc.spark.ml.stub

import com.typesafe.config.ConfigFactory
import io.grpc.ManagedChannelBuilder
import mlserver.mlserver._
import org.apache.spark.SparkContext
import org.apache.spark.mllib.feature.Normalizer
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.MLUtils

/**
  * Created by roydong on 2017/5/11.
  */
object Stub {

  def main(args: Array[String]): Unit = {
    val conf = ConfigFactory.load()
    val channel = ManagedChannelBuilder
      .forAddress("0.0.0.0", conf.getInt("mlserver.port"))
      .usePlaintext(true)
      .build

    val req = Request(
      media = Option(MediaInfo(ip = "180.169.79.250")),
      ads = Seq(AdInfo(ideaid = 1), AdInfo(ideaid = 3))
    )

    val sc = new SparkContext();
    var parsedData = MLUtils.loadLibSVMFile(sc, args(0))


    val normalizer1 = new Normalizer()
    parsedData = parsedData.map{ x=>
      new LabeledPoint(x.label, normalizer1.transform(x.features))
    }

    val splits = parsedData.randomSplit(Array(0.6,0.4), seed = 12L)

    val blockingStub = PredictorGrpc.blockingStub(channel)
    val reply = blockingStub.predict(req)
    println(req.toString, reply.toString)
  }

}

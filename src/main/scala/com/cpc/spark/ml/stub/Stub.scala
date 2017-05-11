package com.cpc.spark.ml.stub

import com.typesafe.config.ConfigFactory
import io.grpc.ManagedChannelBuilder
import mlserver.server._

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

    val request = Request(ads = Seq(AdInfo(adslotid = 12), AdInfo(adslotid = 33)))
    val blockingStub = PredictorGrpc.blockingStub(channel)
    val reply = blockingStub.predict(request)
    println(reply.toString)

  }

}

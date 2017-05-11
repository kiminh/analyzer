package com.cpc.spark.ml.stub

import com.typesafe.config.ConfigFactory
import io.grpc.ManagedChannelBuilder
import mlserver.server.{PredictorGrpc, Request, Response}

/**
  * Created by roydong on 2017/5/11.
  */
object Stub {

  def main(args: Array[String]): Unit = {
    val conf = ConfigFactory.load()
    val channel = ManagedChannelBuilder
      .forAddress("127.0.0.1", conf.getInt("mlserver.port"))
      .usePlaintext(true)
      .build

    val request = Request()
    val blockingStub = PredictorGrpc.blockingStub(channel)
    val reply: Response = blockingStub.predict(request)
    println(reply.toString)
  }

}

package com.cpc.spark.ml.stub

import java.util.Date

import com.cpc.spark.log.parser.{LogParser, UnionLog}
import com.typesafe.config.ConfigFactory
import io.grpc.ManagedChannelBuilder
import mlserver.mlserver._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

/**
  * Created by roydong on 2017/5/11.
  */
object Stub {

  def main(args: Array[String]): Unit = {

    val conf = ConfigFactory.load()
    val channel = ManagedChannelBuilder
      .forAddress("0.0.0.0", args(0).toInt)
      .usePlaintext(true)
      .build

    val ad = AdInfo(
      ideaid = 100
    )
    val m = Media (
    )
    val u = User(
      uid = args(2)
    )
    val d = Device(
      model = args(1)
    )

    val req = Request(media = Option(m), ads = Seq(ad), user = Option(u), device = Option(d))
    val blockingStub = PredictorGrpc.blockingStub(channel)
    val reply = blockingStub.predict(req)
    println(req.toString, reply.toString)
  }

}

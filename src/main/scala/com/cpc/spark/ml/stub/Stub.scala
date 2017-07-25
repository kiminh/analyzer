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
      .forAddress(args(0), args(1).toInt)
      .usePlaintext(true)
      .build

    val ad = AdInfo(
      ideaid = 1010
    )
    val ad1 = AdInfo(
      ideaid = 1010
    )
    val ad2 = AdInfo(
      ideaid = 1010
    )
    val ad3 = AdInfo(
      ideaid = 1010
    )
    val m = Media (
      adslotid = 1023934
    )
    val u = User(
      sex = 1,
      age = 2
    )
    val d = Device(
    )

    val req = Request(version = "v3", media = Option(m), ads = Seq(ad), user = Option(u), device = Option(d))
    val blockingStub = PredictorGrpc.blockingStub(channel)
    val reply = blockingStub.predict(req)
    println(req.toString, reply.toString)
  }

}

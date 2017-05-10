package com.cpc.spark.ml.server

import org.apache.spark.mllib.classification.LogisticRegressionModel
import org.apache.spark.mllib.linalg.Vectors
import com.typesafe.config.ConfigFactory
import io.grpc.ServerBuilder
import mlserver.server.{Prediction, PredictorGrpc, Request, Response}
import mlserver.server.PredictorGrpc.Predictor
import org.apache.spark.{SparkConf, SparkContext}
import scala.util.hashing.MurmurHash3.stringHash

import scala.concurrent.{ExecutionContext, Future}


/**
  * Created by Roy on 2017/5/10.
  */
object MLServer {

  var model: LogisticRegressionModel = null

  var spark: SparkContext = null

  var dataPath = ""

  def main(args: Array[String]): Unit = {
    val coreNum = Runtime.getRuntime.availableProcessors()
    //System.setProperty("scala.concurrent.context.minThreads", coreNum.toString)
    System.setProperty("scala.concurrent.context.maxThreads", coreNum.toString)
    val conf = ConfigFactory.load()

    val dataPath = conf.getString("mlserver.data_path")
    val spark = new SparkContext(new SparkConf().setAppName("cpc ml server ctr predictor"))
    model = LogisticRegressionModel.load(spark, dataPath)

    val loadDataThread = new Thread(new Runnable {
      override def run(): Unit = {
        Thread.sleep(1000 * 60 * 60 * 24)
        model = LogisticRegressionModel.load(spark, dataPath)
      }
    })
    loadDataThread.start()

    val server = ServerBuilder.forPort(conf.getInt("mlserver.port"))
      .addService(PredictorGrpc.bindService(new PredictorService, ExecutionContext.global))
      .build
      .start

    println("server started listen " + conf.getString("mlserver.port"))

    sys.addShutdownHook {
      System.err.println("*** shutting down gRPC server since JVM is shutting down")
      server.shutdown()
      System.err.println("*** server shut down")
    }

    server.awaitTermination()
    loadDataThread.interrupt()
  }

  private class PredictorService extends Predictor {
    override def predict(req: Request): Future[Response] = {
      val resp = Response()
      req.ads.foreach {
        x =>
          val v = Vectors.dense(Array(
            1.0, //isclick
            x.network.toDouble,
            stringHash(x.ip).toDouble,
            x.mediaType.toDouble,
            x.mediaAppsid.toDouble,
            x.bid.toDouble,
            x.ideaid.toDouble,
            x.unitid.toDouble,
            x.planid.toDouble,
            x.userid.toDouble,
            x.country.toDouble,
            x.province.toDouble,
            x.city.toDouble,
            x.isp.toDouble,
            stringHash(x.uid).toDouble,
            x.coin.toDouble,
            stringHash(x.date).toDouble,
            x.hour.toDouble,
            x.adslotid.toDouble,
            x.adslotType.toDouble,
            x.adtype.toDouble,
            x.interaction.toDouble
          ))
          val pre = Prediction(
            adid = x.ideaid,
            value = model.predict(v)
          )
          resp.addResults(pre)
      }
      Future.successful(resp)
    }
  }

  def parse(req: Request): Unit = {

  }
}



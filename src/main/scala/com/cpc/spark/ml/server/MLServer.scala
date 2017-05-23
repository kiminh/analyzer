package com.cpc.spark.ml.server

import java.util.Date

import com.cpc.spark.ml.parser.MLParser
import org.apache.spark.mllib.classification.LogisticRegressionModel
import org.apache.spark.mllib.linalg.Vectors
import com.typesafe.config.ConfigFactory
import io.grpc.ServerBuilder
import mlserver.mlserver._
import mlserver.mlserver.PredictorGrpc.Predictor
import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.feature.Normalizer
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.hashing.MurmurHash3.stringHash
import scala.concurrent.{ExecutionContext, Future}


/**
  * Created by Roy on 2017/5/10.
  */
object MLServer {

  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      System.err.println(s"""
        |Usage: MLServer <model_data_path>
        """.stripMargin)
      System.exit(1)
    }
    Logger.getRootLogger().setLevel(Level.WARN)
    val coreNum = Runtime.getRuntime.availableProcessors()
    //System.setProperty("scala.concurrent.context.minThreads", coreNum.toString)
    System.setProperty("scala.concurrent.context.maxThreads", coreNum.toString)
    val conf = ConfigFactory.load()

    val dataPath = args(0)
    val spark = new SparkContext(new SparkConf().setAppName("cpc ml server ctr predictor"))
    val model = LogisticRegressionModel.load(spark, dataPath)
    model.clearThreshold()
    println("model data loaded", model.toString())

    val server = ServerBuilder.forPort(conf.getInt("mlserver.port"))
      .addService(PredictorGrpc.bindService(new PredictorService(model), ExecutionContext.global))
      .build
      .start

    println("server started listen " + conf.getString("mlserver.port"))

    sys.addShutdownHook {
      System.err.println("*** shutting down gRPC server since JVM is shutting down")
      server.shutdown()
      System.err.println("*** server shut down")
    }

    server.awaitTermination()
    spark.stop()
  }

  private class PredictorService(model: LogisticRegressionModel) extends Predictor {

    override def predict(req: Request): Future[Response] = {
      val st = new Date().getTime
      var resp = Response(recode = 0)
      val m = req.getMedia
      val u = req.getUser
      req.ads.foreach {
        x =>
          val v = MLParser.sparseVector(m, u, x)
          val p = Prediction(
            adid = x.ideaid,
            value = model.predict(v)
          )
          resp = resp.addResults(p)
          println(v.toArray.mkString(" "), p.value)
      }
      val et = new Date().getTime
      println("new predict %d %dms".format(req.ads.length, et - st))
      Future.successful(resp)
    }

    override def ping(request: MlPingReq): Future[MlPingResp] = {
      Future.successful(MlPingResp())
    }
  }
}



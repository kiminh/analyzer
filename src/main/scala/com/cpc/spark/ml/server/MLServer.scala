package com.cpc.spark.ml.server

import java.util.Date

import com.cpc.spark.ml.parser.FeatureParser
import org.apache.spark.mllib.classification.LogisticRegressionModel
import com.typesafe.config.ConfigFactory
import io.grpc.ServerBuilder
import mlserver.mlserver._
import mlserver.mlserver.PredictorGrpc.Predictor
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

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

    val dataPath = args(0).trim
    val ctx = SparkSession.builder()
      .appName("cpc ml server ctr predictor")
      .getOrCreate()
    val spark = ctx.sparkContext
    val conf = ConfigFactory.load()

    val model = LogisticRegressionModel.load(spark, dataPath)
    model.clearThreshold()
    println("model data loaded", model.toString())

    new Thread(new Runnable {
      override def run(): Unit = {
        FeatureParser.loadUserClk()
      }
    }).start()
    new Thread(new Runnable {
      override def run(): Unit = {
        FeatureParser.loadUserPv()
      }
    }).start()

    val service = new PredictorService(model)
    val server = ServerBuilder.forPort(conf.getInt("mlserver.port"))
      .addService(PredictorGrpc.bindService(service, ExecutionContext.global))
      .build
      .start

    println("server started listen " + conf.getString("mlserver.port"))

    sys.addShutdownHook {
      System.err.println("*** shutting down gRPC server since JVM is shutting down")
      server.shutdown()
      System.err.println("*** server shut down")
    }

    spark.stop()
    server.awaitTermination()
  }

  private class PredictorService(model: LogisticRegressionModel) extends Predictor {

    override def predict(req: Request): Future[Response] = {
      val st = new Date().getTime
      var resp = Response(recode = 0)
      val m = req.getMedia
      val u = req.getUser
      val loc = req.getLoc
      val n = req.getNetwork
      val d = req.getDevice
      req.ads.foreach {
        x =>
          val features = FeatureParser.parse(x, m, u, loc, n, d, req.time * 1000L)
          var value = 0D
          if (features != null) {
            value = model.predict(features)
          }
          val p = Prediction(
            adid = x.ideaid,
            value = value
          )
          resp = resp.addResults(p)
      }
      val et = new Date().getTime
      println("new predict %dms".format(et - st), FeatureParser.userClk.size, FeatureParser.userPV.size)
      Future.successful(resp)
    }

    override def ping(request: MlPingReq): Future[MlPingResp] = {
      Future.successful(MlPingResp())
    }
  }
}



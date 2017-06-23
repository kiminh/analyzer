package com.cpc.spark.ml.ctrmodel.v2

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import io.grpc.ServerBuilder
import mlserver.mlserver.PredictorGrpc.Predictor
import mlserver.mlserver._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.classification.LogisticRegressionModel
import org.apache.spark.sql.SparkSession

import scala.concurrent.{ExecutionContext, Future}

/**
  * Created by Roy on 2017/5/10.
  */
object Server {

  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      System.err.println(s"""
        |Usage: MLServer <port:int> <model_data_path:string>
        """.stripMargin)
      System.exit(1)
    }
    Logger.getRootLogger().setLevel(Level.WARN)
    val coreNum = Runtime.getRuntime.availableProcessors()
    //System.setProperty("scala.concurrent.context.minThreads", coreNum.toString)
    System.setProperty("scala.concurrent.context.maxThreads", coreNum.toString)

    val dataPath = args(1).trim
    val ctx = SparkSession.builder()
      .appName("cpc ml server ctr predictor")
      .getOrCreate()
    val spark = ctx.sparkContext

    val model = LogisticRegressionModel.load(spark, dataPath)
    model.clearThreshold()
    println("model data loaded", model.toString())

    val service = new PredictorService(model)
    val server = ServerBuilder.forPort(args(0).toInt)
      .addService(PredictorGrpc.bindService(service, ExecutionContext.global))
      .build
      .start

    println("server started listen " + args(0))

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
            if (req.version == "v2" && value < 0.8) {
              value = value / 2
            }
          }
          val p = Prediction(
            adid = x.ideaid,
            value = value
          )
          resp = resp.addResults(p)
      }
      val et = new Date().getTime
      val rs = resp.results.map(x => "%d=%f".format(x.adid, x.value)).mkString(",")
      println("%s %d %s [%s]".format(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(et), et - st, req.version, rs))
      Future.successful(resp)
    }

    override def ping(request: MlPingReq): Future[MlPingResp] = {
      Future.successful(MlPingResp())
    }
  }
}



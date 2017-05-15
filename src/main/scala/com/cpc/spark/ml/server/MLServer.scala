package com.cpc.spark.ml.server

import java.util.Date

import org.apache.spark.mllib.classification.LogisticRegressionModel
import org.apache.spark.mllib.linalg.Vectors
import com.typesafe.config.ConfigFactory
import io.grpc.ServerBuilder
import mlserver.mlserver._
import mlserver.mlserver.PredictorGrpc.Predictor
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.hashing.MurmurHash3.stringHash
import scala.concurrent.{ExecutionContext, Future}


/**
  * Created by Roy on 2017/5/10.
  */
object MLServer {

  var model: LogisticRegressionModel = null

  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      System.err.println(s"""
        |Usage: MLServer <model_data_path>
        """.stripMargin)
      System.exit(1)
    }
    val coreNum = Runtime.getRuntime.availableProcessors()
    //System.setProperty("scala.concurrent.context.minThreads", coreNum.toString)
    System.setProperty("scala.concurrent.context.maxThreads", coreNum.toString)
    val conf = ConfigFactory.load()

    val dataPath = args(0)
    val spark = new SparkContext(new SparkConf().setAppName("cpc ml server ctr predictor"))
    spark.setLogLevel("WARN")
    model = LogisticRegressionModel.load(spark, dataPath)
    model.clearThreshold()
    println("model data loaded", model.numFeatures, model.getThreshold)

    val loadDataThread = new Thread(new Runnable {
      override def run(): Unit = {
        Thread.sleep(1000 * 60 * 60 * 24)
        model = LogisticRegressionModel.load(spark, dataPath)
        println("model data loaded in loop")
      }
    })
    loadDataThread.start()

    val server = ServerBuilder.forPort(conf.getInt("mlserver.port"))
      .addService(PredictorGrpc.bindService(new PredictorService, ExecutionContext.global))
      .build
      .start

    println("server started listen " + conf.getString("mlserver.port"))
    spark.stop()

    sys.addShutdownHook {
      System.err.println("*** shutting down gRPC server since JVM is shutting down")
      server.shutdown()
      System.err.println("*** server shut down")
    }

    server.awaitTermination()
    loadDataThread.interrupt()
    spark.stop()
  }

  private class PredictorService extends Predictor {
    override def predict(req: Request): Future[Response] = {
      val st = new Date().getTime
      var resp = Response(recode = 0)
      val m = req.getMedia
      req.ads.foreach {
        x =>
          val v = Vectors.dense(Array(
            m.network.toDouble,
            stringHash(m.ip).toDouble,
            m.mediaType.toDouble,
            m.mediaAppsid.toDouble,
            x.bid.toDouble,
            x.ideaid.toDouble,
            x.unitid.toDouble,
            x.planid.toDouble,
            x.userid.toDouble,
            m.country.toDouble,
            m.province.toDouble,
            m.city.toDouble,
            m.isp.toDouble,
            stringHash(m.uid).toDouble,
            m.coin.toDouble,
            stringHash(m.date).toDouble,
            m.hour.toDouble,
            m.adslotid.toDouble,
            m.adslotType.toDouble,
            x.adtype.toDouble,
            x.interaction.toDouble
          ))
          println(v.toArray.mkString(" "))
          val p = Prediction(
            adid = x.ideaid,
            value = model.predict(v)
          )
          resp = resp.addResults(p)
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



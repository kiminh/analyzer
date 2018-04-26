package com.cpc.spark.ml.server

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import com.cpc.spark.ml.parser.{FeatureParser, UserClick}
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
      .enableHiveSupport()
      .getOrCreate()
    val spark = ctx.sparkContext
    val conf = ConfigFactory.load()

    val model = LogisticRegressionModel.load(spark, dataPath)
    model.clearThreshold()
    println("model data loaded", model.toString())

    val uc = new UserClick("cpc-bj05", 6381, 5)
    uc.readFromRedis()
    println("done", uc.userClk.size, uc.userSlotAdClick.size, uc.getPv())

    val service = new PredictorService(model, uc)
    val server = ServerBuilder.forPort(args(0).toInt)
      .addService(PredictorGrpc.bindService(service, ExecutionContext.global))
      .build
      .start

    println("server started listen " + args(0))

    val timer = new java.util.Timer()
    val task = new java.util.TimerTask {
      def run() = {
        val cal = Calendar.getInstance()
        cal.add(Calendar.HOUR_OF_DAY, -2)
        val Array(date, hour) = new SimpleDateFormat("yyyy-MM-dd HH").format(cal.getTime).split(" ")
        val m = cal.get(Calendar.MINUTE)
        if (m <= 40) {
          println("union log is not ready", m)
          Thread.sleep(((40 - m.toLong) * 60 * 1000))
        }
        println("start to load hour click", date, hour)
        if (hour == "04") {
          uc.readFromRedis()
        }
        val n = uc.sumUnionLog(ctx, date, hour)
        println("done", n)
      }
    }
    timer.schedule(task, 3600 * 1000L, 3600 * 1000L)


    sys.addShutdownHook {
      System.err.println("*** shutting down gRPC server since JVM is shutting down")
      server.shutdown()
      System.err.println("*** server shut down")
    }

    spark.stop()
    server.awaitTermination()
    task.cancel()
  }

  private class PredictorService(model: LogisticRegressionModel, uc: UserClick) extends Predictor {

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
          val clk = uc.userCtr(u.uid)
          val pv = uc.userPV.getOrElse(u.uid, 0)
          val ad = uc.userAdCtr(u.uid, x.ideaid)
          val slot = uc.userSlotCtr(u.uid, m.adslotid)
          val slotAd = uc.userSlotAdCtr(u.uid, m.adslotid, x.ideaid)
          val features = FeatureParser.parse(x, m, u, loc, n, d, req.time * 1000L, clk, pv, ad, slot, slotAd)

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



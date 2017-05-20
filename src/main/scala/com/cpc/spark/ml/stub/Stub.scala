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
    Logger.getRootLogger().setLevel(Level.WARN)
    val sparkSession = SparkSession.builder()
      .appName("GenerateAdvSvm v1")
      .enableHiveSupport()
      .getOrCreate()
    import sparkSession.implicits._

    val date = LogParser.dateFormat.format(new Date())
    val log = sparkSession.sql(
      s"""
         |select * from dl_cpc.cpc_union_log where `date` = "%s" and isfill = 1 and adslotid > 0 limit 100
        """.stripMargin.format(date))
      .as[UnionLog].rdd

    val conf = ConfigFactory.load()
    val channel = ManagedChannelBuilder
      .forAddress("0.0.0.0", conf.getInt("mlserver.port"))
      .usePlaintext(true)
      .build

    log.take(100)
      .foreach {
        x =>
          val ad = AdInfo(
            ideaid = x.ideaid,
            unitid = x.unitid,
            planid = x.planid,
            userid = x.userid,
            adtype = x.adtype,
            interaction = x.interaction
          )
          val m = MediaInfo(
            network = x.network,
            mediaAppsid = x.media_appsid.toInt,
            mediaType = x.media_type,
            city = x.city,
            adslotid = x.adslotid.toInt,
            adslotType = x.adslot_type,
            date = x.date,
            hour = x.hour.toInt
          )
          val req = Request(media = Option(m), ads = Seq(ad))
          val blockingStub = PredictorGrpc.blockingStub(channel)
          val reply = blockingStub.predict(req)
          println(req.toString, reply.toString)
      }
  }

}

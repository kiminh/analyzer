package com.cpc.spark.streaming

import java.text.SimpleDateFormat
import java.util.Date

import kafka.serializer.{DefaultDecoder, StringDecoder}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import com.cpc.spark.common.{Event, LogData}

object MLSnapshot {
  def main(args: Array[String]) {
    if (args.length < 3) {
      System.err.println(
        s"""
            haha
        """.stripMargin)
      System.exit(1)
    }
    Logger.getRootLogger.setLevel(Level.WARN)
    val Array(brokers, topics, seconds) = args
    println(args.mkString(" "))

    val spark = SparkSession.builder()
      .appName("ml snapshot from show log")
      .enableHiveSupport()
      .getOrCreate()

    //val sparkConf = new SparkConf().setAppName("ml snapshot: topics = " + topics)
    val ssc = new StreamingContext(spark.sparkContext.getConf, Seconds(seconds.toInt))
    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)

    val base_data = KafkaUtils
      .createDirectStream[String, Array[Byte], StringDecoder, DefaultDecoder](ssc, kafkaParams, topicsSet)
      .map{
        case (key, v) =>
          try {
            val log = Event.parse_show_log(v.toString)
            val event = log.event

            /**
            uid#planid", "uid#unitid", "uid#ideaid", "uid#adclass", "
            uid#adtype", "uid#interact", "uid#userid", "uid#slottype"
              */
            val ad = event.getAd
            val ideaid = ad.getUnitId
            val unitid = ad.getGroupId
            val planid = ad.getPlanId
            val userid = ad.getUserId
            val adclass = ad.getClass_
            val adtype = ad.getType.getNumber
            val interact = ad.getInteraction.getNumber
            val media = event.getMedia
            val slottype = media.getAdslotType.getNumber
            val slotid = media.getAdslotId.toInt   //强转为int

            RawFeature(
              ideaid = ad.getUnitId,
              unitid = ad.getGroupId,
              planid = ad.getPlanId,
              userid = ad.getUnitId,
              adclass = ad.getClass_,
              adtype = ad.getType.getNumber,
              interact = ad.getInteraction.getNumber,
              slottype = media.getAdslotType.getNumber,
              slotid = media.getAdslotId.toInt,
              uid = event.getDevice.getUid
            )

          } catch {
            case e: Exception =>
              null
          }
      }
      .filter(_ != null)

    base_data.print()

    var date = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date().getTime)

    ssc.start()
    ssc.awaitTermination()
  }

  case class RawFeature(
                       ideaid: Int = 0,
                       unitid: Int = 0,
                       planid: Int = 0,
                       userid: Int = 0,
                       adclass: Int = 0,
                       adtype: Int = 0,
                       interact: Int = 0,
                       slottype: Int = 0,
                       slotid: Int = 0,
                       uid: String = ""
                       )

  case class Snapshot(
                     searchid: String = "",
                     version: String = "",
                     model_name: String = "",
                     data_type: Int = 0,
                     strategy: String = "",
                     result: Float = 0,
                     calibrated: Float = 0,
                     raw_int: collection.Map[String, Int] = null,
                     raw_string: collection.Map[String, String] = null,
                     feature_vector: collection.Map[Int, Float] = null,
                     date: String = "",
                     hour: String = ""
                     ){

  }

}
package com.cpc.spark.streaming.anal

import java.text.SimpleDateFormat
import java.util.Date

import com.cpc.spark.common.LogData
import kafka.serializer.{DefaultDecoder, StringDecoder}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka._


object CpcStreamingLog2 {
  def main(args: Array[String]) {
    if (args.length < 4) {
      System.err.println(s"""
                            |Usage: CpcStreamingAnal <brokers> <topics> <out_topics>
                            |  <brokers> is a list of one or more Kafka brokers
                            |  <topics> is a list of one or more kafka topics to consume from
                            |  <out_topics> is the out to kafka topic
        """.stripMargin)
      System.exit(1)
    }
    Logger.getRootLogger.setLevel(Level.WARN)
    val Array(brokers, topics, out_topics, seconds) = args

    val sparkConf = new SparkConf().setAppName("v2.9 demo streaming anal topics = " + topics)
    val ssc = new StreamingContext(sparkConf, Seconds(seconds.toInt))
    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)

    var messages: InputDStream[(String, Array[Byte])] =
      KafkaUtils.createDirectStream[String, Array[Byte], StringDecoder, DefaultDecoder](ssc, kafkaParams, topicsSet)



    val base_data = messages.map {
      case (k, v) =>
        try {
          val logdata = LogData.parseData(v)
          val log_timestamp = logdata.log.getLogTimestamp
          val date = new SimpleDateFormat("yyyy-MM-dd").format(log_timestamp)
          val hour = new SimpleDateFormat("HH").format(log_timestamp)

          val field2 = scala.collection.mutable.Map[String, ExtValue]()
          val fieldCount = logdata.log.getField.getMapCount
          for (i <- 0 to fieldCount - 1) {
            val field = logdata.log.getField.getMap(i)
            val extValue = ExtValue(field.getValue.getIntType, field.getValue.getLongType, field.getValue.getFloatType, field.getValue.getStringType)
            field2 += (field.getKey -> extValue)

          }

          val field: collection.Map[String, ExtValue] = field2.toMap
          SrcLog(logdata.log.getLogTimestamp, logdata.log.getIp, field, date, hour)
        } catch {
          case t: Throwable =>
            t.printStackTrace() // TODO: handle error
            null
        }
    }
      .filter(_ != null)
    base_data.print()

    val spark = SparkSession.builder()
      .appName("zyc_kafka_test")
      .enableHiveSupport()
      .getOrCreate()


    base_data.foreachRDD {
      r =>
        val date = r.first().thedate
        val hour = r.first().thehour

//        r.map{
//          x =>
//            val date = r.first().thedate
//            val hour = r.first().thehour
//
//            val p = date + hour
//            (p, x)
//        }

        spark.createDataFrame(r)
          .toDF("log_timestamp", "ip", "field", "thedate", "thehour")
          .write
          .mode(SaveMode.Overwrite)
          .parquet("/warehouse/dl_cpc.db/test_kafka/%s/%s".format(date, hour))
    }


    ssc.start()
    ssc.awaitTermination()
  }

  def getCurrentDate(message: String) {
    var now: Date = new Date()
    var dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    var hehe = dateFormat.format(now)
    if (message.length() > 0) {
      println("currentDate:" + message + ":" + hehe)
    } else {
      print("currentDate:" + hehe)
    }

  }


  case class SrcLog(
                     log_timestamp: Long = 0,
                     ip: String = "",
                     field: collection.Map[String ,ExtValue] = null,
                     thedate: String = "",
                     thehour: String = ""
                   )

  case class ExtValue(int_type: Int = 0, long_type: Long = 0, float_type: Float = 0, string_type: String = "")
}